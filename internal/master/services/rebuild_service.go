// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the License for the specific language governing permissions and limitations
// under the License.

package services

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Type aliases to maintain original usage without entity prefix.
type (
	RebuildProgressResponse = entity.RebuildProgressResponse
	RebuildStatusResponse   = entity.RebuildStatusResponse
	RebuildRequest          = entity.RebuildRequest
	SpaceRebuildRecord      = entity.SpaceRebuildRecord
	PartitionRebuildTask    = entity.PartitionRebuildTask
)

// PS rebuild status constants returned by GetRebuildStatus RPC.
// Status 0 means "not found" and is reported through Exists=false.
const (
	PSRebuildStatusRunning   = 1
	PSRebuildStatusCompleted = 2
	PSRebuildStatusFailed    = 3
)

// Rebuild status string constants.
const (
	RebuildStatusStringPending   = entity.RebuildStatusStringPending
	RebuildStatusStringRunning   = entity.RebuildStatusStringRunning
	RebuildStatusStringCompleted = entity.RebuildStatusStringCompleted
	RebuildStatusStringCancelled = entity.RebuildStatusStringCancelled
	RebuildStatusStringFailed    = entity.RebuildStatusStringFailed
	RebuildStatusStringNotFound  = entity.RebuildStatusStringNotFound
)

// scheduling cadence
const (
	tickInterval = 2 * time.Second
)

// defaultMaxRetries applies when the caller does not set MaxRetries.
const defaultMaxRetries = 3

// maxDispatchAttempts caps per-task dispatch retries.
const maxDispatchAttempts = 3

// maxPollFailureStreak caps consecutive status poll failures per task.
const maxPollFailureStreak = 15

// RebuildService is the public façade.
type RebuildService struct {
	client    *client.Client
	scheduler *RebuildScheduler
}

// NewRebuildService creates a service; Start launches its scheduler.
func NewRebuildService(c *client.Client) *RebuildService {
	return &RebuildService{
		client:    c,
		scheduler: newRebuildScheduler(c),
	}
}

// SetLeaderChecker gates scheduler ticks in multi-master deployments.
func (s *RebuildService) SetLeaderChecker(isLeader func() bool) {
	s.scheduler.setLeaderChecker(isLeader)
}

// StartEtcdLeaderCampaign elects one scheduler leader through an etcd lock.
func (s *RebuildService) StartEtcdLeaderCampaign(ctx context.Context, ttl time.Duration) func() bool {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	state := &leaderCampaign{}
	go state.run(ctx, s.client, ttl)
	return state.isLeader
}

// leaderCampaign tracks local ownership of the scheduler lock.
type leaderCampaign struct {
	mu     sync.RWMutex
	leader bool
}

func (lc *leaderCampaign) isLeader() bool {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return lc.leader
}

func (lc *leaderCampaign) setLeader(v bool) {
	lc.mu.Lock()
	lc.leader = v
	lc.mu.Unlock()
}

// run repeatedly tries to acquire and keep the scheduler lock.
func (lc *leaderCampaign) run(ctx context.Context, c *client.Client, ttl time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("rebuild leader campaign panic: %v\n%s", r, debug.Stack())
		}
	}()
	retryDelay := 5 * time.Second
	refresh := ttl / 3
	if refresh < time.Second {
		refresh = time.Second
	}
	for {
		if ctx.Err() != nil {
			lc.setLeader(false)
			return
		}
		lock := c.Master().NewLock(ctx, entity.LockRebuildScheduler(), ttl)
		acquired, err := lock.TryLock()
		if err != nil || !acquired {
			lc.setLeader(false)
			if err != nil {
				log.Debug("rebuild leader campaign: TryLock failed (will retry): %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
				continue
			}
		}
		lc.setLeader(true)
		log.Info("rebuild leader campaign: this master is now scheduler leader")
		// Keep the lease alive until ctx cancellation or lease expiry.
		holdTicker := time.NewTicker(refresh)
		held := true
		for held {
			select {
			case <-ctx.Done():
				holdTicker.Stop()
				_ = lock.Unlock()
				lc.setLeader(false)
				return
			case <-holdTicker.C:
				if err := lock.KeepAliveOnce(); err != nil {
					log.Warn("rebuild leader campaign: lease keep-alive failed, stepping down: %v", err)
					lc.setLeader(false)
					held = false
				}
			}
		}
		holdTicker.Stop()
		_ = lock.Unlock()
	}
}

// Start launches the scheduler tick goroutine.
func (s *RebuildService) Start() {
	s.scheduler.start()
	log.Info("RebuildService started successfully")
}

// Stop shuts down the scheduler.
func (s *RebuildService) Stop() {
	s.scheduler.stop()
}

// StartRebuild validates the request and enqueues a pending rebuild record.
func (s *RebuildService) StartRebuild(ctx context.Context, req *RebuildRequest) (*RebuildProgressResponse, error) {
	if req == nil || req.Database == "" || req.Space == "" {
		return nil, fmt.Errorf("database and space are required")
	}

	mc := s.client.Master()

	// (1) DB existence.
	dbID, err := mc.QueryDBName2ID(ctx, req.Database)
	if err != nil {
		return nil, fmt.Errorf("db %s not found: %v", req.Database, err)
	}

	// (2) Space existence + enabled.
	space, err := mc.QuerySpaceByName(ctx, dbID, req.Space)
	if err != nil {
		return nil, fmt.Errorf("query space %s/%s: %v", req.Database, req.Space, err)
	}
	if space == nil {
		return nil, fmt.Errorf("space %s/%s not found", req.Database, req.Space)
	}
	if space.Enabled != nil && !*space.Enabled {
		return nil, fmt.Errorf("space %s/%s is disabled", req.Database, req.Space)
	}

	// (3) Resolve the (field, indexType) target list.
	if _, _, nerr := entity.NormalizeRebuildTarget(req.FieldName, req.IndexType); nerr != nil {
		return nil, nerr
	}
	var indexTargets []entity.IndexTarget
	if req.FieldName != "" {
		// First check the field exists at all
		if !space.HasField(req.FieldName) {
			return nil, fmt.Errorf("space %s/%s has no field %q",
				req.Database, req.Space, req.FieldName)
		}
		idx := space.GetIndexByFieldAndType(req.FieldName, req.IndexType)
		if idx == nil {
			return nil, fmt.Errorf("space %s/%s field %q has no index of type %q",
				req.Database, req.Space, req.FieldName, req.IndexType)
		}
		// Re-read fieldName/indexType from the schema
		fieldName := req.FieldName
		if idx.FieldName != "" {
			fieldName = idx.FieldName
		}
		indexTargets = []entity.IndexTarget{{FieldName: fieldName, IndexType: idx.Type}}
	} else {
		indexTargets = space.AllIndexTargets()
		if len(indexTargets) == 0 {
			return nil, fmt.Errorf("space %s/%s has no rebuildable index targets",
				req.Database, req.Space)
		}
	}

	// (4) Reject if a non-terminal record already exists.
	key := entity.RebuildSpaceKey(req.Database, req.Space)
	existingExisting := false
	existing, lerr := s.loadRecord(ctx, key)
	if lerr != nil {
		return nil, fmt.Errorf("load existing record for %s/%s: %v",
			req.Database, req.Space, lerr)
	}
	if existing != nil {
		switch existing.Status {
		case RebuildStatusStringPending, RebuildStatusStringRunning:
			return nil, fmt.Errorf("rebuild for %s/%s already %s",
				req.Database, req.Space, existing.Status)
		case RebuildStatusStringCompleted, RebuildStatusStringFailed, RebuildStatusStringCancelled:
			// New requests overwrite terminal records.
			log.Info("rebuild for %s/%s overwriting previous terminal record (status=%s)",
				req.Database, req.Space, existing.Status)
			existingExisting = true
		}
	}

	// (5) Partition health check.
	targets, err := resolveRebuildPartitions(space, req.PartitionId)
	if err != nil {
		return nil, err
	}
	if err := s.checkPartitionsHealthy(ctx, space, targets); err != nil {
		return nil, fmt.Errorf("partition health check failed: %v", err)
	}

	dropBefore := 0
	if req.DropBefore {
		dropBefore = 1
	}

	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	rec := &SpaceRebuildRecord{
		DBName:      req.Database,
		SpaceName:   req.Space,
		Status:      RebuildStatusStringPending,
		DropBefore:  dropBefore,
		LimitCPU:    req.LimitCPU,
		Describe:    req.Describe,
		PartitionID: req.PartitionId,
		EnqueuedAt:  time.Now(),
		MaxRetries:  maxRetries,
		Indexes:     indexTargets,
	}
	if existingExisting {
		// Overwrite the existing terminal record.
		if err := s.scheduler.persistRecord(ctx, rec); err != nil {
			return nil, fmt.Errorf("save rebuild record: %v", err)
		}
	} else {
		if err := s.saveRecord(ctx, key, rec); err != nil {
			return nil, fmt.Errorf("save rebuild record: %v", err)
		}
	}

	log.Info("rebuild record enqueued: %s/%s (partitionID=%d)", req.Database, req.Space, req.PartitionId)
	return buildProgressFromRecord(rec), nil
}

// GetRebuildProgress returns the current progress for one space.
func (s *RebuildService) GetRebuildProgress(ctx context.Context, dbName, spaceName string) (*RebuildProgressResponse, error) {
	key := entity.RebuildSpaceKey(dbName, spaceName)
	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return &RebuildProgressResponse{
			SpaceKey: dbName + "-" + spaceName,
			Status:   RebuildStatusStringNotFound,
		}, nil
	}
	return buildProgressFromRecord(rec), nil
}

// ListAllRebuildProgress summarizes all rebuild records.
func (s *RebuildService) ListAllRebuildProgress(ctx context.Context) (*entity.RebuildSummaryResponse, error) {
	return s.listRebuildProgressByPrefix(ctx, entity.PrefixRebuild)
}

// ListDBRebuildProgress summarizes rebuild records for one database.
func (s *RebuildService) ListDBRebuildProgress(ctx context.Context, dbName string) (*entity.RebuildSummaryResponse, error) {
	prefix := entity.PrefixRebuild + dbName + "/"
	return s.listRebuildProgressByPrefix(ctx, prefix)
}

func (s *RebuildService) listRebuildProgressByPrefix(ctx context.Context, prefix string) (*entity.RebuildSummaryResponse, error) {
	mc := s.client.Master()
	_, bytesList, err := mc.PrefixScan(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("scan rebuild records: %v", err)
	}

	summary := &entity.RebuildSummaryResponse{}
	for _, bs := range bytesList {
		rec := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal(bs, rec); err != nil {
			log.Warn("unmarshal rebuild record in list: %v", err)
			continue
		}
		if rec.DBName == "" || rec.SpaceName == "" {
			continue
		}
		progress := buildProgressFromRecord(rec)
		summary.Results = append(summary.Results, progress)
		summary.Total++

		switch progress.Status {
		case RebuildStatusStringCompleted:
			summary.CompletedCount++
		case RebuildStatusStringFailed:
			summary.FailedCount++
		case RebuildStatusStringCancelled:
			summary.CancelledCount++
		case RebuildStatusStringRunning:
			summary.RunningCount++
		case RebuildStatusStringPending:
			summary.PendingCount++
		case RebuildStatusStringNotFound:
			summary.NotFoundCount++
		}
	}

	terminal := summary.CompletedCount + summary.FailedCount + summary.CancelledCount
	active := summary.RunningCount + summary.PendingCount
	if terminal+active > 0 {
		summary.SuccessRatio = float64(summary.CompletedCount) / float64(terminal+active)
	}

	return summary, nil
}

// CancelRebuild cancels a rebuild only while it is still pending.
func (s *RebuildService) CancelRebuild(ctx context.Context, dbName, spaceName string) (*entity.CancelRebuildResponse, error) {
	key := entity.RebuildSpaceKey(dbName, spaceName)
	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load rebuild record: %v", err)
	}
	if rec == nil {
		return nil, fmt.Errorf("no rebuild record found for %s/%s", dbName, spaceName)
	}

	switch rec.Status {
	case RebuildStatusStringCompleted, RebuildStatusStringFailed:
		// Already terminal.
		return &entity.CancelRebuildResponse{
			DBName:    dbName,
			SpaceName: spaceName,
			Cancelled: false,
			Reason:    fmt.Sprintf("rebuild already %s, cannot cancel", rec.Status),
			Status:    rec.Status,
		}, nil
	case RebuildStatusStringCancelled:
		// Already cancelled.
		return &entity.CancelRebuildResponse{
			DBName:    dbName,
			SpaceName: spaceName,
			Cancelled: true,
			Reason:    "already cancelled",
			Status:    rec.Status,
		}, nil
	case RebuildStatusStringPending:
		// Cancel with STM to avoid racing pending -> running admission.
		key := entity.RebuildSpaceKey(dbName, spaceName)
		cancelled, err := s.casCancelPending(ctx, key)
		if err != nil {
			return nil, err
		}
		if cancelled {
			log.Info("cancelled pending rebuild for %s/%s", dbName, spaceName)
			return &entity.CancelRebuildResponse{
				DBName:    dbName,
				SpaceName: spaceName,
				Cancelled: true,
				Reason:    "pending record cancelled",
				Status:    RebuildStatusStringCancelled,
			}, nil
		}
		// Status changed; reload and report the new outcome.
		rec2, err2 := s.loadRecord(ctx, key)
		if err2 != nil {
			return nil, fmt.Errorf("reload after CAS conflict: %v", err2)
		}
		if rec2 == nil {
			return nil, fmt.Errorf("no rebuild record found for %s/%s (disappeared after CAS conflict)", dbName, spaceName)
		}
		switch rec2.Status {
		case RebuildStatusStringRunning:
			return &entity.CancelRebuildResponse{
				DBName:    dbName,
				SpaceName: spaceName,
				Cancelled: false,
				Reason:    "rebuild was already admitted to running; cannot cancel once tasks have been dispatched to PS",
				Status:    rec2.Status,
			}, nil
		default:
			return &entity.CancelRebuildResponse{
				DBName:    dbName,
				SpaceName: spaceName,
				Cancelled: false,
				Reason:    fmt.Sprintf("rebuild status changed to %q before cancel could apply", rec2.Status),
				Status:    rec2.Status,
			}, nil
		}
	case RebuildStatusStringRunning:
		// Running tasks cannot be interrupted safely.
		return &entity.CancelRebuildResponse{
			DBName:    dbName,
			SpaceName: spaceName,
			Cancelled: false,
			Reason:    "rebuild is running; cannot cancel once tasks have been dispatched to PS",
			Status:    rec.Status,
		}, nil
	default:
		return nil, fmt.Errorf("unknown rebuild status %q for %s/%s", rec.Status, dbName, spaceName)
	}
}

// casCancelPending atomically changes a pending record to cancelled.
func (s *RebuildService) casCancelPending(ctx context.Context, key string) (bool, error) {
	var conflict bool
	err := s.client.Master().STM(ctx, func(stm concurrency.STM) error {
		raw := stm.Get(key)
		if raw == "" {
			conflict = true
			return nil
		}
		rec := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal([]byte(raw), rec); err != nil {
			return fmt.Errorf("unmarshal in CAS cancel: %v", err)
		}
		if rec.Status != RebuildStatusStringPending {
			conflict = true
			return nil
		}
		rec.Status = RebuildStatusStringCancelled
		rec.ErrorMsg = "cancelled by user while pending"
		rec.FinishedAt = time.Now()
		value, err := vjson.Marshal(rec)
		if err != nil {
			return err
		}
		stm.Put(key, string(value))
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("STM cancel pending: %v", err)
	}
	return !conflict, nil
}

// loadRecord reads the etcd record. Returns (nil, nil) when not found.
func (s *RebuildService) loadRecord(ctx context.Context, key string) (*SpaceRebuildRecord, error) {
	bytes, err := s.client.Master().Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, nil
	}
	rec := &SpaceRebuildRecord{}
	if err := vjson.Unmarshal(bytes, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (s *RebuildService) saveRecord(ctx context.Context, key string, rec *SpaceRebuildRecord) error {
	value, err := vjson.Marshal(rec)
	if err != nil {
		return err
	}
	return s.client.Master().STM(ctx, func(stm concurrency.STM) error {
		if existing := stm.Get(key); existing != "" {
			return fmt.Errorf("rebuild record for %s already exists", key)
		}
		stm.Put(key, string(value))
		return nil
	})
}

// buildProgressFromRecord converts the persistent record into the API response.
func buildProgressFromRecord(rec *SpaceRebuildRecord) *RebuildProgressResponse {
	resp := &RebuildProgressResponse{
		SpaceKey:       rec.SpaceKey(),
		Status:         rec.Status,
		TotalTasks:     rec.TotalReplicas,
		CompletedTasks: rec.CompletedReplicas,
		FailedTasks:    rec.FailedReplicas,
		ErrorMsg:       rec.ErrorMsg,
		EnqueuedAt:     rec.EnqueuedAt,
		StartedAt:      rec.StartedAt,
		FinishedAt:     rec.FinishedAt,
		RetryCount:     rec.RetryCount,
		MaxRetries:     rec.MaxRetries,
		Tasks:          rec.Tasks,
		Indexes:        rec.Indexes,
		// CurrentIndex is 1-based for API users.
		CurrentIndex:  clampOneBased(rec.CurrentIndexIdx, len(rec.Indexes)),
		CurrentTarget: rec.CurrentTarget(),
	}
	// Build task counts and weighted progress.
	progressSum := 0
	progressCount := 0
	for _, t := range rec.Tasks {
		switch t.Status {
		case entity.RebuildStatusRunning:
			if t.Dispatched {
				resp.RunningTasks++
			} else {
				resp.PendingTasks++
			}
		case entity.RebuildStatusCompleted:
			progressSum += 100
			progressCount++
			continue
		case entity.RebuildStatusFailed:
			// Failed tasks are accounted for separately.
			continue
		}
		progressSum += t.Progress
		progressCount++
	}
	if resp.TotalTasks > 0 {
		resp.SuccessRatio = float64(resp.CompletedTasks) / float64(resp.TotalTasks)
	}
	// Divide by TotalTasks so failed replicas lower overall progress.
	if resp.TotalTasks > 0 {
		resp.OverallPercent = progressSum / resp.TotalTasks
		if resp.OverallPercent > 100 {
			resp.OverallPercent = 100
		}
	}
	return resp
}

// ---------------------------------------------------------------------------
// Pre-flight helpers
// ---------------------------------------------------------------------------

// resolveRebuildPartitions returns the target partitions for a rebuild request.
func resolveRebuildPartitions(space *entity.Space, partitionID uint32) ([]*entity.Partition, error) {
	if len(space.Partitions) == 0 {
		return nil, fmt.Errorf("space %s has no partitions", space.Name)
	}
	if partitionID == 0 {
		return space.Partitions, nil
	}
	for _, p := range space.Partitions {
		if p.Id == entity.PartitionID(partitionID) {
			return []*entity.Partition{p}, nil
		}
	}
	return nil, fmt.Errorf("partition %d does not belong to space %s", partitionID, space.Name)
}

// checkPartitionsHealthy validates metadata health for target partitions.
func (s *RebuildService) checkPartitionsHealthy(ctx context.Context,
	space *entity.Space, targets []*entity.Partition) error {

	mc := s.client.Master()
	expectedReplicas := int(space.ReplicaNum)

	for _, p := range targets {
		latest, err := mc.QueryPartition(ctx, p.Id)
		if err != nil || latest == nil {
			return fmt.Errorf("partition %d meta not found: %v", p.Id, err)
		}
		if latest.LeaderID == 0 {
			return fmt.Errorf("partition %d has no leader", latest.Id)
		}
		if expectedReplicas > 0 && len(latest.Replicas) != expectedReplicas {
			return fmt.Errorf("partition %d replica count %d != expected %d",
				latest.Id, len(latest.Replicas), expectedReplicas)
		}
		if len(latest.Replicas) == 0 {
			return fmt.Errorf("partition %d has no replicas", latest.Id)
		}
		for _, nodeID := range latest.Replicas {
			server, qerr := mc.QueryServer(ctx, nodeID)
			if qerr != nil || server == nil {
				return fmt.Errorf("partition %d replica nodeID=%d server unregistered: %v",
					latest.Id, nodeID, qerr)
			}
		}
		for nodeID, st := range latest.ReStatusMap {
			if st != entity.ReplicasOK {
				return fmt.Errorf("partition %d replica nodeID=%d not ready (status=%d)",
					latest.Id, nodeID, st)
			}
		}

		// Reject indexes that have never been built.
		leaderServer, qerr := mc.QueryServer(ctx, latest.LeaderID)
		if qerr != nil || leaderServer == nil {
			return fmt.Errorf("partition %d leader nodeID=%d server unregistered: %v",
				latest.Id, latest.LeaderID, qerr)
		}
		pi, piErr := client.PartitionInfo(leaderServer.RpcAddr(), latest.Id, false)
		if piErr != nil {
			log.Warn("checkPartitionsHealthy: partition %d PartitionInfo RPC failed: %v; skipping index_status check",
				latest.Id, piErr)
		} else if pi.IndexStatus == 0 { // 0 == UNINDEXED
			return fmt.Errorf("partition %d index has not been built (index_status=UNINDEXED); rebuild requires an existing index",
				latest.Id)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// RebuildScheduler
// ---------------------------------------------------------------------------

// RebuildScheduler reconciles etcd records into PS rebuild tasks.
// One PS runs at most one space rebuild; one partition rebuilds one replica at a time.
type RebuildScheduler struct {
	client *client.Client

	// tickMu serializes reconciliation.
	tickMu sync.Mutex

	// isLeader gates ticks in multi-master deployments.
	leaderMu sync.RWMutex
	isLeader func() bool

	stopCh chan struct{}
	once   sync.Once
}

func newRebuildScheduler(c *client.Client) *RebuildScheduler {
	return &RebuildScheduler{
		client: c,
		stopCh: make(chan struct{}),
	}
}

func (sc *RebuildScheduler) setLeaderChecker(isLeader func() bool) {
	sc.leaderMu.Lock()
	sc.isLeader = isLeader
	sc.leaderMu.Unlock()
}

// shouldRun reports whether this node should reconcile now.
func (sc *RebuildScheduler) shouldRun() bool {
	sc.leaderMu.RLock()
	check := sc.isLeader
	sc.leaderMu.RUnlock()
	if check == nil {
		return true
	}
	return check()
}

func (sc *RebuildScheduler) start() {
	go sc.tickLoop()
}

func (sc *RebuildScheduler) stop() {
	sc.once.Do(func() { close(sc.stopCh) })
}

// tickLoop drives the scheduler periodically.
func (sc *RebuildScheduler) tickLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("tickLoop panic: %v\n%s", r, debug.Stack())
		}
	}()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sc.stopCh:
			return
		case <-ticker.C:
			sc.tick()
		}
	}
}

// tick executes one full reconciliation pass.
func (sc *RebuildScheduler) tick() {
	// Only the elected leader reconciles records.
	if !sc.shouldRun() {
		return
	}
	if !sc.tickMu.TryLock() {
		// previous tick still running, skip
		return
	}
	defer sc.tickMu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			log.Error("tick panic: %v", r)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mc := sc.client.Master()
	_, bytesList, err := mc.PrefixScan(ctx, entity.PrefixRebuild)
	if err != nil {
		log.Error("scan rebuild records: %v", err)
		return
	}

	records := make([]*SpaceRebuildRecord, 0, len(bytesList))
	for _, bs := range bytesList {
		rec := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal(bs, rec); err != nil {
			log.Error("unmarshal rebuild record: %v", err)
			continue
		}
		// Skip non-record payloads under the rebuild prefix.
		if rec.DBName == "" || rec.SpaceName == "" {
			continue
		}
		records = append(records, rec)
	}

	// 1. Build global PS occupancy from currently-running records.
	psBusy := make(map[entity.NodeID]string) // nodeID -> spaceKey
	for _, rec := range records {
		if rec.Status != RebuildStatusStringRunning {
			continue
		}
		for _, t := range rec.Tasks {
			if isReplicaTerminal(t.Status) {
				continue
			}
			psBusy[t.NodeID] = rec.SpaceKey()
		}
	}

	// 2. Process running records first (advance dispatch / poll / finalize).
	for _, rec := range records {
		if rec.Status == RebuildStatusStringRunning {
			sc.reconcileRunning(ctx, rec, psBusy)
		}
	}

	// 3. Then admit pending records in FIFO order, respecting PS occupancy.
	pending := make([]*SpaceRebuildRecord, 0)
	for _, rec := range records {
		if rec.Status == RebuildStatusStringPending {
			pending = append(pending, rec)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].EnqueuedAt.Before(pending[j].EnqueuedAt)
	})
	for _, rec := range pending {
		sc.reconcilePending(ctx, rec, psBusy)
	}
}

// isReplicaTerminal reports whether a replica task is done.
func isReplicaTerminal(st entity.RebuildTaskStatus) bool {
	return st == entity.RebuildStatusCompleted || st == entity.RebuildStatusFailed
}

// ---------------------------------------------------------------------------
// reconcilePending: admit a pending space if all required PS are free.
// ---------------------------------------------------------------------------

func (sc *RebuildScheduler) reconcilePending(ctx context.Context,
	rec *SpaceRebuildRecord, psBusy map[entity.NodeID]string) {

	mc := sc.client.Master()

	dbID, err := mc.QueryDBName2ID(ctx, rec.DBName)
	if err != nil {
		log.Warn("pending %s: query db: %v", rec.SpaceKey(), err)
		return
	}
	space, err := mc.QuerySpaceByName(ctx, dbID, rec.SpaceName)
	if err != nil || space == nil {
		log.Warn("pending %s: space gone, dropping record", rec.SpaceKey())
		rec.Status = RebuildStatusStringFailed
		rec.ErrorMsg = "space not found"
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}

	// Resolve target partitions.
	var partitions []*entity.Partition
	if rec.PartitionID > 0 {
		for _, p := range space.Partitions {
			if p.Id == entity.PartitionID(rec.PartitionID) {
				partitions = []*entity.Partition{p}
				break
			}
		}
		if len(partitions) == 0 {
			log.Warn("pending %s: partition %d gone, marking as failed", rec.SpaceKey(), rec.PartitionID)
			rec.Status = RebuildStatusStringFailed
			rec.ErrorMsg = fmt.Sprintf("partition %d not found", rec.PartitionID)
			rec.FinishedAt = time.Now()
			_ = sc.persistRecord(ctx, rec)
			return
		}
	} else {
		partitions = space.Partitions
	}

	// Build the candidate task plan.
	tasks := make([]*PartitionRebuildTask, 0)
	psSet := make(map[entity.NodeID]struct{})
	// All tasks in this pass rebuild the current target.
	target := rec.CurrentTarget()
	if target.IsZero() {
		log.Warn("pending %s: no current rebuild target (Indexes=%v, Idx=%d), marking as failed",
			rec.SpaceKey(), rec.Indexes, rec.CurrentIndexIdx)
		rec.Status = RebuildStatusStringFailed
		rec.ErrorMsg = fmt.Sprintf("no current rebuild target (Indexes=%v, Idx=%d)", rec.Indexes, rec.CurrentIndexIdx)
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}
	for _, p := range partitions {
		for replicaIdx, nodeID := range p.Replicas {
			server, qerr := mc.QueryServer(ctx, nodeID)
			if qerr != nil || server == nil {
				log.Warn("pending %s: skip replica nodeID=%d: %v",
					rec.SpaceKey(), nodeID, qerr)
				continue
			}
			tasks = append(tasks, &PartitionRebuildTask{
				PartitionID:  p.Id,
				NodeID:       nodeID,
				ReplicaIndex: replicaIdx,
				PSNodeAddr:   server.RpcAddr(),
				SpaceKey:     rec.SpaceKey(),
				TaskType:     "rebuild",
				FieldName:    target.FieldName,
				IndexType:    target.IndexType,
				// Running + Dispatched=false means planned but not sent.
				Status:     entity.RebuildStatusRunning,
				DropBefore: rec.DropBefore,
				LimitCPU:   rec.LimitCPU,
				Describe:   rec.Describe,
				MaxRetries: rec.MaxRetries,
			})
			psSet[nodeID] = struct{}{}
		}
	}
	if len(tasks) == 0 {
		log.Warn("pending %s: no replicas resolved, marking as failed", rec.SpaceKey())
		rec.Status = RebuildStatusStringFailed
		rec.ErrorMsg = "no replicas resolved for rebuild"
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}

	// Check PS occupancy: must wait if any required PS is busy.
	for ps := range psSet {
		if owner, busy := psBusy[ps]; busy {
			log.Debug("pending %s: PS %d busy by %s, wait", rec.SpaceKey(), ps, owner)
			return
		}
	}

	// Admit: transition pending -> running and attach the task plan.
	rec.Status = RebuildStatusStringRunning
	rec.StartedAt = time.Now()
	rec.TotalReplicas = len(tasks)
	rec.CompletedReplicas = 0
	rec.FailedReplicas = 0
	rec.Tasks = tasks

	// Reserve PSs for this tick.
	for ps := range psSet {
		psBusy[ps] = rec.SpaceKey()
	}
	log.Info("space %s admitted, ps=%d totalReplicas=%d",
		rec.SpaceKey(), len(psSet), len(tasks))

	// Persist running before dispatch so crash recovery is idempotent.
	// STM also avoids racing with CancelRebuild.
	admitted, err := sc.casAdmitPending(ctx, rec)
	if err != nil {
		log.Error("CAS admit pending %s: %v", rec.SpaceKey(), err)
		return
	}
	if !admitted {
		log.Info("space %s not admitted (status changed before CAS, likely cancelled)", rec.SpaceKey())
		return
	}

	// Dispatch initial tasks in this tick.
	sc.dispatchPending(ctx, rec)

	// Persist Dispatched=true flags.
	if err := sc.persistRecord(ctx, rec); err != nil {
		log.Error("persist post-dispatch admission %s: %v", rec.SpaceKey(), err)
	}
}

// ---------------------------------------------------------------------------
// reconcileRunning: poll active tasks, dispatch the next task per PS,
// finalize the space if all tasks are terminal.
// ---------------------------------------------------------------------------

func (sc *RebuildScheduler) reconcileRunning(ctx context.Context,
	rec *SpaceRebuildRecord, psBusy map[entity.NodeID]string) {

	dirty := false

	// (a) Poll dispatched-but-not-terminal tasks.
	for _, t := range rec.Tasks {
		if !t.Dispatched || isReplicaTerminal(t.Status) {
			continue
		}
		resp, err := client.GetRebuildStatus(t.PSNodeAddr, rec.SpaceKey(),
			t.FieldName, t.IndexType, t.PartitionID)
		if err != nil {
			t.PollFailureStreak++
			// Persist every streak increment so restarts do not reset it.
			dirty = true
			log.Warn("GetRebuildStatus %s pid=%d nodeID=%d (streak=%d/%d): %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID,
				t.PollFailureStreak, maxPollFailureStreak, err)
			// Stop polling forever once the failure streak crosses the budget.
			if t.PollFailureStreak >= maxPollFailureStreak {
				markReplicaFailed(t,
					fmt.Sprintf("GetRebuildStatus failed %d consecutive times: %v",
						t.PollFailureStreak, err))
			}
			continue // will retry next tick (or finalize if marked failed)
		}
		// Successful poll resets the streak.
		if t.PollFailureStreak > 0 {
			t.PollFailureStreak = 0
			dirty = true
		}

		if !resp.Exists {
			// Missing PS task is terminal; finalize decides whether to retry.
			markReplicaFailed(t,
				fmt.Sprintf("ps reports task missing (pid=%d, nodeID=%d); partitionwill be retried by scheduler",
					t.PartitionID, t.NodeID))
			dirty = true
			continue
		}

		switch resp.Status {
		case PSRebuildStatusRunning:
			if t.Status != entity.RebuildStatusRunning {
				t.Status = entity.RebuildStatusRunning
				dirty = true
			}
			if resp.Progress != t.Progress {
				// Keep displayed progress monotonic.
				if resp.Progress > t.Progress {
					t.Progress = resp.Progress
					dirty = true
				}
			}
		case PSRebuildStatusCompleted:
			t.Status = entity.RebuildStatusCompleted
			t.CompleteTime = time.Now()
			t.Progress = 100
			dirty = true
		case PSRebuildStatusFailed:
			markReplicaFailed(t, resp.ErrorMessage)
			dirty = true
		default:
			log.Warn("unknown PS rebuild status %d for %s pid=%d",
				resp.Status, rec.SpaceKey(), t.PartitionID)
		}
	}

	// (b) Dispatch pending tasks under the per-PS-serial constraint.
	if sc.dispatchPending(ctx, rec) {
		dirty = true
	}

	// Clear Rebuilding markers for terminal tasks.
	sc.unmarkRebuildingForTerminalTasks(ctx, rec)

	// (c) Recompute counters; release psBusy slots that are now drained.
	sc.recountAndReleasePS(rec, psBusy)

	// (d) Persist if anything changed.
	if dirty {
		if err := sc.persistRecord(ctx, rec); err != nil {
			log.Error("persist running record %s: %v", rec.SpaceKey(), err)
			return
		}
	}

	// (e) Finalize if all replicas are terminal.
	allTerminal := true
	for _, t := range rec.Tasks {
		if !isReplicaTerminal(t.Status) {
			allTerminal = false
			break
		}
	}
	if allTerminal {
		sc.finalize(ctx, rec, psBusy)
	}
}

// dispatchPending sends pending tasks while preserving PS and partition serialism.
func (sc *RebuildScheduler) dispatchPending(ctx context.Context, rec *SpaceRebuildRecord) bool {
	_ = ctx
	// Active PSs already have a dispatched non-terminal task.
	active := make(map[entity.NodeID]bool)
	// Active partitions rebuild one replica at a time.
	activePartition := make(map[entity.PartitionID]bool)
	for _, t := range rec.Tasks {
		if t.Dispatched && !isReplicaTerminal(t.Status) {
			active[t.NodeID] = true
			activePartition[t.PartitionID] = true
		}
	}

	dirty := false
	for _, t := range rec.Tasks {
		if t.Dispatched || isReplicaTerminal(t.Status) {
			continue
		}
		if active[t.NodeID] {
			continue // serialize on this PS
		}
		if activePartition[t.PartitionID] {
			continue // one replica at a time per partition
		}

		t.DispatchAttempts++
		t.DispatchAt = time.Now()
		t.StartTime = time.Now()
		err := client.ExecuteRebuildIndex(t.PSNodeAddr, rec.SpaceKey(),
			t.FieldName, t.IndexType, t.PartitionID,
			t.DropBefore, t.LimitCPU, t.Describe)
		if err != nil {
			// Retry transient dispatch failures before failing the replica.
			t.LastErrorMsg = err.Error()
			dirty = true
			if t.DispatchAttempts >= maxDispatchAttempts {
				log.Error("ExecuteRebuildIndex %s pid=%d nodeID=%d gave up after %d attempts: %v",
					rec.SpaceKey(), t.PartitionID, t.NodeID, t.DispatchAttempts, err)
				markReplicaFailed(t,
					fmt.Sprintf("ExecuteRebuildIndex failed %d times: %v",
						t.DispatchAttempts, err))
				continue
			}
			log.Warn("ExecuteRebuildIndex %s pid=%d nodeID=%d failed (attempt=%d/%d), will retry: %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID,
				t.DispatchAttempts, maxDispatchAttempts, err)
			// Keep Dispatched=false so other PSs can still make progress.
			continue
		}
		t.Dispatched = true
		t.Status = entity.RebuildStatusRunning
		active[t.NodeID] = true
		activePartition[t.PartitionID] = true
		dirty = true
		log.Info("rebuild dispatched: space=%s pid=%d nodeID=%d (attempt=%d)",
			rec.SpaceKey(), t.PartitionID, t.NodeID, t.DispatchAttempts)
		// Mark this replica as Rebuilding in the partition record.
		if err := sc.markReplicaRebuilding(ctx, t.PartitionID, t.NodeID, true); err != nil {
			log.Warn("markReplicaRebuilding(rebuilding) failed for pid=%d nodeID=%d: %v",
				t.PartitionID, t.NodeID, err)
		}
	}
	return dirty
}

// recountAndReleasePS refreshes counters and frees drained PS slots.

func (sc *RebuildScheduler) recountAndReleasePS(rec *SpaceRebuildRecord, psBusy map[entity.NodeID]string) {
	completed, failed := 0, 0
	stillBusy := make(map[entity.NodeID]struct{})
	for _, t := range rec.Tasks {
		switch t.Status {
		case entity.RebuildStatusCompleted:
			completed++
		case entity.RebuildStatusFailed:
			failed++
		default:
			// Non-terminal tasks keep their PS occupied.
			stillBusy[t.NodeID] = struct{}{}
		}
	}
	rec.CompletedReplicas = completed
	rec.FailedReplicas = failed

	// Release PSs no longer busy for this record.
	spaceKey := rec.SpaceKey()
	for nodeID, owner := range psBusy {
		if owner != spaceKey {
			continue
		}
		if _, busy := stillBusy[nodeID]; !busy {
			delete(psBusy, nodeID)
		}
	}
}

// markReplicaFailed sets a per-replica task to terminal failed state.
func markReplicaFailed(t *PartitionRebuildTask, msg string) {
	t.Status = entity.RebuildStatusFailed
	t.LastErrorMsg = msg
	t.CompleteTime = time.Now()
}

// markReplicaRebuilding toggles the router-visible Rebuilding marker.
func (sc *RebuildScheduler) markReplicaRebuilding(ctx context.Context,
	pid entity.PartitionID, nodeID entity.NodeID, rebuilding bool) error {

	key := entity.PartitionKey(pid)
	return sc.client.Master().STM(ctx, func(stm concurrency.STM) error {
		raw := stm.Get(key)
		if raw == "" {
			return fmt.Errorf("partition %d not found", pid)
		}
		p := &entity.Partition{}
		if err := vjson.Unmarshal([]byte(raw), p); err != nil {
			return fmt.Errorf("unmarshal partition %d: %w", pid, err)
		}
		if p.ReStatusMap == nil {
			p.ReStatusMap = make(map[uint64]uint32)
		}

		cur := p.ReStatusMap[uint64(nodeID)]
		if rebuilding {
			if cur == entity.ReplicasRebuilding {
				return nil // already set, no-op (no Put → STM commits empty)
			}
			p.ReStatusMap[uint64(nodeID)] = entity.ReplicasRebuilding
		} else {
			if cur != entity.ReplicasRebuilding {
				return nil // not currently Rebuilding; don't clobber NotReady etc.
			}
			p.ReStatusMap[uint64(nodeID)] = entity.ReplicasOK
		}

		// Bump UpdateTime so router partition caches accept this write.
		p.UpdateTime = time.Now().UnixNano()

		bytes, err := vjson.Marshal(p)
		if err != nil {
			return fmt.Errorf("marshal partition %d: %w", pid, err)
		}
		stm.Put(key, string(bytes))
		return nil
	})
}

// unmarkRebuildingForTerminalTasks clears markers for finished tasks.
func (sc *RebuildScheduler) unmarkRebuildingForTerminalTasks(
	ctx context.Context, rec *SpaceRebuildRecord) {
	for _, t := range rec.Tasks {
		if !t.Dispatched || !isReplicaTerminal(t.Status) {
			continue
		}
		if err := sc.markReplicaRebuilding(ctx, t.PartitionID, t.NodeID, false); err != nil {
			log.Warn("markReplicaRebuilding(reset) %s pid=%d nodeID=%d: %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID, err)
		}
	}
}

// unmarkRebuildingForAllTasks clears markers for every task in a record.
func (sc *RebuildScheduler) unmarkRebuildingForAllTasks(
	ctx context.Context, rec *SpaceRebuildRecord) {
	for _, t := range rec.Tasks {
		if err := sc.markReplicaRebuilding(ctx, t.PartitionID, t.NodeID, false); err != nil {
			log.Warn("markReplicaRebuilding(finalize-reset) %s pid=%d nodeID=%d: %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID, err)
		}
	}
}

// ---------------------------------------------------------------------------
// finalize: handle a record where every replica task is terminal.
// ---------------------------------------------------------------------------

func (sc *RebuildScheduler) finalize(ctx context.Context, rec *SpaceRebuildRecord, psBusy map[entity.NodeID]string) {
	spaceKey := rec.SpaceKey()
	// Final safety sweep for stale Rebuilding markers.
	sc.unmarkRebuildingForTerminalTasks(ctx, rec)

	// Do not release psBusy until we know no partition retry will reuse it.

	// Phase 1: retry only failed partitions that still have budget.
	if rec.PartitionRetries == nil {
		rec.PartitionRetries = map[entity.PartitionID]int{}
	}
	byPartition := map[entity.PartitionID][]*PartitionRebuildTask{}
	for _, t := range rec.Tasks {
		byPartition[t.PartitionID] = append(byPartition[t.PartitionID], t)
	}

	requeuedAny := false
	for pid, group := range byPartition {
		anyFailed := false
		for _, t := range group {
			if t.Status == entity.RebuildStatusFailed {
				anyFailed = true
				break
			}
		}
		if !anyFailed {
			continue
		}
		if rec.PartitionRetries[pid] >= rec.MaxRetries {
			log.Info("partition %d in space %s exhausted retries (%d/%d), keeping failed",
				pid, spaceKey, rec.PartitionRetries[pid], rec.MaxRetries)
			continue
		}
		newTasks, err := sc.replanPartitionTasks(ctx, rec, pid)
		if err != nil {
			// Keep this partition failed if it cannot be replanned.
			log.Warn("partition %d in space %s replan failed: %v — leaving failed", pid, spaceKey, err)
			continue
		}
		if len(newTasks) == 0 {
			log.Warn("partition %d in space %s yielded no live replicas — leaving failed", pid, spaceKey)
			continue
		}
		// Replace this partition's tasks; next tick dispatches them.
		rec.Tasks = replacePartitionTasks(rec.Tasks, pid, newTasks)
		rec.PartitionRetries[pid]++
		requeuedAny = true
		log.Info("partition %d in space %s requeued for retry %d/%d (%d replicas)",
			pid, spaceKey, rec.PartitionRetries[pid], rec.MaxRetries, len(newTasks))
	}

	// Recompute counters after retry replanning.
	sc.recountRecord(rec)

	if requeuedAny {
		// Re-assert PS occupancy for replanned running tasks.
		for _, t := range rec.Tasks {
			if !isReplicaTerminal(t.Status) {
				psBusy[t.NodeID] = spaceKey
			}
		}
		// RetryCount mirrors the deepest partition retry depth.
		rec.Status = RebuildStatusStringRunning
		rec.RetryCount = maxPartitionRetry(rec.PartitionRetries)
		rec.ErrorMsg = fmt.Sprintf("partition-level retry in progress (depth=%d/%d)",
			rec.RetryCount, rec.MaxRetries)
		if err := sc.persistRecord(ctx, rec); err != nil {
			log.Error("persist partition-retry record %s: %v", spaceKey, err)
		}
		return
	}

	// No retry remains; release PS slots owned by this record.
	for nodeID, owner := range psBusy {
		if owner == spaceKey {
			delete(psBusy, nodeID)
		}
	}

	// Phase 2: finish this target, then advance or mark terminal.
	failed := rec.FailedReplicas
	completed := rec.CompletedReplicas
	total := rec.TotalReplicas
	if failed == 0 && rec.HasMoreTargets() {
		previousTarget := rec.CurrentTarget()
		rec.CurrentIndexIdx++
		nextTarget := rec.CurrentTarget()
		rec.Status = RebuildStatusStringPending
		rec.Tasks = nil
		rec.PartitionRetries = nil
		rec.RetryCount = 0
		rec.TotalReplicas = 0
		rec.CompletedReplicas = 0
		rec.FailedReplicas = 0
		rec.ErrorMsg = ""
		// Requeue fairly against other pending records.
		rec.EnqueuedAt = time.Now()
		if err := sc.persistRecord(ctx, rec); err != nil {
			log.Error("persist next-target advance %s: %v", spaceKey, err)
			return
		}
		log.Info("space %s advanced rebuild target: %s -> %s (%d/%d targets done)",
			spaceKey, previousTarget, nextTarget, rec.CurrentIndexIdx, len(rec.Indexes))
		return
	}

	finalStatus := RebuildStatusStringCompleted
	finalErr := ""
	if failed > 0 {
		finalStatus = RebuildStatusStringFailed
		finalErr = fmt.Sprintf("%d/%d replicas failed on target %s (max partition retry %d)",
			failed, total, rec.CurrentTarget(), maxPartitionRetry(rec.PartitionRetries))
	}
	// Keep terminal records so the last result remains queryable.
	rec.Status = finalStatus
	rec.ErrorMsg = finalErr
	rec.FinishedAt = time.Now()
	if err := sc.persistRecord(ctx, rec); err != nil {
		log.Error("persist finalized record %s: %v", spaceKey, err)
	}
	log.Info("space %s finalized: status=%s completed=%d failed=%d retry=%d targets=%d/%d err=%q",
		spaceKey, finalStatus, completed, failed, maxPartitionRetry(rec.PartitionRetries),
		rec.CurrentIndexIdx+1, len(rec.Indexes), finalErr)
}

// replanPartitionTasks rebuilds one partition's task plan from current metadata.
func (sc *RebuildScheduler) replanPartitionTasks(ctx context.Context,
	rec *SpaceRebuildRecord, pid entity.PartitionID) ([]*PartitionRebuildTask, error) {

	mc := sc.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, rec.DBName)
	if err != nil {
		return nil, fmt.Errorf("query db: %v", err)
	}
	space, err := mc.QuerySpaceByName(ctx, dbID, rec.SpaceName)
	if err != nil || space == nil {
		return nil, fmt.Errorf("space gone: %v", err)
	}
	var part *entity.Partition
	for _, p := range space.Partitions {
		if p.Id == pid {
			part = p
			break
		}
	}
	if part == nil {
		return nil, fmt.Errorf("partition %d not in space", pid)
	}
	out := make([]*PartitionRebuildTask, 0, len(part.Replicas))
	target := rec.CurrentTarget()
	for replicaIdx, nodeID := range part.Replicas {
		server, qerr := mc.QueryServer(ctx, nodeID)
		if qerr != nil || server == nil {
			log.Warn("replan partition %d: skip replica nodeID=%d: %v", pid, nodeID, qerr)
			continue
		}
		out = append(out, &PartitionRebuildTask{
			PartitionID:  pid,
			NodeID:       nodeID,
			ReplicaIndex: replicaIdx,
			PSNodeAddr:   server.RpcAddr(),
			SpaceKey:     rec.SpaceKey(),
			TaskType:     "rebuild",
			FieldName:    target.FieldName,
			IndexType:    target.IndexType,
			Status:       entity.RebuildStatusRunning,
			// Retries must not drop existing index data again.
			DropBefore: 0,
			LimitCPU:   rec.LimitCPU,
			Describe:   rec.Describe,
			MaxRetries: rec.MaxRetries,
		})
	}
	return out, nil
}

// replacePartitionTasks swaps one partition's tasks for newGroup.
func replacePartitionTasks(all []*PartitionRebuildTask, pid entity.PartitionID,
	newGroup []*PartitionRebuildTask) []*PartitionRebuildTask {

	out := make([]*PartitionRebuildTask, 0, len(all)-1+len(newGroup))
	inserted := false
	for _, t := range all {
		if t.PartitionID == pid {
			if !inserted {
				out = append(out, newGroup...)
				inserted = true
			}
			continue
		}
		out = append(out, t)
	}
	if !inserted {
		out = append(out, newGroup...)
	}
	return out
}

// recountRecord refreshes replica counters from rec.Tasks.
func (sc *RebuildScheduler) recountRecord(rec *SpaceRebuildRecord) {
	completed, failed := 0, 0
	for _, t := range rec.Tasks {
		switch t.Status {
		case entity.RebuildStatusCompleted:
			completed++
		case entity.RebuildStatusFailed:
			failed++
		}
	}
	rec.TotalReplicas = len(rec.Tasks)
	rec.CompletedReplicas = completed
	rec.FailedReplicas = failed
}

// clampOneBased converts a 0-based cursor into a clamped 1-based counter.
func clampOneBased(cursor, total int) int {
	if total <= 0 {
		return 0
	}
	if cursor < 0 {
		return 0
	}
	if cursor >= total {
		return total
	}
	return cursor + 1
}

// maxPartitionRetry returns the deepest partition retry count.
func maxPartitionRetry(m map[entity.PartitionID]int) int {
	max := 0
	for _, v := range m {
		if v > max {
			max = v
		}
	}
	return max
}

// ---------------------------------------------------------------------------
// etcd helpers
// ---------------------------------------------------------------------------

// casAdmitPending atomically changes a pending record to running.
func (sc *RebuildScheduler) casAdmitPending(ctx context.Context, rec *SpaceRebuildRecord) (bool, error) {
	var conflict bool
	err := sc.client.Master().STM(ctx, func(stm concurrency.STM) error {
		key := entity.RebuildSpaceKey(rec.DBName, rec.SpaceName)
		raw := stm.Get(key)
		if raw == "" {
			conflict = true
			return nil
		}
		current := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal([]byte(raw), current); err != nil {
			return fmt.Errorf("unmarshal in CAS admit: %v", err)
		}
		if current.Status != RebuildStatusStringPending {
			conflict = true
			return nil
		}
		// rec already contains prepared tasks, counters, and StartedAt.
		rec.Status = RebuildStatusStringRunning
		value, err := vjson.Marshal(rec)
		if err != nil {
			return err
		}
		stm.Put(key, string(value))
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("STM admit pending: %v", err)
	}
	return !conflict, nil
}

// persistRecord writes the in-memory record back to etcd.
func (sc *RebuildScheduler) persistRecord(ctx context.Context, rec *SpaceRebuildRecord) error {
	value, err := vjson.Marshal(rec)
	if err != nil {
		return err
	}
	key := entity.RebuildSpaceKey(rec.DBName, rec.SpaceName)
	return sc.client.Master().STM(ctx, func(stm concurrency.STM) error {
		_ = stm.Get(key) // include in read set for CAS retry
		stm.Put(key, string(value))
		return nil
	})
}

// deleteRecord removes a rebuild record from etcd.
func (sc *RebuildScheduler) deleteRecord(ctx context.Context, key string) error {
	return sc.client.Master().Delete(ctx, key)
}
