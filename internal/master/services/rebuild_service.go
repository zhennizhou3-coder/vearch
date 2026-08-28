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

	"github.com/google/uuid"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Type aliases to maintain original usage without entity prefix.
type (
	RebuildProgressResponse = entity.RebuildProgressResponse
	RebuildRequest          = entity.RebuildRequest
	SpaceRebuildRecord      = entity.SpaceRebuildRecord
	RebuildTask             = entity.RebuildTask
)

// scheduling cadence
const (
	tickInterval = 2 * time.Second
)

// defaultMaxRetries applies when the caller does not set MaxRetries.
const defaultMaxRetries = 3

// maxDispatchAttempts caps per-task dispatch retries.
const maxDispatchAttempts = 3

// maxPollRetries caps consecutive status poll failures per task.
const maxPollRetries = 15

// Leadership-transfer knobs: before rebuilding the replica that is currently
// the partition leader, the scheduler transfers leadership to a healthy
// follower so leader-typed reads stay served during the rebuild.
const (
	// leaderTransferTimeout caps how long we wait for a triggered transfer to
	// take effect (LeaderID actually changes) before giving up.
	leaderTransferTimeout = 10 * time.Second
	// leaderTransferPollInterval is how often we poll the partition LeaderID
	// while waiting for the transfer to complete.
	leaderTransferPollInterval = 500 * time.Millisecond
)

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
	if req == nil || req.DBName == "" || req.SpaceName == "" {
		return nil, fmt.Errorf("database and space are required")
	}

	mc := s.client.Master()

	dbID, err := mc.QueryDBName2ID(ctx, req.DBName)
	if err != nil {
		return nil, fmt.Errorf("resolve db %s: %v", req.DBName, err)
	}

	space, err := mc.QuerySpaceByName(ctx, dbID, req.SpaceName)
	if err != nil {
		return nil, fmt.Errorf("resolve space %s/%s: %v", req.DBName, req.SpaceName, err)
	}

	// Resolve the target index name list.
	var indexNames []string
	if req.IndexName != "" {
		idx := space.GetIndexByName(req.IndexName)
		if idx == nil {
			return nil, fmt.Errorf("space %s/%s has no index named %q",
				req.DBName, req.SpaceName, req.IndexName)
		}
		if idx.FieldName == "" || !space.IsVectorField(idx.FieldName) {
			return nil, fmt.Errorf("space %s/%s index %q is not a rebuildable vector index",
				req.DBName, req.SpaceName, req.IndexName)
		}
		indexNames = []string{idx.Name}
	} else {
		indexNames = space.AllVectorIndexes()
		if len(indexNames) == 0 {
			return nil, fmt.Errorf("space %s/%s has no rebuildable index targets",
				req.DBName, req.SpaceName)
		}
	}

	// (4) Partition health check runs before the STM so we do not hold
	// the etcd session across per-partition RPCs.
	partitions, err := selectPartitions(space, req.PartitionId)
	if err != nil {
		return nil, err
	}
	if err := s.checkPartitionsHealthy(ctx, space, partitions); err != nil {
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
		DBName:      req.DBName,
		SpaceName:   req.SpaceName,
		Status:      entity.RebuildStatusPending,
		RebuildID:   uuid.NewString(),
		DropBefore:  dropBefore,
		LimitCPU:    req.LimitCPU,
		Describe:    req.Describe,
		PartitionID: req.PartitionId,
		EnqueuedAt:  time.Now(),
		MaxRetries:  maxRetries,
		Indexes:     indexNames,
	}

	// (5) Atomic enqueue: reject if a non-terminal record exists;
	// overwrite terminal record; create fresh otherwise. Merging the
	// existence check and the write into a single STM eliminates the
	// TOCTOU where two concurrent StartRebuild callers both observe a
	// terminal record and both overwrite — only one rebuild actually
	// runs and the losing caller would otherwise receive a stale
	// "started" response.
	key := entity.RebuildSpaceKey(req.DBName, req.SpaceName)
	value, err := vjson.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal rebuild record: %v", err)
	}
	var conflictStatus entity.RebuildStatus
	err = s.client.Master().STM(ctx, func(stm concurrency.STM) error {
		raw := stm.Get(key)
		if raw != "" {
			var cur SpaceRebuildRecord
			if uerr := vjson.Unmarshal([]byte(raw), &cur); uerr == nil {
				if !cur.Status.IsTerminal() {
					conflictStatus = cur.Status
					return nil
				}
				log.Info("rebuild for %s/%s overwriting previous terminal record (status=%s)",
					req.DBName, req.SpaceName, cur.Status)
			}
		}
		stm.Put(key, string(value))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("save rebuild record: %v", err)
	}
	if conflictStatus != "" {
		return nil, fmt.Errorf("rebuild for %s/%s already %s",
			req.DBName, req.SpaceName, conflictStatus)
	}

	log.Info("rebuild record enqueued: %s/%s (partitionID=%d)", req.DBName, req.SpaceName, req.PartitionId)
	return rebuildProgressFromRecord(rec), nil
}

// GetRebuildProgress returns the current progress for one space.
// Returns a REBUILD_RECORD_NOT_EXIST error when no record exists for
// (dbName, spaceName). Callers/handlers should surface that as 404.
func (s *RebuildService) GetRebuildProgress(ctx context.Context, dbName, spaceName string) (*RebuildProgressResponse, error) {
	key := entity.RebuildSpaceKey(dbName, spaceName)
	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_REBUILD_RECORD_NOT_EXIST,
			fmt.Errorf("rebuild record for %s/%s does not exist", dbName, spaceName))
	}
	return rebuildProgressFromRecord(rec), nil
}

// ListAllRebuildProgress summarizes all rebuild records.
func (s *RebuildService) ListAllRebuildProgress(ctx context.Context) (*entity.RebuildSummaryResponse, error) {
	return s.listRebuildProgressByPrefix(ctx, entity.PrefixRebuild)
}

// ListDBRebuildProgress summarizes rebuild records for one database.
func (s *RebuildService) ListDBRebuildProgress(ctx context.Context, dbName string) (*entity.RebuildSummaryResponse, error) {
	// Validate db existence up-front so a nonexistent db returns DB_NOT_EXIST
	// rather than an empty summary that is indistinguishable from "db exists
	// but has no rebuild records".
	if _, err := s.client.Master().QueryDBName2ID(ctx, dbName); err != nil {
		return nil, err
	}
	prefix := entity.PrefixRebuild + dbName + "/"
	return s.listRebuildProgressByPrefix(ctx, prefix)
}

func (s *RebuildService) listRebuildProgressByPrefix(ctx context.Context, prefix string) (*entity.RebuildSummaryResponse, error) {
	mc := s.client.Master()
	_, bytesList, err := mc.PrefixScan(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("scan rebuild records: %v", err)
	}

	// Results is initialized to an empty (non-nil) slice so JSON always
	// serializes as `"results": []` rather than `null` when no records
	// exist. Callers iterate `results` unconditionally.
	summary := &entity.RebuildSummaryResponse{
		Results: []*RebuildProgressResponse{},
	}
	for _, bs := range bytesList {
		rec := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal(bs, rec); err != nil {
			log.Warn("unmarshal rebuild record in list: %v", err)
			continue
		}
		if rec.DBName == "" || rec.SpaceName == "" {
			continue
		}
		progress := rebuildProgressFromRecord(rec)
		summary.Results = append(summary.Results, progress)
		summary.Total++

		switch progress.Status {
		case entity.RebuildStatusCompleted:
			summary.CompletedCount++
		case entity.RebuildStatusFailed:
			summary.FailedCount++
		case entity.RebuildStatusCancelled:
			summary.CancelledCount++
		case entity.RebuildStatusRunning:
			summary.RunningCount++
		case entity.RebuildStatusPending:
			summary.PendingCount++
		}
	}

	terminal := summary.CompletedCount + summary.FailedCount + summary.CancelledCount
	active := summary.RunningCount + summary.PendingCount
	if terminal+active > 0 {
		summary.SuccessRatio = float64(summary.CompletedCount) / float64(terminal+active)
	}

	return summary, nil
}

// CancelRebuild cancels rebuild work for one space.
//
// Cancellation is task-scoped (best-effort):
//   - Pending record: whole record is transitioned to Cancelled (unchanged
//     behavior; casCancelPending performs the CAS).
//   - Running record: every task that is still Pending (not yet dispatched)
//     is transitioned to Cancelled. Already-dispatched tasks are left
//     running and will finish naturally; the record itself stays Running
//     until finalize converges. Returns the count of cancelled tasks so the
//     caller can distinguish "everything was already in flight" (0) from
//     "some pending tasks were skipped" (>0).
//   - Terminal / unknown states: read-only response, no mutation.
//
// Only "record not found" and STM / IO failures return an error.
func (s *RebuildService) CancelRebuild(ctx context.Context, dbName, spaceName string) (*entity.CancelRebuildResponse, error) {
	key := entity.RebuildSpaceKey(dbName, spaceName)
	rec, err := s.loadRecord(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load rebuild record: %v", err)
	}
	if rec == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_REBUILD_RECORD_NOT_EXIST,
			fmt.Errorf("no rebuild record found for %s/%s", dbName, spaceName))
	}

	// classify maps a record's current terminal / read-only status to a
	// CancelRebuildResponse. The Pending and Running paths mutate below.
	classify := func(cur *SpaceRebuildRecord, reasonOverride string) (*entity.CancelRebuildResponse, error) {
		resp := &entity.CancelRebuildResponse{
			DBName:    dbName,
			SpaceName: spaceName,
			Status:    cur.Status,
		}
		switch cur.Status {
		case entity.RebuildStatusCompleted, entity.RebuildStatusFailed:
			resp.Reason = fmt.Sprintf("rebuild already %s, cannot cancel", cur.Status)
		case entity.RebuildStatusCancelled:
			resp.Cancelled = true
			resp.Reason = "already cancelled"
		case entity.RebuildStatusPending:
			// Only reachable when caller has decided not to mutate (e.g. the
			// CAS failed and reload found the record still Pending — unlikely).
			resp.Reason = "rebuild is pending; retry cancel"
		default:
			return nil, fmt.Errorf("unknown rebuild status %q for %s/%s", cur.Status, dbName, spaceName)
		}
		if reasonOverride != "" {
			resp.Reason = reasonOverride
		}
		return resp, nil
	}

	switch rec.Status {
	case entity.RebuildStatusPending:
		// Cancel the whole record with STM to avoid racing pending -> running admission.
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
				Status:    entity.RebuildStatusCancelled,
			}, nil
		}
		// Status changed under us; reload and re-dispatch.
		rec2, err2 := s.loadRecord(ctx, key)
		if err2 != nil {
			return nil, fmt.Errorf("reload after CAS conflict: %v", err2)
		}
		if rec2 == nil {
			return nil, fmt.Errorf("no rebuild record found for %s/%s (disappeared after CAS conflict)", dbName, spaceName)
		}
		if rec2.Status == entity.RebuildStatusRunning {
			// Fall through into the Running best-effort path.
			rec = rec2
		} else {
			return classify(rec2,
				fmt.Sprintf("rebuild status changed to %q before cancel could apply", rec2.Status))
		}
		fallthrough

	case entity.RebuildStatusRunning:
		n, err := s.casCancelRunningTasks(ctx, key)
		if err != nil {
			return nil, err
		}
		// The record was flagged CancelRequested inside the same STM; finalize
		// will refuse to advance to any remaining Indexes[] target.
		const suffix = " no further index targets will be started"
		reason := ""
		switch {
		case n == 0:
			reason = "rebuild is running and every task is already dispatched or terminal; already-dispatched tasks will run to completion;" + suffix
		case n == 1:
			reason = "cancelled 1 not-yet-dispatched task; already-dispatched tasks will run to completion;" + suffix
		default:
			reason = fmt.Sprintf("cancelled %d not-yet-dispatched tasks; already-dispatched tasks will run to completion;%s", n, suffix)
		}
		log.Info("cancel running rebuild for %s/%s: cancelled %d pending task(s), CancelRequested=true", dbName, spaceName, n)
		return &entity.CancelRebuildResponse{
			DBName:         dbName,
			SpaceName:      spaceName,
			Cancelled:      false, // record itself stays Running until finalize converges it
			CancelledTasks: n,
			Reason:         reason,
			Status:         entity.RebuildStatusRunning,
		}, nil

	default:
		return classify(rec, "")
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
		if rec.Status != entity.RebuildStatusPending {
			conflict = true
			return nil
		}
		rec.Status = entity.RebuildStatusCancelled
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

// casCancelRunningTasks marks the record's CancelRequested flag and every
// not-yet-dispatched, non-terminal task of a Running record as Cancelled.
// The record itself stays Running; the scheduler will observe the flag +
// Cancelled tasks on the next tick, skip cancelled entries during
// dispatchPending, refuse to advance to the next Indexes[] target in
// finalize, and converge the record once every remaining (already-dispatched)
// task reaches a terminal state.
func (s *RebuildService) casCancelRunningTasks(ctx context.Context, key string) (int, error) {
	cancelled := 0
	err := s.client.Master().STM(ctx, func(stm concurrency.STM) error {
		cancelled = 0
		raw := stm.Get(key)
		if raw == "" {
			return nil
		}
		rec := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal([]byte(raw), rec); err != nil {
			return fmt.Errorf("unmarshal in CAS cancel running: %v", err)
		}
		if rec.Status != entity.RebuildStatusRunning {
			return nil
		}
		now := time.Now()
		for _, t := range rec.Tasks {
			if t.Status != entity.RebuildStatusPending {
				continue
			}
			t.Status = entity.RebuildStatusCancelled
			t.ErrorMessage = "cancelled by user before dispatch"
			t.CompleteTime = now
			cancelled++
		}
		// Short-circuit only when the flag is already set AND no new task-
		// level cancels were applied — otherwise we still need to persist.
		if rec.CancelRequested && cancelled == 0 {
			return nil
		}
		rec.CancelRequested = true
		value, err := vjson.Marshal(rec)
		if err != nil {
			return err
		}
		stm.Put(key, string(value))
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("STM cancel running tasks: %v", err)
	}
	return cancelled, nil
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

// rebuildProgressFromRecord converts the persistent record into the API response.
func rebuildProgressFromRecord(rec *SpaceRebuildRecord) *RebuildProgressResponse {
	resp := &RebuildProgressResponse{
		SpaceKey:       rec.SpaceKey(),
		Status:         rec.Status,
		TotalTasks:     rec.TotalTasks,
		CompletedTasks: rec.CompletedTasks,
		FailedTasks:    rec.FailedTasks,
		ErrorMsg:       rec.ErrorMsg,
		EnqueuedAt:     rec.EnqueuedAt,
		StartedAt:      rec.StartedAt,
		FinishedAt:     rec.FinishedAt,
		MaxRetries:     rec.MaxRetries,
		Tasks:          rec.Tasks,
		Indexes:        rec.Indexes,
		// CurrentIndex is 1-based for API users.
		CurrentIndex:  clampOneBased(rec.CurrentIndexIdx, len(rec.Indexes)),
		CurrentTarget: rec.CurrentTarget(),
	}
	// Build task counts and aggregate retry count.
	cancelled := 0
	for _, t := range rec.Tasks {
		resp.RetryCount += t.RetryCount
		switch t.Status {
		case entity.RebuildStatusPending:
			resp.PendingTasks++
		case entity.RebuildStatusRunning:
			resp.RunningTasks++
		case entity.RebuildStatusCancelled:
			// Cancelled before dispatch on a user cancel: never ran, excluded
			// from the overall_percent denominator below.
			cancelled++
		case entity.RebuildStatusCompleted, entity.RebuildStatusFailed:
			// Counted via resp.CompletedTasks / FailedTasks; no per-task work here.
		}
	}
	if resp.TotalTasks > 0 {
		resp.SuccessRatio = float64(resp.CompletedTasks) / float64(resp.TotalTasks)
		// overall_percent reflects replicas that have actually completed. Running
		// and pending tasks contribute 0: a running replica at progress 100 is
		// still backfilling and not yet complete (completion is gated on backfill
		// catch-up), so it must not inflate the bar. Cancelled tasks leave the
		// denominator (they never ran); failed tasks stay in it and hold the bar
		// below 100.
		if denom := resp.TotalTasks - cancelled; denom > 0 {
			resp.OverallPercent = resp.CompletedTasks * 100 / denom
		}
	}
	return resp
}

// ---------------------------------------------------------------------------
// Pre-flight helpers
// ---------------------------------------------------------------------------

// selectPartitions returns the target partitions from a space:
// partitionID=0 selects all partitions; otherwise the one matching id.
func selectPartitions(space *entity.Space, partitionID uint32) ([]*entity.Partition, error) {
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

	// Global invariant (INV-0): at most one running rebuild record cluster-wide.
	// Phase 1 advances all currently-running records (may be >1 during an
	// upgrade window that carried over the old parallel scheduler); Phase 2
	// admits a new pending record only when Phase 1 leaves the cluster idle.
	// In-record serialism (one task per PS, one replica per partition) is
	// still enforced inside dispatchPending().

	// Phase 1: advance every running record (poll / dispatch / finalize).
	runningCount := 0
	// Records already Failed at scan time are eligible for idle self-heal.
	// Records finalized to Failed by Phase 1 below are deliberately excluded,
	// so a fresh failure stays observable for at least one tick before it is
	// retried.
	var failedAtScan []*SpaceRebuildRecord
	for _, rec := range records {
		if rec.Status == entity.RebuildStatusRunning {
			runningCount++
		}
		if rec.Status == entity.RebuildStatusFailed {
			failedAtScan = append(failedAtScan, rec)
		}
	}
	if runningCount > 1 {
		log.Warn("rebuild scheduler observed %d running records; expected <=1 (INV-0)", runningCount)
	}
	for _, rec := range records {
		if rec.Status == entity.RebuildStatusRunning {
			sc.advanceRunningRecord(ctx, rec)
		}
	}

	// Phase 2: at most one pending admission per tick, and only when
	// nothing is running after Phase 1. reconcileRunning may have moved
	// records to terminal / pending states, so re-check rec.Status here.
	for _, rec := range records {
		if rec.Status == entity.RebuildStatusRunning {
			return
		}
	}
	pending := make([]*SpaceRebuildRecord, 0)
	for _, rec := range records {
		if rec.Status == entity.RebuildStatusPending {
			pending = append(pending, rec)
		}
	}
	if len(pending) == 0 {
		// Nothing new to admit: use the idle slot to retry records that were
		// already failed before this tick.
		sc.retryFailedRecord(ctx, failedAtScan)
		return
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].EnqueuedAt.Before(pending[j].EnqueuedAt)
	})
	sc.admitPending(ctx, pending[0])
}

// ---------------------------------------------------------------------------
// admitPending: transition one pending record to running. Caller (tick)
// guarantees no other running record exists (INV-0).
// ---------------------------------------------------------------------------

func (sc *RebuildScheduler) admitPending(ctx context.Context,
	rec *SpaceRebuildRecord) {

	mc := sc.client.Master()

	dbID, err := mc.QueryDBName2ID(ctx, rec.DBName)
	if err != nil {
		log.Warn("pending %s: query db: %v", rec.SpaceKey(), err)
		return
	}
	space, err := mc.QuerySpaceByName(ctx, dbID, rec.SpaceName)
	if err != nil || space == nil {
		log.Warn("pending %s: space gone, dropping record", rec.SpaceKey())
		rec.Status = entity.RebuildStatusFailed
		rec.ErrorMsg = "space not found"
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}

	// Resolve target partitions.
	partitions, err := selectPartitions(space, rec.PartitionID)
	if err != nil {
		log.Warn("pending %s: %v, marking as failed", rec.SpaceKey(), err)
		rec.Status = entity.RebuildStatusFailed
		rec.ErrorMsg = err.Error()
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}

	// Build the candidate task plan.
	tasks := make([]*RebuildTask, 0)
	// All tasks in this pass rebuild the current target index name.
	target := rec.CurrentTarget()
	if target == "" {
		log.Warn("pending %s: no current rebuild target (Indexes=%v, Idx=%d), marking as failed",
			rec.SpaceKey(), rec.Indexes, rec.CurrentIndexIdx)
		rec.Status = entity.RebuildStatusFailed
		rec.ErrorMsg = fmt.Sprintf("no current rebuild target (Indexes=%v, Idx=%d)", rec.Indexes, rec.CurrentIndexIdx)
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}
	for _, p := range partitions {
		tasks = append(tasks, sc.buildReplicaTasks(ctx, rec, p, target, indexTypeOf(space, target), rec.DropBefore)...)
	}
	if len(tasks) == 0 {
		log.Warn("pending %s: no replicas resolved, marking as failed", rec.SpaceKey())
		rec.Status = entity.RebuildStatusFailed
		rec.ErrorMsg = "no replicas resolved for rebuild"
		rec.FinishedAt = time.Now()
		_ = sc.persistRecord(ctx, rec)
		return
	}

	// Admit: transition pending -> running and attach the task plan.
	// Cross-space PS occupancy is no longer checked (INV-0 is enforced
	// by the tick-level check that no other record is running).
	rec.Status = entity.RebuildStatusRunning
	rec.StartedAt = time.Now()
	rec.TotalTasks = len(tasks)
	rec.CompletedTasks = 0
	rec.FailedTasks = 0
	rec.Tasks = tasks

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
	log.Info("space %s admitted, totalReplicas=%d", rec.SpaceKey(), len(tasks))

	// Dispatch initial tasks in this tick.
	sc.dispatchPending(ctx, rec)

	// Persist dispatch state changes.
	if err := sc.persistRecord(ctx, rec); err != nil {
		log.Error("persist post-dispatch admission %s: %v", rec.SpaceKey(), err)
	}
}

// retryFailedRecord retries one failed partition when the scheduler is idle.
// It picks the failed task retried least recently and revives every
// non-completed replica of that task's partition, so recovery rounds rotate
// fairly across partitions.
func (sc *RebuildScheduler) retryFailedRecord(ctx context.Context,
	records []*SpaceRebuildRecord) {

	// Retry the earliest-failed record first for fairness.
	sort.Slice(records, func(i, j int) bool {
		return records[i].FinishedAt.Before(records[j].FinishedAt)
	})
	for _, rec := range records {
		if rec.Status != entity.RebuildStatusFailed {
			continue
		}
		if rec.CancelRequested {
			continue // user abandoned this rebuild; do not resurrect it
		}
		// Pick the failed task retried least recently so rounds rotate.
		var seed *RebuildTask
		for _, t := range rec.Tasks {
			if t.Status != entity.RebuildStatusFailed {
				continue // completed stay; cancelled handled per-partition below
			}
			if seed == nil || t.CompleteTime.Before(seed.CompleteTime) {
				seed = t
			}
		}
		if seed == nil {
			continue
		}

		// Revive the WHOLE partition, not just the failed task: reset every
		// non-completed replica of seed's partition — the failed task plus any
		// siblings R1 cancelled when it exhausted retries — back to Pending.
		// A lone failed leader replica can never rebuild: leader-last defers it
		// until a follower is rebuilt, but its only followers were cancelled and
		// are ineligible leadership-transfer targets, so ensureLeaderMovedAway
		// fails every round and self-heal spins forever. Rebuilding the
		// followers first restores an eligible transfer target and lets the
		// partition (and thus the record) converge. dispatchPending still sends
		// at most one task, so INV-0 (single running task) holds;
		// advanceRunningRecord drives the remaining replicas across later ticks.
		revived := 0
		for _, t := range rec.Tasks {
			if t.PartitionID != seed.PartitionID {
				continue
			}
			if t.Status == entity.RebuildStatusCompleted {
				continue // already rebuilt this round; a valid transfer target
			}
			t.RetryCount = 0 // reset so the normal path regains MaxRetries retries
			t.Status = entity.RebuildStatusPending
			t.Progress = 0
			t.ErrorMessage = ""
			t.DispatchAttempts = 0
			t.PollRetryCount = 0
			t.DropBefore = 0 // retries must never re-drop the index
			t.StartTime = time.Time{}
			t.CompleteTime = time.Time{}
			revived++
		}

		rec.Status = entity.RebuildStatusRunning
		rec.ErrorMsg = ""
		rec.FinishedAt = time.Time{}
		sc.recountTaskCounters(rec)

		// Reuse the normal dispatch path; it sends one task (leader-last).
		sc.dispatchPending(ctx, rec)
		if err := sc.persistRecord(ctx, rec); err != nil {
			log.Error("persist idle retry %s: %v", rec.SpaceKey(), err)
		}
		log.Info("idle retry: space=%s pid=%d revived %d non-completed replica(s)",
			rec.SpaceKey(), seed.PartitionID, revived)
		return // one partition per idle tick keeps rotation fair
	}
}

// ---------------------------------------------------------------------------
// reconcileRunning: poll active tasks, dispatch the next task per PS,
// finalize the space if all tasks are terminal.
// ---------------------------------------------------------------------------

func (sc *RebuildScheduler) advanceRunningRecord(ctx context.Context,
	rec *SpaceRebuildRecord) {

	dirty := false

	// (a) Poll in-flight (Running) tasks.
	for _, t := range rec.Tasks {
		if t.Status != entity.RebuildStatusRunning {
			continue
		}
		resp, err := client.GetRebuildStatus(t.PSNodeAddr, rec.DBName, rec.SpaceName,
			t.IndexName, t.PartitionID)
		if err != nil {
			t.PollRetryCount++
			// Persist every streak increment so restarts do not reset it.
			dirty = true
			log.Warn("GetRebuildStatus %s pid=%d nodeID=%d (streak=%d/%d): %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID,
				t.PollRetryCount, maxPollRetries, err)
			// Stop polling forever once the failure streak crosses the budget.
			if t.PollRetryCount >= maxPollRetries {
				sc.handleReplicaFailure(rec, t,
					fmt.Sprintf("GetRebuildStatus failed %d consecutive times: %v",
						t.PollRetryCount, err))
			}
			continue // will retry next tick (or finalize if marked failed)
		}
		// Successful poll resets the streak.
		if t.PollRetryCount > 0 {
			t.PollRetryCount = 0
			dirty = true
		}

		if !resp.Exists {
			// Missing PS task is terminal; handleReplicaFailure decides
			// whether to retry in place or cancel siblings.
			sc.handleReplicaFailure(rec, t,
				fmt.Sprintf("ps reports task missing (pid=%d, nodeID=%d)",
					t.PartitionID, t.NodeID))
			dirty = true
			continue
		}

		switch resp.Status {
		case entity.RebuildStatusRunning:
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
		case entity.RebuildStatusCompleted:
			t.Status = entity.RebuildStatusCompleted
			t.CompleteTime = time.Now()
			t.Progress = 100
			t.ArtifactsPublished = resp.ArtifactsPublished
			dirty = true
		case entity.RebuildStatusFailed:
			sc.handleReplicaFailure(rec, t, resp.ErrorMessage)
			dirty = true
		default:
			log.Warn("unknown PS rebuild status %q for %s pid=%d",
				resp.Status, rec.SpaceKey(), t.PartitionID)
		}
	}

	// Clear Rebuilding markers for tasks that reached a terminal state in (a),
	// BEFORE dispatching the next task. Leader-last means dispatchPending may
	// need to transfer leadership onto a follower that just finished rebuilding
	// this same tick; ensureLeaderMovedAway only accepts a follower whose
	// persisted replica status is ReplicasOK. Clearing first flips that
	// follower's marker back to OK so it is an eligible transfer target now;
	// otherwise its stale ReplicasRebuildingIndex marker bars the transfer and
	// the leader replica is wrongly failed with "no healthy follower".
	sc.unmarkRebuildingForTerminalTasks(ctx, rec)

	// (b) Dispatch pending tasks under the per-PS-serial constraint.
	if sc.dispatchPending(ctx, rec) {
		dirty = true
	}

	// (c) Recompute counters. Cross-record PS occupancy no longer exists;
	// INV-0 guarantees this record is the only running one.
	sc.recountTaskCounters(rec)

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
		if !t.Status.IsTerminal() {
			allTerminal = false
			break
		}
	}
	if allTerminal {
		sc.finalize(ctx, rec)
	}
}

// trainingArtifactsSharingSupported reports whether an index family supports
// sharing its training artifacts across replicas.
func trainingArtifactsSharingSupported(indexType string) bool {
	switch indexType {
	case "IVFPQ", "IVFFLAT", "IVFRABITQ", "IVFPQFastScan", "BINARYIVF",
		"GPU_IVFFLAT", "GPU_IVFPQ", "NPU_IVFFLAT", "NPU_IVFRABITQ":
		return true
	default:
		return false
	}
}

// partitionReplicaCount returns how many replica tasks the record holds for one
// partition (used to decide whether the training-artifacts-sharing scheme applies).
func partitionReplicaCount(rec *SpaceRebuildRecord, pid entity.PartitionID) int {
	n := 0
	for _, t := range rec.Tasks {
		if t.PartitionID == pid {
			n++
		}
	}
	return n
}

// trainerTaskOf returns the partition's designated trainer task (IsTrainer), or
// nil if none has been chosen yet this round.
func trainerTaskOf(rec *SpaceRebuildRecord, pid entity.PartitionID) *RebuildTask {
	for _, t := range rec.Tasks {
		if t.PartitionID == pid && t.IsTrainer {
			return t
		}
	}
	return nil
}

// modelSourceAddrOf picks a live replica that already holds this round's model
// and can serve it to a follower: any completed replica of
// the partition (trainer or an already-installed follower) whose node is still
// registered, excluding the puller itself. Preferring any completed replica —
// not just the trainer — lets the round survive the trainer's node dying after
// it finished (§10). Returns "" and refreshes the chosen task's PSNodeAddr.
func (sc *RebuildScheduler) modelSourceAddrOf(ctx context.Context, rec *SpaceRebuildRecord,
	pid entity.PartitionID, excludeNodeID entity.NodeID) string {
	mc := sc.client.Master()
	var trainerAddr, fallbackAddr string
	for _, t := range rec.Tasks {
		if t.PartitionID != pid || t.NodeID == excludeNodeID {
			continue
		}
		if t.Status != entity.RebuildStatusCompleted || !t.ArtifactsPublished {
			continue // only a replica that actually published this round's model is a source
		}
		// A registered server (TTL-backed) is our liveness signal; refresh the addr.
		server, err := mc.QueryServer(ctx, t.NodeID)
		if err != nil || server == nil {
			continue
		}
		if t.IsTrainer {
			trainerAddr = server.RpcAddr()
		} else if fallbackAddr == "" {
			fallbackAddr = server.RpcAddr()
		}
	}
	if trainerAddr != "" {
		return trainerAddr
	}
	return fallbackAddr
}

// dispatchPending sends pending tasks one at a time. Within a record, tasks
// run strictly serially: the next task is dispatched only after every
// previously dispatched task has reached a terminal state. This is enforced
// on top of INV-0 (one running record cluster-wide), giving the whole
// scheduler a single active task at any moment.
func (sc *RebuildScheduler) dispatchPending(ctx context.Context, rec *SpaceRebuildRecord) bool {
	// If any task in this record is already in flight, don't dispatch more.
	for _, t := range rec.Tasks {
		if t.Status == entity.RebuildStatusRunning {
			return false
		}
	}

	mc := sc.client.Master()
	dirty := false
	for _, t := range rec.Tasks {
		if t.Status != entity.RebuildStatusPending {
			continue
		}

		// Rebuild the leader replica LAST. If this pending task's replica is
		// currently its partition's leader AND the partition still has another
		// pending (not-yet-rebuilt) replica, defer it: rebuild the followers
		// first. By the time only the leader replica is left, every other
		// replica is already rebuilt, so ensureLeaderMovedAway can transfer to
		// any of them — exactly one transfer per partition, and no replica is
		// skipped even for a full-space rebuild.
		if sc.hasOtherPendingReplica(rec, t.PartitionID, t.NodeID) {
			part, perr := mc.QueryPartition(ctx, t.PartitionID)
			if perr == nil && part != nil && part.LeaderID == t.NodeID {
				log.Debug("deferring leader replica rebuild: space=%s pid=%d nodeID=%d (followers first)",
					rec.SpaceKey(), t.PartitionID, t.NodeID)
				continue
			}
		}

		// Refresh PSNodeAddr before every dispatch
		server, qerr := mc.QueryServer(ctx, t.NodeID)
		if qerr != nil || server == nil {
			log.Error("QueryServer nodeID=%d failed for %s pid=%d: %v; failing replica",
				t.NodeID, rec.SpaceKey(), t.PartitionID, qerr)
			sc.handleReplicaFailure(rec, t,
				fmt.Sprintf("server nodeID=%d unregistered: %v", t.NodeID, qerr))
			return true
		}
		t.PSNodeAddr = server.RpcAddr()

		// If this replica is the current partition leader, move leadership to a
		// healthy follower first so the rebuild runs on a follower and
		// leader-typed reads stay available. If leadership cannot be moved
		// (no healthy follower / transfer timed out), skip rebuilding this
		// replica rather than take the leader offline for a rebuild.
		if skip, serr := sc.ensureLeaderMovedAway(ctx, rec, t); serr != nil {
			log.Warn("leader transfer before rebuild %s pid=%d nodeID=%d failed: %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID, serr)
			if skip {
				markReplicaFailed(t, fmt.Sprintf(
					"skipped: replica is leader and leadership could not be moved: %v", serr))
				return true
			}
		}

		// Coordinate one training per
		// partition. The first replica dispatched for a partition (a follower,
		// by leader-last ordering) becomes the trainer and dumps its model; every
		// later replica pulls from it instead of training. Strict-serial dispatch
		// (one live task at a time, next only after the current completes)
		// guarantees the trainer has completed — its model already on disk —
		// before any follower is dispatched. Single-replica partitions get no
		// round (nothing to share).
		var roundID, trainerAddr string
		var isTrainer bool
		if rec.RebuildID != "" && partitionReplicaCount(rec, t.PartitionID) > 1 &&
			trainingArtifactsSharingSupported(t.IndexType) {
			roundID = rec.RebuildID
			if trainer := trainerTaskOf(rec, t.PartitionID); trainer == nil {
				t.IsTrainer = true // first replica of this partition → trainer
				isTrainer = true
			} else if trainer.NodeID == t.NodeID {
				isTrainer = true // re-dispatch of the same trainer
			} else if trainer.Status == entity.RebuildStatusCompleted && !trainer.ArtifactsPublished {
				// No-model round: the trainer completed without a shared model
				// (untrained/below threshold). Rebuild locally so every replica lands
				// in the same untrained state; do not pull.
				roundID = ""
			} else {
				// Follower: pull from any live replica already holding the model.
				trainerAddr = sc.modelSourceAddrOf(ctx, rec, t.PartitionID, t.NodeID)
				if trainerAddr == "" {
					// No live source (every completed replica's node is gone).
					// Fail the replica rather than train locally (which would
					// produce an inconsistent index); retry/new-round re-trains.
					sc.handleReplicaFailure(rec, t,
						fmt.Sprintf("no live training-artifacts source for pid=%d round=%s", t.PartitionID, roundID))
					return true
				}
			}
		}

		t.DispatchAttempts++
		t.DispatchAt = time.Now()
		t.StartTime = time.Now()
		err := client.ExecuteRebuildIndex(t.PSNodeAddr, rec.DBName, rec.SpaceName,
			t.IndexName, t.PartitionID,
			t.DropBefore, t.LimitCPU, t.Describe,
			roundID, isTrainer, trainerAddr)
		if err != nil {
			// Retry transient dispatch failures before failing the replica.
			t.ErrorMessage = err.Error()
			dirty = true
			if t.DispatchAttempts >= maxDispatchAttempts {
				log.Error("ExecuteRebuildIndex %s pid=%d nodeID=%d gave up after %d attempts: %v",
					rec.SpaceKey(), t.PartitionID, t.NodeID, t.DispatchAttempts, err)
				sc.handleReplicaFailure(rec, t,
					fmt.Sprintf("ExecuteRebuildIndex failed %d times: %v",
						t.DispatchAttempts, err))
				return true
			}
			log.Warn("ExecuteRebuildIndex %s pid=%d nodeID=%d failed (attempt=%d/%d), will retry: %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID,
				t.DispatchAttempts, maxDispatchAttempts, err)
			// Non-terminal transient failure: retry this same task next tick.
			return dirty
		}
		t.Status = entity.RebuildStatusRunning
		dirty = true
		log.Info("rebuild dispatched: space=%s pid=%d nodeID=%d (attempt=%d)",
			rec.SpaceKey(), t.PartitionID, t.NodeID, t.DispatchAttempts)
		// Mark this replica as Rebuilding in the partition record.
		if err := sc.setReplicaRebuildStatus(ctx, t.PartitionID, t.NodeID, entity.ReplicasRebuildingIndex); err != nil {
			log.Warn("setReplicaRebuildStatus(rebuilding) failed for pid=%d nodeID=%d: %v",
				t.PartitionID, t.NodeID, err)
		}
		// Strict serialism: at most one live task at any moment.
		return dirty
	}
	return dirty
}

// recountTaskCounters refreshes Total / Completed / Failed from rec.Tasks.
// Called after any mutation of rec.Tasks (dispatch, poll result, replan);
// TotalReplicas may change when finalize replans a partition with a different
// live-replica count.
func (sc *RebuildScheduler) recountTaskCounters(rec *SpaceRebuildRecord) {
	completed, failed := 0, 0
	for _, t := range rec.Tasks {
		switch t.Status {
		case entity.RebuildStatusCompleted:
			completed++
		case entity.RebuildStatusFailed:
			failed++
		}
	}
	rec.TotalTasks = len(rec.Tasks)
	rec.CompletedTasks = completed
	rec.FailedTasks = failed
}

// ensureLeaderMovedAway makes sure task t's replica is NOT the partition
// leader before its index is rebuilt, by transferring leadership to a healthy
// follower. Rebuilding the leader's replica would make leader-typed reads fail
// for the whole rebuild; moving leadership first keeps them served.
//
// Returns (skip, err):
//   - (false, nil): the replica is already a follower (or became one) — safe
//     to rebuild it now.
//   - (true, err): the replica is the leader and leadership could NOT be moved
//     (no healthy follower, RPC error, or transfer did not take effect within
//     leaderTransferTimeout) — caller should skip rebuilding this replica.
func (sc *RebuildScheduler) ensureLeaderMovedAway(ctx context.Context,
	rec *SpaceRebuildRecord, t *RebuildTask) (bool, error) {

	mc := sc.client.Master()
	part, err := mc.QueryPartition(ctx, t.PartitionID)
	if err != nil || part == nil {
		return true, fmt.Errorf("query partition %d: %v", t.PartitionID, err)
	}
	// Not the leader → nothing to do, rebuild may proceed.
	if part.LeaderID != t.NodeID {
		return false, nil
	}

	// Single-replica partition: there is no follower to move leadership to.
	// Rebuild in place (as before this feature existed) rather than skip —
	// leader-typed reads will fail during the rebuild, an inherent limitation
	// of a single replica, not a reason to abort the rebuild.
	if len(part.Replicas) <= 1 {
		log.Info("single-replica partition %d: rebuilding leader in place (no transfer possible)",
			t.PartitionID)
		return false, nil
	}

	// Pick a healthy follower to become the new leader: registered, ReplicasOK,
	// not the current leader, and not itself a rebuild target of this record.
	// A follower that just finished rebuilding reads as ReplicasOK here only
	// because advanceRunningRecord clears its marker
	// (unmarkRebuildingForTerminalTasks) BEFORE dispatching this leader task.
	target := entity.NodeID(0)
	var targetAddr string
	for _, nodeID := range part.Replicas {
		if nodeID == t.NodeID {
			continue
		}
		if part.ReStatusMap[uint64(nodeID)] != entity.ReplicasOK {
			continue
		}
		if sc.nodeIsRebuildTarget(rec, t.PartitionID, nodeID) {
			continue
		}
		srv, qerr := mc.QueryServer(ctx, nodeID)
		if qerr != nil || srv == nil {
			continue
		}
		target = nodeID
		targetAddr = srv.RpcAddr()
		break
	}
	if target == 0 {
		return true, fmt.Errorf("no healthy follower to take leadership of partition %d", t.PartitionID)
	}

	// Ask the target to campaign. RPC success only means the campaign started;
	// confirm by polling the partition LeaderID.
	if terr := client.TransferLeader(targetAddr, t.PartitionID); terr != nil {
		return true, fmt.Errorf("transfer leader to nodeID=%d: %v", target, terr)
	}
	log.Info("leader transfer requested: space=%s pid=%d %d->%d, awaiting confirmation",
		rec.SpaceKey(), t.PartitionID, t.NodeID, target)

	deadline := time.Now().Add(leaderTransferTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			// Scheduler tick is being torn down (master step-down / shutdown /
			// 30s tick deadline). Stop waiting; the transfer was not confirmed,
			// so report it as not-moved (skip rebuilding this leader replica).
			return true, fmt.Errorf("leader transfer to nodeID=%d aborted: %w",
				target, ctx.Err())
		case <-time.After(leaderTransferPollInterval):
		}
		latest, qerr := mc.QueryPartition(ctx, t.PartitionID)
		if qerr != nil || latest == nil {
			continue
		}
		if latest.LeaderID != t.NodeID {
			log.Info("leader transfer confirmed: space=%s pid=%d new leader=%d",
				rec.SpaceKey(), t.PartitionID, latest.LeaderID)
			return false, nil
		}
	}
	return true, fmt.Errorf("leader transfer to nodeID=%d did not take effect within %s",
		target, leaderTransferTimeout)
}

// nodeIsRebuildTarget reports whether (pid, nodeID) is a non-terminal task in
// this record — i.e. it will itself be rebuilt, so it is a poor transfer target.
func (sc *RebuildScheduler) nodeIsRebuildTarget(rec *SpaceRebuildRecord,
	pid entity.PartitionID, nodeID entity.NodeID) bool {
	for _, t := range rec.Tasks {
		if t.PartitionID == pid && t.NodeID == nodeID && !t.Status.IsTerminal() {
			return true
		}
	}
	return false
}

// hasOtherPendingReplica reports whether the partition has another replica
// task still Pending besides the one on excludeNode. Used to defer rebuilding
// the leader replica until its followers are done.
func (sc *RebuildScheduler) hasOtherPendingReplica(rec *SpaceRebuildRecord,
	pid entity.PartitionID, excludeNode entity.NodeID) bool {
	for _, t := range rec.Tasks {
		if t.PartitionID == pid && t.NodeID != excludeNode &&
			t.Status == entity.RebuildStatusPending {
			return true
		}
	}
	return false
}

// markReplicaFailed sets a per-replica task to terminal failed state.
func markReplicaFailed(t *RebuildTask, msg string) {
	t.Status = entity.RebuildStatusFailed
	t.ErrorMessage = msg
	t.CompleteTime = time.Now()
}

// handleReplicaFailure is the single decision point when a replica task
// hits a failure signal (poll streak exceeded, PS reports task missing,
// PS reports Failed, or dispatch RPC gave up).
func (sc *RebuildScheduler) handleReplicaFailure(rec *SpaceRebuildRecord,
	t *RebuildTask, msg string) {
	if t.RetryCount < rec.MaxRetries {
		t.RetryCount++
		t.Status = entity.RebuildStatusPending
		t.Progress = 0
		t.ErrorMessage = ""
		t.DispatchAttempts = 0
		t.PollRetryCount = 0
		// Retries must not drop existing index data again.
		t.DropBefore = 0
		t.StartTime = time.Time{}
		t.CompleteTime = time.Time{}
		log.Info("replica in-place retry: space=%s pid=%d nodeID=%d retry=%d/%d reason=%q",
			rec.SpaceKey(), t.PartitionID, t.NodeID,
			t.RetryCount, rec.MaxRetries, msg)
		return
	}
	// Retry budget exhausted for this partition.
	markReplicaFailed(t, msg)
	cancelled := 0
	for _, sib := range rec.Tasks {
		if sib == t || sib.PartitionID != t.PartitionID {
			continue
		}
		if sib.Status != entity.RebuildStatusPending {
			continue
		}
		sib.Status = entity.RebuildStatusCancelled
		sib.ErrorMessage = fmt.Sprintf(
			"skipped: sibling replica nodeID=%d exhausted retries; preserving remaining replicas of partition %d",
			t.NodeID, t.PartitionID)
		sib.CompleteTime = time.Now()
		cancelled++
	}
	if cancelled > 0 {
		log.Info("replica retry exhausted: space=%s pid=%d failed nodeID=%d; cancelled %d sibling replica task(s) to keep partition available",
			rec.SpaceKey(), t.PartitionID, t.NodeID, cancelled)
	}
}

// setReplicaRebuildStatus transitions the router-visible rebuild marker for a
// replica. Setting ReplicasRebuildingIndex is the dispatch-time mark. Clearing
// to ReplicasOK / ReplicasRebuildFailed only fires when the replica is
// currently ReplicasRebuildingIndex, so it never clobbers a concurrently-set
// ReplicasNotReady (raft-lag) status. Setting ReplicasRebuildingIndex from a
// prior ReplicasRebuildFailed is allowed, so an idle self-heal retry re-marks a
// previously-failed replica as rebuilding again.
func (sc *RebuildScheduler) setReplicaRebuildStatus(ctx context.Context,
	pid entity.PartitionID, nodeID entity.NodeID, target uint32) error {

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
		if target == entity.ReplicasRebuildingIndex {
			if cur == entity.ReplicasRebuildingIndex {
				return nil // already set, no-op
			}
		} else if cur != entity.ReplicasRebuildingIndex {
			return nil // only clear from Rebuilding; don't clobber NotReady etc.
		}
		p.ReStatusMap[uint64(nodeID)] = target

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

// unmarkRebuildingForTerminalTasks clears markers for finished tasks. A failed
// task leaves ReplicasRebuildFailed so reads keep avoiding the replica (its
// index may be partial after a drop=true rebuild) until a later rebuild
// succeeds; completed/cancelled tasks return to ReplicasOK.
func (sc *RebuildScheduler) unmarkRebuildingForTerminalTasks(
	ctx context.Context, rec *SpaceRebuildRecord) {
	for _, t := range rec.Tasks {
		if !t.Status.IsTerminal() {
			continue
		}
		target := uint32(entity.ReplicasOK)
		if t.Status == entity.RebuildStatusFailed {
			target = entity.ReplicasRebuildFailed
		}
		if err := sc.setReplicaRebuildStatus(ctx, t.PartitionID, t.NodeID, target); err != nil {
			log.Warn("setReplicaRebuildStatus(reset) %s pid=%d nodeID=%d: %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID, err)
		}
	}
}

// ---------------------------------------------------------------------------
// finalize: handle a record where every replica task is terminal.
// ---------------------------------------------------------------------------
func (sc *RebuildScheduler) finalize(ctx context.Context, rec *SpaceRebuildRecord) {
	spaceKey := rec.SpaceKey()
	// Final safety sweep for stale Rebuilding markers.
	sc.unmarkRebuildingForTerminalTasks(ctx, rec)

	// Count terminal categories for the current target.
	completed, failed, cancelled := 0, 0, 0
	for _, t := range rec.Tasks {
		switch t.Status {
		case entity.RebuildStatusCompleted:
			completed++
		case entity.RebuildStatusFailed:
			failed++
		case entity.RebuildStatusCancelled:
			cancelled++
		}
	}
	total := len(rec.Tasks)

	// If this target had zero failures and more index targets remain, advance
	// in place: keep Status=Running, build the next target's tasks now, so
	// the same space keeps its scheduler slot across the whole Indexes list
	// without yielding to other pending records.
	//
	// CancelRequested short-circuits this advance: a user cancel means the
	// whole rebuild is abandoned, so no new target is planned even if the
	// current one had zero failures.
	if failed == 0 && rec.HasMoreTargets() && !rec.CancelRequested {
		previousTarget := rec.CurrentTarget()
		if err := sc.prepareNextTarget(ctx, rec); err != nil {
			// Cannot plan the next target: fall through to terminal path.
			log.Error("space %s advance target failed: %v — marking record failed", spaceKey, err)
			rec.Status = entity.RebuildStatusFailed
			rec.ErrorMsg = fmt.Sprintf("advance to next target: %v", err)
			rec.FinishedAt = time.Now()
			if perr := sc.persistRecord(ctx, rec); perr != nil {
				log.Error("persist failed advance %s: %v", spaceKey, perr)
			}
			return
		}
		if err := sc.persistRecord(ctx, rec); err != nil {
			log.Error("persist next-target advance %s: %v", spaceKey, err)
			return
		}
		log.Info("space %s advanced rebuild target: %s -> %s (%d/%d targets done, cancelled=%d completed=%d)",
			spaceKey, previousTarget, rec.CurrentTarget(),
			rec.CurrentIndexIdx, len(rec.Indexes), cancelled, completed)
		return
	}

	// Terminal state.
	//   FailedTasks > 0                       → Failed (hard fact wins even
	//                                            when the user also cancelled)
	//   CancelRequested                       → Cancelled (user abandoned the
	//                                            rebuild; any completed tasks
	//                                            already ran to completion for
	//                                            what actually started)
	//   FailedTasks == 0 && completed > 0     → Completed
	//   FailedTasks == 0 && completed == 0    → Cancelled (nothing ran)
	finalStatus := entity.RebuildStatusCompleted
	finalErr := ""
	switch {
	case failed > 0:
		finalStatus = entity.RebuildStatusFailed
		finalErr = fmt.Sprintf("%d/%d replicas failed on target %s (max task retry %d/%d)",
			failed, total, rec.CurrentTarget(),
			maxTaskRetry(rec.Tasks), rec.MaxRetries)
	case rec.CancelRequested:
		finalStatus = entity.RebuildStatusCancelled
		remaining := len(rec.Indexes) - (rec.CurrentIndexIdx + 1)
		finalErr = fmt.Sprintf("cancelled by user on target %s: %d completed, %d cancelled before dispatch; %d subsequent index target(s) skipped",
			rec.CurrentTarget(), completed, cancelled, remaining)
	case completed == 0 && cancelled > 0:
		finalStatus = entity.RebuildStatusCancelled
		finalErr = fmt.Sprintf("all %d tasks cancelled before completion on target %s",
			cancelled, rec.CurrentTarget())
	}

	rec.Status = finalStatus
	rec.ErrorMsg = finalErr
	rec.FinishedAt = time.Now()
	if err := sc.persistRecord(ctx, rec); err != nil {
		log.Error("persist finalized record %s: %v", spaceKey, err)
	}
	log.Info("space %s finalized: status=%s completed=%d failed=%d cancelled=%d retry=%d targets=%d/%d err=%q",
		spaceKey, finalStatus, completed, failed, cancelled,
		maxTaskRetry(rec.Tasks),
		rec.CurrentIndexIdx+1, len(rec.Indexes), finalErr)
}

// prepareNextTarget advances rec.CurrentIndexIdx and rebuilds rec.Tasks for
// the new target from live space metadata. On return the record is ready
// for the next advanceRunningRecord tick to dispatch. Status is left as
// Running so the record never yields its scheduler slot between targets.
func (sc *RebuildScheduler) prepareNextTarget(ctx context.Context,
	rec *SpaceRebuildRecord) error {

	mc := sc.client.Master()
	dbID, err := mc.QueryDBName2ID(ctx, rec.DBName)
	if err != nil {
		return fmt.Errorf("query db: %v", err)
	}
	space, err := mc.QuerySpaceByName(ctx, dbID, rec.SpaceName)
	if err != nil || space == nil {
		return fmt.Errorf("space gone: %v", err)
	}
	partitions, err := selectPartitions(space, rec.PartitionID)
	if err != nil {
		return err
	}

	rec.CurrentIndexIdx++
	target := rec.CurrentTarget()
	if target == "" {
		return fmt.Errorf("no target at index %d (Indexes=%v)", rec.CurrentIndexIdx, rec.Indexes)
	}

	tasks := make([]*RebuildTask, 0)
	for _, p := range partitions {
		// dropBefore=0: once the initial target has been rebuilt, subsequent
		// targets on the same space must never re-drop, same as retries.
		tasks = append(tasks, sc.buildReplicaTasks(ctx, rec, p, target, indexTypeOf(space, target), 0)...)
	}
	if len(tasks) == 0 {
		return fmt.Errorf("no replicas resolved for target %s", target)
	}

	rec.Tasks = tasks
	rec.TotalTasks = len(tasks)
	rec.CompletedTasks = 0
	rec.FailedTasks = 0
	rec.ErrorMsg = ""
	// rec.Status stays Running; rec.StartedAt stays as the whole-record start.
	return nil
}

// buildReplicaTasks expands one partition into a RebuildTask per live
// replica. Replicas whose PS server metadata cannot be resolved are
// skipped (logged). dropBefore is passed separately because retries force
// dropBefore=0 even when the record's DropBefore=1 (retrying must never
// re-drop the index).
func (sc *RebuildScheduler) buildReplicaTasks(ctx context.Context,
	rec *SpaceRebuildRecord, part *entity.Partition, target, indexType string,
	dropBefore int) []*RebuildTask {

	mc := sc.client.Master()
	out := make([]*RebuildTask, 0, len(part.Replicas))
	for replicaIdx, nodeID := range part.Replicas {
		server, qerr := mc.QueryServer(ctx, nodeID)
		if qerr != nil || server == nil {
			log.Warn("%s partition %d: skip replica nodeID=%d: %v",
				rec.SpaceKey(), part.Id, nodeID, qerr)
			continue
		}
		out = append(out, &RebuildTask{
			PartitionID:  part.Id,
			NodeID:       nodeID,
			ReplicaIndex: replicaIdx,
			PSNodeAddr:   server.RpcAddr(),
			DBName:       rec.DBName,
			SpaceName:    rec.SpaceName,
			IndexName:    target,
			// IndexType is normally PS-only, but master populates it so
			// dispatchPending can gate the training-artifacts-sharing scheme by index
			// family (see trainingArtifactsSharingSupported for the supported set).
			IndexType: indexType,
			// Pending means planned but not yet dispatched.
			Status:     entity.RebuildStatusPending,
			DropBefore: dropBefore,
			LimitCPU:   rec.LimitCPU,
			Describe:   rec.Describe,
		})
	}
	return out
}

// indexTypeOf resolves the index family (e.g. "IVFPQ") for target from the space
// schema, or "" if the index is absent.
func indexTypeOf(space *entity.Space, target string) string {
	if space == nil {
		return ""
	}
	if idx := space.GetIndexByName(target); idx != nil {
		return idx.Type
	}
	return ""
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

// maxTaskRetry returns the deepest per-task retry count.
func maxTaskRetry(tasks []*RebuildTask) int {
	max := 0
	for _, t := range tasks {
		if t.RetryCount > max {
			max = t.RetryCount
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
		if current.Status != entity.RebuildStatusPending {
			conflict = true
			return nil
		}
		// rec already contains prepared tasks, counters, and StartedAt.
		rec.Status = entity.RebuildStatusRunning
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

// persistRecord writes the in-memory record back to etcd, preserving any
// task-level Cancelled markers that CancelRebuild may have set between the
// tick's initial load and this write
func (sc *RebuildScheduler) persistRecord(ctx context.Context, rec *SpaceRebuildRecord) error {
	key := entity.RebuildSpaceKey(rec.DBName, rec.SpaceName)
	return sc.client.Master().STM(ctx, func(stm concurrency.STM) error {
		raw := stm.Get(key)
		if raw == "" {
			return fmt.Errorf("persist %s: record gone from etcd", key)
		}
		persisted := &SpaceRebuildRecord{}
		if err := vjson.Unmarshal([]byte(raw), persisted); err != nil {
			return fmt.Errorf("persist %s: unmarshal persisted: %w", key, err)
		}
		if persisted.Status == entity.RebuildStatusCancelled {
			log.Info("persistRecord %s: etcd record is Cancelled; skipping persist", key)
			return nil
		}
		// Merge task-level Cancelled markers so they survive this write.
		if merged := rec.MergeCancelledFrom(persisted); merged > 0 {
			log.Info("persistRecord %s: preserved %d task-level cancels from concurrent CancelRebuild", key, merged)
		}
		value, err := vjson.Marshal(rec)
		if err != nil {
			return fmt.Errorf("persist %s: marshal: %w", key, err)
		}
		stm.Put(key, string(value))
		return nil
	})
}
