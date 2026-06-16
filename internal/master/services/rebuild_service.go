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
//
// NOTE: The "Inited" value (0) is intentionally omitted. The PS side never
// transitions a real task into the Inited state — a task is created already
// in Running and only ever ends in Completed/Failed (see PSRebuildManager).
// A status of 0 in a successful RPC reply therefore means "task not present",
// and that case is handled via RebuildStatusResponse.Exists, not via a status
// constant.
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

// defaultMaxRetries applies when the caller does not specify MaxRetries.
// The same value caps both per-partition retries and the (historically)
// space-wide retries: see PartitionRetries on SpaceRebuildRecord.
const defaultMaxRetries = 3

// maxDispatchAttempts is the per-task upper bound on RPC dispatch
// attempts before the task is marked failed. This caps the initial
// dispatch path only; per-replica resurrection after Exists=false is
// intentionally NOT done — see the !resp.Exists branch in
// reconcileRunning for the reasoning.
const maxDispatchAttempts = 3

// maxPollFailureStreak caps how many consecutive GetRebuildStatus RPC
// failures we tolerate on a single in-flight task before declaring it
// failed. Previously a permanently unreachable PS produced an infinite
// stream of warn logs while the task stayed in Running forever and never
// finalized — both the master and the user were left in limbo. 15 ticks at
// tickInterval=2s gives ~30s of tolerance for transient network blips
// before we give up and let the retry budget take over.
const maxPollFailureStreak = 15

// RebuildService is the public façade.
type RebuildService struct {
	client    *client.Client
	scheduler *RebuildScheduler
}

// NewRebuildService creates a new RebuildService.
//
// The returned service does not run the scheduler tick until Start() is
// called. SetLeaderChecker should be invoked before Start() in a multi-master
// deployment so the scheduler can self-suppress on follower nodes; otherwise
// every master replica would race on the same etcd records (see P0-#1).
func NewRebuildService(c *client.Client) *RebuildService {
	return &RebuildService{
		client:    c,
		scheduler: newRebuildScheduler(c),
	}
}

// SetLeaderChecker installs a predicate that the scheduler consults at the
// beginning of every tick. When it returns false the tick is skipped — this
// is how we make the scheduler safe to construct on every master node while
// only the etcd raft leader actually drives reconciliation.
//
// Passing nil disables the check (useful for single-node tests). The check
// is read under tick's lock so swapping it at runtime is safe.
func (s *RebuildService) SetLeaderChecker(isLeader func() bool) {
	s.scheduler.setLeaderChecker(isLeader)
}

// StartEtcdLeaderCampaign spins up a goroutine that competes for a
// cluster-wide lock in etcd. The returned predicate reports whether THIS
// process is the current lock holder; the scheduler installs it via
// SetLeaderChecker. Designed for the SelfManageEtcd deployment, where the
// master has no embedded etcdserver to consult for raft leadership but
// still must guarantee that only one replica drives reconciliation
// (P1-#3). The campaign owns no other state — Stop()'s scheduler shutdown
// implicitly cancels the goroutine through ctx.
//
// ttl bounds the lock lease: if a leader dies, followers will pick the
// work up at most ttl later. 30s matches the pattern used elsewhere in
// the codebase (see space/role lock TTLs) and is well above tickInterval.
func (s *RebuildService) StartEtcdLeaderCampaign(ctx context.Context, ttl time.Duration) func() bool {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	state := &leaderCampaign{}
	go state.run(ctx, s.client, ttl)
	return state.isLeader
}

// leaderCampaign holds the lock-holder bit shared between the campaign
// goroutine and the scheduler tick. The boolean is updated under the mutex
// so a reader never sees a torn write on architectures with weaker memory
// models than amd64.
type leaderCampaign struct {
	mu       sync.RWMutex
	leader   bool
	leaseRef *struct{} // sentinel to detect ownership changes across cycles
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

// run is the campaign loop. It alternates between two phases:
//
//  1. Acquire: try to grab the rebuild scheduler lock. On failure, sleep
//     and retry — we don't block on Lock() because that would prevent
//     orderly shutdown.
//  2. Hold: while holding the lock, mark this process as leader and
//     refresh the lease every ttl/3. When the lease cannot be refreshed
//     (etcd outage, lock contention, ctx cancellation) we drop back to
//     phase 1.
//
// Any panic inside the loop is logged and the campaign restarts after a
// short cooldown to avoid hot-spinning on persistent errors.
func (lc *leaderCampaign) run(ctx context.Context, c *client.Client, ttl time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("rebuild leader campaign panic: %v", r)
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
		// Hold phase. KeepAliveOnce on a cadence shorter than the TTL
		// keeps the lease alive; on ctx cancellation we release the
		// lock and exit. KeepAliveOnce is best-effort: the DistLock
		// helper does not surface its error, but a fully partitioned
		// etcd will eventually let the lease expire and another
		// replica will pick up leadership on its next TryLock cycle.
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
				lock.KeepAliveOnce()
			}
		}
		holdTicker.Stop()
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

// StartRebuild persists a SpaceRebuildRecord(pending) into etcd and returns
// immediately. The actual rebuild is driven by the scheduler tick.
//
// Pre-flight validation performed before the record is written:
//  1. DB exists.
//  2. Space exists and is enabled.
//  3. Space has at least one index defined.
//  4. No other rebuild for the same space is currently pending or running.
//  5. Target partition(s) are healthy: present in meta, have a leader, replica
//     count matches space.ReplicaNum, every replica's PS server is registered,
//     and per-replica ReStatusMap (when reported) shows ReplicasOK.
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

	// (3) Index existence.
	if len(space.Indexes) == 0 {
		return nil, fmt.Errorf("space %s/%s has no index defined, nothing to rebuild",
			req.Database, req.Space)
	}

	// (4) Resolve the (field, indexType) target list.
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

	// (5) Reject if a non-terminal record already exists.
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
			// Terminal records are kept in etcd so users can query the
			// last rebuild result. A new rebuild request overwrites the
			// existing terminal record directly.
			log.Info("rebuild for %s/%s overwriting previous terminal record (status=%s)",
				req.Database, req.Space, existing.Status)
			existingExisting = true
		}
	}

	// (6) Partition health check.
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

// GetRebuildProgress returns the current scheduling/progress view of a space.
// All state is derived from the etcd-persisted record; there is no in-memory
// runtime to merge with.
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

// ListAllRebuildProgress scans all rebuild records in etcd and returns a
// summary. Each space's status is updated independently and asynchronously,
// so the result is an eventually-consistent snapshot, not tied to any
// particular batch or time window.
func (s *RebuildService) ListAllRebuildProgress(ctx context.Context) (*entity.RebuildSummaryResponse, error) {
	return s.listRebuildProgressByPrefix(ctx, entity.PrefixRebuild)
}

// ListDBRebuildProgress scans all rebuild records for a given database in
// etcd and returns a summary.
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

// CancelRebuild cancels a pending rebuild for a specific space.
// Cancellation is only possible while the record is still in Pending
// state (no tasks dispatched to PS yet). The record is transitioned
// to "cancelled" (a terminal state) and persisted in etcd.
//
//   - Pending: transition to "cancelled" and persist.
//   - Running: cannot cancel (cancelled=false); rebuild must complete.
//   - Already terminal (completed/failed/cancelled): return an informative
//     response with cancelled=false (completed/failed) or cancelled=true
//     (already cancelled, idempotent).
//   - Not found: return an informative error.
//
// The returned CancelRebuildResponse describes the outcome.
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
		// Already terminal — cannot cancel. User should just observe the result.
		return &entity.CancelRebuildResponse{
			DBName:    dbName,
			SpaceName: spaceName,
			Cancelled: false,
			Reason:    fmt.Sprintf("rebuild already %s, cannot cancel", rec.Status),
			Status:    rec.Status,
		}, nil
	case RebuildStatusStringCancelled:
		// Already cancelled — idempotent success.
		return &entity.CancelRebuildResponse{
			DBName:    dbName,
			SpaceName: spaceName,
			Cancelled: true,
			Reason:    "already cancelled",
			Status:    rec.Status,
		}, nil
	case RebuildStatusStringPending:
		// No tasks have been dispatched. Transition to cancelled via
		// STM to close the TOCTOU race with reconcilePending: the
		// scheduler may have already admitted this record (pending→running)
		// between our load and this write. STM ensures we only overwrite
		// if the status is still "pending".
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
		// STM detected that status is no longer "pending" (most likely
		// "running"). Reload the record and re-evaluate.
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
		// Running records cannot be cancelled. Once the scheduler has
		// transitioned a record to Running, at least one task has been
		// (or is about to be) dispatched to a PS. Since space is the
		// minimum cancellation unit and we cannot interrupt an in-flight
		// PS rebuild, the only option is to let it run to completion.
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

// casCancelPending atomically transitions a record from "pending" to
// "cancelled" using an etcd STM. Returns (true, nil) on success, (false, nil)
// if the status is no longer "pending" (race with reconcilePending), or
// (false, err) on STM failure.
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
		// CurrentIndex is reported 1-based for end users (1 of N). When
		// the cursor is past the end (terminal state), clamp to len.
		CurrentIndex:  clampOneBased(rec.CurrentIndexIdx, len(rec.Indexes)),
		CurrentTarget: rec.CurrentTarget(),
	}
	// Running vs pending breakdown plus weighted progress sum.
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
			// Failed tasks do not contribute to overall progress —
			// they are accounted for separately via FailedTasks.
			continue
		}
		progressSum += t.Progress
		progressCount++
	}
	if resp.TotalTasks > 0 {
		resp.SuccessRatio = float64(resp.CompletedTasks) / float64(resp.TotalTasks)
	}
	// OverallPercent averages progress across every task that is either
	// running or completed. We deliberately use TotalTasks as the divisor
	// (instead of progressCount) so that failed replicas drag the bar
	// down — a 4/5 finished space with one failed replica should not
	// report 100%.
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

// checkPartitionsHealthy validates the metadata-level health of the given
// partitions.
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

		// Check index status: reject rebuild if the index has never been
		// built. The C++ engine rejects RebuildIndex when index_status_ ==
		// UNINDEXED (returns -1), so catching this upfront avoids dispatching
		// a task that will inevitably fail at the PS.
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
//
// The scheduler is fully stateless. On each tick it:
//
//  1. lists every SpaceRebuildRecord under PrefixRebuild,
//  2. computes global PS occupancy from the union of all running records,
//  3. for each record drives one of:
//     - pending  -> running   (allocate Tasks, persist)
//     - running  -> dispatch / poll / finalize (one PS at most one active task)
//     - finalize -> retry (back to pending) or delete (terminal)
//
// All decisions are a pure function of the etcd state plus PS RPC replies in
// this tick. There is no in-memory queue; restarting the master simply
// rebuilds these decisions from etcd on the next tick.
// ---------------------------------------------------------------------------

// RebuildScheduler is the central scheduler.
//
// Concurrency rules:
//  1. Scheduling unit is a space.
//  2. A single PS node is occupied by at most one space at a time.
//  3. Inside a space, replicas hosted on the same PS are dispatched serially;
//     replicas on different PSs are dispatched in parallel.
//  4. Within a partition, replicas are rebuilt one at a time (sequential
//     rebuild). This ensures that at least one replica remains available
//     for queries during the rebuild, preserving read availability.
//     Different partitions can be rebuilt in parallel as long as they
//     don't share a PS node.
type RebuildScheduler struct {
	client *client.Client

	// tickMu serializes tick execution so two ticks never race against the
	// same set of etcd records.
	tickMu sync.Mutex

	// isLeader, when non-nil, gates every tick. Returning false skips the
	// tick entirely (P0-#1). In multi-master deployments only the etcd raft
	// leader should run reconciliation; on follower nodes the scheduler
	// goroutine still spins but immediately returns. Guarded by leaderMu
	// so SetLeaderChecker can be called at runtime without data race.
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

// shouldRun returns true when the scheduler is allowed to drive this tick.
// A nil checker is treated as "always leader" so unit tests and single-node
// setups don't need to wire one up.
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
			log.Error("tickLoop panic: %v", r)
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
	// P0-#1: only the etcd raft leader should drive reconciliation. On
	// followers the goroutine still wakes every tickInterval, but returns
	// here so we don't race on the same etcd records (which would corrupt
	// counters, double-dispatch RPCs, and "resurrect" finalized records).
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
		// A genuine SpaceRebuildRecord always has DBName and SpaceName
		// set; anything we deserialize successfully but with empty
		// identifiers is almost certainly a foreign payload that
		// happened to land under PrefixRebuild. Skip it.
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

// isReplicaTerminal reports whether a per-replica task has reached a terminal
// state (success or failure).
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
	// Snapshot the IndexTarget the cursor is currently on. Every task
	// minted in this pass rebuilds exactly this (field, indexType); the
	// scheduler advances the cursor only after every replica for the
	// current target has reached a terminal state.
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
				// Running + Dispatched=false means "planned, awaiting RPC
				// dispatch". After dispatchPending() succeeds, Dispatched
				// flips to true. There is no separate Inited state.
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

	// Admit: transition pending -> running and build the task plan in
	// memory. All tasks start with Dispatched=false; dispatchPending()
	// below will flip them to true one by one.
	rec.Status = RebuildStatusStringRunning
	rec.StartedAt = time.Now()
	rec.TotalReplicas = len(tasks)
	rec.CompletedReplicas = 0
	rec.FailedReplicas = 0
	rec.Tasks = tasks

	// Reserve PS in this tick's view so subsequent pending records see them
	// as busy.
	for ps := range psSet {
		psBusy[ps] = rec.SpaceKey()
	}
	log.Info("space %s admitted, ps=%d totalReplicas=%d",
		rec.SpaceKey(), len(psSet), len(tasks))

	// P0: Persist the running state BEFORE dispatching any RPCs.
	//
	// Without this, a master crash after dispatch but before persist leaves
	// etcd in "pending" with no tasks. A new leader re-admits and
	// re-dispatches. If the PS also crashed (losing its in-memory task),
	// the second dispatch starts a fresh rebuild — and with dropBefore=1
	// that means the index gets dropped twice (data loss).
	//
	// With this first persist, the worst case after a crash is:
	//   - etcd says "running" with Dispatched=false tasks
	//   - new leader calls reconcileRunning -> dispatchPending (idempotent)
	//   - PS-side idempotency (existing.Status==Running -> ignore) prevents
	//     double dispatch even if the first dispatch did land.
	//
	// Use STM with status CAS to close the TOCTOU race with CancelRebuild:
	// a concurrent cancel request may have already transitioned this record
	// to "cancelled". If so, skip admission.
	admitted, err := sc.casAdmitPending(ctx, rec)
	if err != nil {
		log.Error("CAS admit pending %s: %v", rec.SpaceKey(), err)
		return
	}
	if !admitted {
		log.Info("space %s not admitted (status changed before CAS, likely cancelled)", rec.SpaceKey())
		return
	}

	// Dispatch initial tasks within the same tick — one per PS, serially per PS.
	sc.dispatchPending(ctx, rec)

	// Second persist: capture the post-dispatch Dispatched=true flags so
	// a new leader doesn't needlessly re-dispatch on the next tick.
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
			// P0-#2: persist the increment immediately. Previously we only
			// flipped dirty when the streak crossed maxPollFailureStreak,
			// which meant the first N-1 increments lived purely in memory
			// — a master restart would reset the counter and the task
			// could never reach the failure threshold. Setting dirty on
			// every failure makes the streak durable.
			dirty = true
			log.Warn("GetRebuildStatus %s pid=%d nodeID=%d (streak=%d/%d): %v",
				rec.SpaceKey(), t.PartitionID, t.NodeID,
				t.PollFailureStreak, maxPollFailureStreak, err)
			// P1-#11: don't loop forever on a permanently unreachable PS.
			// Once the streak crosses the budget we mark the task failed;
			// the space-level retry budget (rec.MaxRetries) then decides
			// whether to re-queue the whole space.
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
			// PS reports the task is gone. PS-side state is now in-memory
			// only (the persistence layer was removed), so Exists=false
			// means either (a) PS restarted and the task slot vanished
			// with the process, or (b) the task's terminalRetentionPeriod
			// window expired before we polled. Either way the replica is
			// authoritatively terminal: mark it failed and let
			// partition-level retry (in finalize) decide whether to
			// re-plan that partition's tasks.
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
				// Progress is monotonic: never let a transient lower
				// reading from a flaky PS roll the displayed bar back.
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

	// (b.5) Clear the Rebuilding marker on partition.ReStatusMap for any
	// task that just reached terminal state, so the router resumes
	// routing queries to that replica. Idempotent — already-cleared
	// replicas short-circuit inside markReplicaRebuilding.
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

// dispatchPending dispatches not-yet-dispatched tasks under two
// constraints:
//
//  1. Per-PS serial: a single PS node has at most one active (dispatched,
//     not terminal) rebuild task at a time.
//  2. Per-partition serial: replicas of the same partition are rebuilt one
//     at a time. This guarantees that at least one replica of each partition
//     remains available for queries during rebuild, preserving read
//     availability.
//
// Different partitions can be rebuilt in parallel as long as they don't
// share a PS node.
//
// It returns true if any task was modified.
func (sc *RebuildScheduler) dispatchPending(ctx context.Context, rec *SpaceRebuildRecord) bool {
	_ = ctx
	// Collect which PSs already have an active (dispatched, not terminal) task.
	active := make(map[entity.NodeID]bool)
	// Collect which partitions already have an active task (per-partition
	// serial constraint: one replica at a time per partition).
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
			// P0-#3: transient RPC failures must not nuke the replica on
			// the very first attempt.
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
			// Keep Dispatched=false; don't mark this PS as active so other
			// tasks on different PSs are not starved waiting for us.
			continue
		}
		t.Dispatched = true
		t.Status = entity.RebuildStatusRunning
		active[t.NodeID] = true
		activePartition[t.PartitionID] = true
		dirty = true
		log.Info("rebuild dispatched: space=%s pid=%d nodeID=%d (attempt=%d)",
			rec.SpaceKey(), t.PartitionID, t.NodeID, t.DispatchAttempts)
		// Mark this replica as Rebuilding in the partition record
		if err := sc.markReplicaRebuilding(ctx, t.PartitionID, t.NodeID, true); err != nil {
			log.Warn("markReplicaRebuilding(rebuilding) failed for pid=%d nodeID=%d: %v",
				t.PartitionID, t.NodeID, err)
		}
	}
	return dirty
}

// recountAndReleasePS updates the record's counters from rec.Tasks, and
// releases PSs that no longer have any non-terminal task (so the next pending
// space can be admitted on them within this same tick).

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
			// Non-terminal — includes both Dispatched and not-yet-Dispatched
			// Running tasks. Stays consistent with tick()'s isReplicaTerminal
			// filter.
			stillBusy[t.NodeID] = struct{}{}
		}
	}
	rec.CompletedReplicas = completed
	rec.FailedReplicas = failed

	// Release PSs that are no longer busy on behalf of THIS record.
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

// markReplicaRebuilding flips partition.ReStatusMap[nodeID] between
// ReplicasRebuilding and ReplicasOK. Routers exclude Rebuilding replicas
// from query routing (see GetNodeIdsByClientType).
//
// rebuilding=true:  set to Rebuilding (idempotent if already Rebuilding).
// rebuilding=false: clear ONLY if current value is Rebuilding, so we
//
//	don't clobber other states (e.g. ReplicasNotReady)
//	that PS may have written during the rebuild window.
//
// Read-modify-write without CAS, matching vearch's existing pattern
// (PS heartbeat path also does plain Put). Race window is small;
// last-writer-wins is acceptable because both writers (PS heartbeat
// and rebuild scheduler) eventually converge on the truthful state.
func (sc *RebuildScheduler) markReplicaRebuilding(ctx context.Context,
	pid entity.PartitionID, nodeID entity.NodeID, rebuilding bool) error {

	mc := sc.client.Master()
	p, err := mc.QueryPartition(ctx, pid)
	if err != nil {
		return fmt.Errorf("query partition %d: %w", pid, err)
	}
	if p == nil {
		return fmt.Errorf("partition %d not found", pid)
	}
	if p.ReStatusMap == nil {
		p.ReStatusMap = make(map[uint64]uint32)
	}

	cur := p.ReStatusMap[uint64(nodeID)]
	if rebuilding {
		if cur == entity.ReplicasRebuilding {
			return nil // already set, no-op
		}
		p.ReStatusMap[uint64(nodeID)] = entity.ReplicasRebuilding
	} else {
		if cur != entity.ReplicasRebuilding {
			return nil // not currently Rebuilding; don't clobber NotReady etc.
		}
		p.ReStatusMap[uint64(nodeID)] = entity.ReplicasOK
	}

	// Bump UpdateTime so router's partitionCache watcher accepts this write
	// (master_cache.go gates updates on UpdateTime monotonically increasing).
	// Without this, router would drop the new ReStatusMap and continue
	// routing Leader-type queries to the rebuilding leader.
	p.UpdateTime = time.Now().UnixNano()

	bytes, err := vjson.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal partition %d: %w", pid, err)
	}
	if err := mc.Put(ctx, entity.PartitionKey(pid), bytes); err != nil {
		return fmt.Errorf("put partition %d: %w", pid, err)
	}
	return nil
}

// unmarkRebuildingForTerminalTasks clears the Rebuilding marker for any
// dispatched task that has reached a terminal state. Safe to call on
// every tick: the helper short-circuits when current state is not
// Rebuilding (so already-cleared replicas just incur 1 etcd read).
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

// unmarkRebuildingForAllTasks clears the Rebuilding marker for every
// task in the record regardless of state. Used by finalize as a safety
// sweep when the record is about to leave running state (terminate or
// be replanned), to make sure no replica stays stuck in Rebuilding.
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
	// Defensive: ensure no replica stays stuck in Rebuilding state after
	// finalize. reconcileRunning's per-tick sweep should have already
	// cleared terminals, but a transient etcd write failure earlier
	// could leave stale markers. Re-sweep here before any partition
	// retry replan (which discards old task entries) or terminal
	// disposition (which keeps tasks in their last state).
	sc.unmarkRebuildingForTerminalTasks(ctx, rec)

	// P0-1 fix: do NOT release psBusy here. partition-retry below may keep
	// the record in Running with fresh non-terminal tasks on the original
	// PSs; releasing now would let the same-tick reconcilePending hand
	// those PSs to another space, violating the one-space-per-PS
	// invariant. The actual release lives in the Phase 2 terminal branches
	// (advance-cursor / completed / failed), where we know the record will
	// genuinely stop occupying these PSs.

	// Phase 1: partition-level retry.
	//
	// Group every Tasks entry by PartitionID and inspect the group:
	//
	//   - all Completed → leave the partition alone (success).
	//   - any Failed AND PartitionRetries[pid] < MaxRetries → re-plan this
	//     partition's tasks from the current space metadata. Other
	//     partitions (already Completed or still retrying with budget) are
	//     untouched. The record stays in Running status; the scheduler
	//     will pick the freshly planned tasks up on the next tick.
	//   - any Failed AND retry budget exhausted → leave the partition's
	//     Failed tasks intact (terminal).
	//
	// Partition-scoped retry replaces the previous whole-space "reset
	// Tasks to nil, go back to pending" model. The whole-space reset
	// was destructive: every Completed replica was retried alongside the
	// failed ones, and with dropBefore=1 that meant rebuilding partitions
	// that had already finished successfully.
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
			// Could not resolve replicas (space gone, all PSs missing,
			// etc.). Treat as terminal failure for this partition; the
			// space-level finalize below will surface the error.
			log.Warn("partition %d in space %s replan failed: %v — leaving failed", pid, spaceKey, err)
			continue
		}
		if len(newTasks) == 0 {
			log.Warn("partition %d in space %s yielded no live replicas — leaving failed", pid, spaceKey)
			continue
		}
		// Replace the partition's tasks; the next dispatchPending tick
		// will issue the RPCs.
		rec.Tasks = replacePartitionTasks(rec.Tasks, pid, newTasks)
		rec.PartitionRetries[pid]++
		requeuedAny = true
		log.Info("partition %d in space %s requeued for retry %d/%d (%d replicas)",
			pid, spaceKey, rec.PartitionRetries[pid], rec.MaxRetries, len(newTasks))
	}

	// Recompute summary counters from the new Tasks state so progress
	// reporting and the all-terminal check below stay consistent.
	sc.recountRecord(rec)

	if requeuedAny {
		// P0-1 fix: this record is staying in Running with freshly
		// re-planned tasks (Status=Running, Dispatched=false) on the
		// same PSs. Defensively re-assert PS occupancy so the
		// same-tick reconcilePending cannot hand any of these PSs to
		// another pending space. recountAndReleasePS, run earlier in
		// the tick, may have released PSs that briefly had no
		// non-terminal task between the old plan failing and the new
		// plan being installed.
		for _, t := range rec.Tasks {
			if !isReplicaTerminal(t.Status) {
				psBusy[t.NodeID] = spaceKey
			}
		}
		// Record stays in Running. RetryCount mirrors the deepest
		// per-partition retry depth so users still see a single number.
		rec.Status = RebuildStatusStringRunning
		rec.RetryCount = maxPartitionRetry(rec.PartitionRetries)
		rec.ErrorMsg = fmt.Sprintf("partition-level retry in progress (depth=%d/%d)",
			rec.RetryCount, rec.MaxRetries)
		if err := sc.persistRecord(ctx, rec); err != nil {
			log.Error("persist partition-retry record %s: %v", spaceKey, err)
		}
		return
	}

	// P0-1 fix: from here on the record is going to a terminal disposition
	// for this IndexTarget — either advance to the next target (and drop
	// back to Pending, which itself re-acquires PSs through reconcilePending
	// next tick) or finalize completed/failed. Release PS slots owned by
	// this record now so the next pending space can be admitted.
	for nodeID, owner := range psBusy {
		if owner == spaceKey {
			delete(psBusy, nodeID)
		}
	}

	// Phase 2: terminal finalize.
	//
	// No partition still has retry budget AND a failed replica, so the
	// CURRENT IndexTarget is done. Two outcomes:
	//
	//   - Any failed replica → finalize the whole record as failed. We
	//     deliberately do NOT continue to the next target after a failure
	//     because the user almost always wants to investigate the failure
	//     first; silently kicking off the next index would mask it.
	//
	//   - All replicas succeeded → if there are more IndexTargets,
	//     advance the cursor, wipe Tasks/PartitionRetries/counters, and
	//     drop the record back to pending so the next tick's
	//     reconcilePending re-runs PS-occupancy checks (one PS still hosts
	//     at most one space at a time) and emits fresh tasks for the new
	//     target. If there are no more targets, finalize as completed and
	//     remove the record.
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
		// Re-set EnqueuedAt so this record competes fairly against any
		// other pending records in FIFO order on the next tick. Without
		// this, a long-finished prior target would let this record jump
		// ahead of newly enqueued requests indefinitely.
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
	// Persist the terminal record in etcd (do NOT delete).
	// A space always has at most one rebuild record; terminal records
	// (completed/failed) are kept so users can query the last rebuild
	// result. A new rebuild request overwrites the existing record.
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

// replanPartitionTasks builds a fresh PartitionRebuildTask slice for one
// partition, using the current cluster metadata. Used exclusively by the
// partition-level retry path in finalize; the initial admission goes
// through reconcilePending which builds tasks across all partitions at once.
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
			// P0-3 fix: retries MUST NOT carry the original DropBefore.
			// The original request might have set DropBefore=1 to wipe
			// the existing index before rebuilding; replaying that on a
			// retry would destroy a partition that already finished
			// successfully on this or another replica. Keeping
			// DropBefore=0 on retries makes the operation idempotent
			// w.r.t. partition data — the engine will rebuild over the
			// existing state instead of dropping it. This is the
			// counterpart to terminalRetentionPeriod=2h (P0-3a): even
			// if a Completed task is evicted on PS before master polls,
			// the resulting retry won't be destructive.
			DropBefore: 0,
			LimitCPU:   rec.LimitCPU,
			Describe:   rec.Describe,
			MaxRetries: rec.MaxRetries,
		})
	}
	return out, nil
}

// replacePartitionTasks returns a new Tasks slice in which every task
// belonging to pid is swapped for newGroup. Order is preserved for tasks
// that don't belong to pid; new tasks are appended at the position of the
// first removed task.
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

// recountRecord rederives TotalReplicas / CompletedReplicas / FailedReplicas
// from rec.Tasks. Called after partition-level retry replaces tasks so the
// progress counters stay in sync without a full reconcile-running pass.
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

// clampOneBased converts a 0-based cursor into a 1-based "step N of total"
// counter for end-user display, clamped to [0, total]. A negative cursor
// returns 0; a cursor past the end returns total. When total is zero we
// always return 0 so the field stays meaningful in degenerate records.
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

// maxPartitionRetry returns the deepest retry count across all partitions.
// Used to surface a single retry depth in the progress API.
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

// casAdmitPending atomically transitions a record from "pending" to "running"
// using an etcd STM. Returns (true, nil) on success, (false, nil) if the
// status is no longer "pending" (race with CancelRebuild), or (false, err)
// on STM failure.
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
		// Only update the status field; the rest of rec (Tasks, counters,
		// StartedAt) was already prepared by reconcilePending.
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

func (sc *RebuildScheduler) persistRecord(ctx context.Context, rec *SpaceRebuildRecord) error {
	value, err := vjson.Marshal(rec)
	if err != nil {
		return err
	}
	key := entity.RebuildSpaceKey(rec.DBName, rec.SpaceName)
	return sc.client.Master().Put(ctx, key, value)
}

// deleteRecord removes the rebuild record from etcd.
// Used only for cleanup when a record should be explicitly removed
// (e.g. administrative purge). Terminal states (completed/failed/cancelled)
// are persisted in etcd so the last rebuild result is queryable.
func (sc *RebuildScheduler) deleteRecord(ctx context.Context, key string) error {
	return sc.client.Master().Delete(ctx, key)
}
