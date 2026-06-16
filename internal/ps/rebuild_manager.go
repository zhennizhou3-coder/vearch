package ps

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
)

type RebuildStatus int

// PS-side rebuild task status. NOTE: these values are exported on the wire
// to the master scheduler — keep the encoding stable and aligned with the
// master-side PSRebuildStatus* constants.
const (
	RebuildStatusRunning   RebuildStatus = 1
	RebuildStatusCompleted RebuildStatus = 2
	RebuildStatusFailed    RebuildStatus = 3
)

// gamma engine IndexStatus values, as defined in
// internal/engine/search/engine.h: enum IndexStatus { UNINDEXED, INDEXING, INDEXED }.
// We re-declare them here to avoid sprinkling magic numbers through the
// rebuild monitoring loop.
const (
	engineIndexStatusUnindexed = 0
	engineIndexStatusIndexing  = 1
	engineIndexStatusIndexed   = 2
)

// Rebuild monitor knobs. They are intentionally generous — a real-world
// rebuild on a multi-million-doc partition can run for hours.
const (
	rebuildPollInterval = 2 * time.Second
	// rebuildMaxDuration caps how long the PS-side monitor will wait for
	// engine rebuild to finish. After this the task is failed; the master
	// scheduler will then drive its own retry-or-finalize logic.
	rebuildMaxDuration = 24 * time.Hour
	// indexInfoFailureBudget is the number of consecutive engine.IndexInfo
	// failures we tolerate before marking the task failed.
	indexInfoFailureBudget = 5
	// terminalRetentionPeriod is how long a Completed/Failed task is kept
	// in memory after it terminates.
	terminalRetentionPeriod = 2 * time.Hour
)

// RebuildTask captures the in-memory state of a PS-side rebuild task.
// State is in-memory only; a PS restart drops all running task records and
// the master scheduler treats Exists=false as a terminal failure on its
// next status poll.
type RebuildTask struct {
	PartitionID  uint32        `json:"partition_id"`
	SpaceKey     string        `json:"space_key"`
	FieldName    string        `json:"field_name,omitempty"`
	IndexType    string        `json:"index_type,omitempty"`
	Status       RebuildStatus `json:"status"`
	ErrorMessage string        `json:"error_message,omitempty"`
	Progress     int           `json:"progress"`
	StartTime    int64         `json:"start_time"`
	CompleteTime int64         `json:"complete_time,omitempty"`

	// Rebuild parameters retained so a recovered task can rebuild its
	// engine-side context (and could, if ever needed, be re-issued).
	DropBefore int `json:"drop_before,omitempty"`
	LimitCPU   int `json:"limit_cpu,omitempty"`
	Describe   int `json:"describe,omitempty"`
}

// RebuildManager registers rebuild tasks and exposes their status. The task
// identity key is (spaceKey, partitionID, fieldName, indexType): different
// (field, indexType) targets on the same partition can therefore be tracked
// independently. Cross-task scheduling (e.g. one space at a time per PS) is
// the master scheduler's responsibility and is not enforced here.
type RebuildManager interface {
	StartRebuildTask(spaceKey, fieldName, indexType string, partitionID uint32,
		dropBefore int, limitCPU int, describe int) error
	GetRebuildTaskStatus(spaceKey, fieldName, indexType string, partitionID uint32) (
		status int, errorMsg string, exists bool, progress int)
}

type PSRebuildManager struct {
	mu     sync.RWMutex
	tasks  map[string]*RebuildTask
	server *Server
}

func NewPSRebuildManager(server *Server) *PSRebuildManager {
	return &PSRebuildManager{
		tasks:  make(map[string]*RebuildTask),
		server: server,
	}
}

// getTaskKey returns the per-task identity key used in the in-memory map
// and on the wire status query. It is (spaceKey, partitionID, fieldName,
// indexType). FieldName/IndexType are empty for legacy callers that
// haven't been updated to the new RPC shape; we keep them in the key as-is
// so an old "anonymous" task and a new (field, indexType)-qualified task
// don't accidentally collide.
func (r *PSRebuildManager) getTaskKey(spaceKey, fieldName, indexType string, partitionID uint32) string {
	return fmt.Sprintf("%s|%d|%s|%s", spaceKey, partitionID, fieldName, indexType)
}

// StartRebuildTask registers a rebuild task and kicks off a background
// monitor goroutine. The task starts in Running state because, as soon as
// engine.Rebuild() returns, gamma has spawned its own internal goroutine to
// do the work. The monitor goroutine then polls engine.IndexInfo() to detect
// real completion.
func (r *PSRebuildManager) StartRebuildTask(spaceKey, fieldName, indexType string,
	partitionID uint32, dropBefore int, limitCPU int, describe int) error {
	taskKey := r.getTaskKey(spaceKey, fieldName, indexType, partitionID)

	r.mu.Lock()
	if existing, ok := r.tasks[taskKey]; ok && existing.Status == RebuildStatusRunning {
		r.mu.Unlock()
		// Re-issuing while still running is a no-op rather than an error,
		// so that the master's at-least-once dispatch is idempotent.
		log.Info("rebuild task already running for %s pid=%d field=%s indexType=%s, ignoring duplicate start",
			spaceKey, partitionID, fieldName, indexType)
		return nil
	}
	now := time.Now().Unix()
	task := &RebuildTask{
		PartitionID: partitionID,
		SpaceKey:    spaceKey,
		FieldName:   fieldName,
		IndexType:   indexType,
		Status:      RebuildStatusRunning,
		Progress:    0,
		StartTime:   now,
		DropBefore:  dropBefore,
		LimitCPU:    limitCPU,
		Describe:    describe,
	}
	r.tasks[taskKey] = task
	r.mu.Unlock()

	go r.executeRebuild(task, dropBefore, limitCPU, describe)

	return nil
}

// executeRebuild triggers the engine rebuild and then polls engine.IndexInfo()
// until the index reaches the INDEXED state (or a terminal error / timeout).
func (r *PSRebuildManager) executeRebuild(task *RebuildTask, dropBefore int, limitCPU int, describe int) {
	defer func() {
		if p := recover(); p != nil {
			r.markFailed(task, fmt.Sprintf("panic: %v", p))
		}
	}()

	partitionID := entity.PartitionID(task.PartitionID)
	store := r.server.GetPartition(partitionID)
	if store == nil {
		r.markFailed(task, fmt.Sprintf("partition %d not found", task.PartitionID))
		return
	}
	engine := store.GetEngine()
	if engine == nil {
		r.markFailed(task, fmt.Sprintf("engine not initialized for partition %d", task.PartitionID))
		return
	}

	// Snapshot the index status BEFORE issuing the rebuild call so we can
	// distinguish "rebuild has progressed" vs "engine just happened to be
	// INDEXED already". For dropBefore=1 the engine will transition INDEXED
	// -> UNINDEXED/INDEXING -> INDEXED, so observing a non-INDEXED state at
	// least once guarantees the rebuild has actually started.
	preStatus, preIndexed, preMaxDocid, preErr := engine.IndexInfoWithErr()
	if preErr != nil {
		// Engine refused to report status BEFORE we even tried to rebuild —
		// fail fast instead of pretending we have a meaningful baseline.
		r.markFailed(task, fmt.Sprintf("pre-rebuild engine.IndexInfo: %v", preErr))
		return
	}

	// If no index exists at all (UNINDEXED), there is nothing to rebuild.
	// A rebuild operates on an existing index — creating an index from
	// scratch is the engine's normal indexing flow, not a rebuild.
	if preStatus == engineIndexStatusUnindexed {
		r.markFailed(task, "cannot rebuild: index does not exist (status=UNINDEXED)")
		return
	}

	// Run the actual rebuild synchronously inside this goroutine so we can
	// observe its return value. Buffered so the goroutine never blocks even
	// if monitorRebuild has already returned via timeout.
	//
	// P1-#7: the CGO call inside engine.RebuildIndex cannot be cancelled
	// from Go. If the PS starts shutting down while this is running,
	// monitorRebuild will notice (via the server context or engine.HasClosed)
	// and exit, but THIS goroutine keeps holding onto engine state. The
	// buffered doneCh + recover ensure we don't leak goroutines on panic;
	// monitorRebuild additionally gives the CGO call a short grace period
	// to finish before declaring the task failed (see waitForRebuildExit).
	doneCh := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				doneCh <- fmt.Errorf("RebuildIndex panic: %v", p)
			}
		}()
		// Per-(field, indexType) target dispatch. RebuildFieldIndex is
		// the single entry point exposed by the engine layer; the
		// gammacb implementation today still falls back to the
		// whole-partition rebuild because the per-field gamma C++ API
		// hasn't landed yet. Once it does, this call becomes a true
		// per-(field, indexType) operation without any change here.
		// Empty FieldName/IndexType means a legacy whole-partition
		// rebuild — the engine handles that fallback internally.
		doneCh <- engine.RebuildFieldIndex(task.FieldName, task.IndexType,
			dropBefore, limitCPU, describe)
	}()
	log.Info("rebuild engine.RebuildFieldIndex dispatched: pid=%d field=%s indexType=%s preStatus=%d preIndexed=%d preMaxDocid=%d dropBefore=%d limitCPU=%d describe=%d",
		task.PartitionID, task.FieldName, task.IndexType,
		preStatus, preIndexed, preMaxDocid, dropBefore, limitCPU, describe)

	// Monitor loop. doneCh is live and the in-flight CGO goroutine will
	// eventually publish on it.
	r.monitorRebuild(task, store, doneCh, preStatus, preIndexed, preMaxDocid)
}

// monitorRebuild is the polling loop that watches engine.IndexInfo() until
// the rebuild settles into a terminal state.
func (r *PSRebuildManager) monitorRebuild(task *RebuildTask, store PartitionStore,
	doneCh <-chan error, preStatus, preIndexed, preMaxDocid int) {
	// serverCtx is the PS-wide shutdown signal. Once it fires we exit the
	// polling loop promptly instead of waiting up to rebuildPollInterval.
	var serverCtx context.Context
	if r.server != nil {
		serverCtx = r.server.ctx
	}
	deadline := time.Now().Add(rebuildMaxDuration)
	ticker := time.NewTicker(rebuildPollInterval)
	defer ticker.Stop()
	// shutdownGrace bounds how long we wait for the in-flight CGO call to
	// drain after the engine closes or the server shuts down. Picked to be
	// short enough to keep PS shutdown responsive while still giving a
	// fast-finishing rebuild a chance to surface its real terminal state.
	const shutdownGrace = 5 * time.Second
	// drain waits for the CGO goroutine to publish its result (or the
	// grace period to elapse) and returns the appropriate failure message.
	drain := func(reason string) string {
		timer := time.NewTimer(shutdownGrace)
		defer timer.Stop()
		select {
		case err := <-doneCh:
			if err != nil {
				return fmt.Sprintf("%s; engine.RebuildIndex: %v", reason, err)
			}
			return reason
		case <-timer.C:
			return fmt.Sprintf("%s; CGO rebuild did not exit within %s",
				reason, shutdownGrace)
		}
	}

	// observedNonIndexed becomes true once we've seen a non-INDEXED state
	// after the rebuild was issued, proving the engine actually entered the
	// rebuild work. Without this gate, a rebuild on an already-INDEXED
	// partition could be (mis)completed on the very first poll before the
	// engine has even started doing the work, because gammaEngine.Rebuild
	// dispatches the real CGO call asynchronously.
	observedNonIndexed := preStatus != engineIndexStatusIndexed

	// rebuildReturned records whether the synchronous engine.RebuildIndex
	// goroutine has already finished. Once it has, an INDEXED reading is
	// authoritative — no need to wait for the stability heuristic.
	rebuildReturned := false

	failureStreak := 0

	for {
		select {
		case err := <-doneCh:
			// CGO returned. Either propagate the failure immediately or
			// record success and let the next IndexInfo poll confirm the
			// final indexed/maxDocid for accurate progress reporting.
			if err != nil {
				r.markFailed(task, fmt.Sprintf("engine.RebuildIndex: %v", err))
				return
			}
			rebuildReturned = true
			log.Info("engine.RebuildIndex returned success for pid=%d, awaiting IndexInfo confirmation",
				task.PartitionID)
		case <-ticker.C:
		case <-ctxDone(serverCtx):
			// P1-#7: PS is shutting down. Give CGO a grace window then bail.
			r.markFailed(task, drain("PS server shutting down"))
			return
		}

		if time.Now().After(deadline) {
			r.markFailed(task, fmt.Sprintf("rebuild monitor timeout after %s", rebuildMaxDuration))
			return
		}

		engine := store.GetEngine()
		if engine == nil || engine.HasClosed() {
			// P1-#7: engine is going away. Wait for CGO to finish (with
			// grace) before returning so the partition close path doesn't
			// race against an in-flight RebuildIndex call.
			r.markFailed(task, drain("engine closed during rebuild"))
			return
		}

		status, indexedNum, maxDocid, infoErr := engine.IndexInfoWithErr()

		if infoErr != nil {
			failureStreak++
			log.Warn("engine.IndexInfo failed for pid=%d (streak=%d/%d): %v",
				task.PartitionID, failureStreak, indexInfoFailureBudget, infoErr)
			if failureStreak >= indexInfoFailureBudget {
				timer := time.NewTimer(shutdownGrace)
				select {
				case err := <-doneCh:
					timer.Stop()
					if err == nil {
						log.Info("engine.RebuildIndex returned success for pid=%d despite IndexInfo failures, marking completed",
							task.PartitionID)
						r.markCompleted(task, preIndexed, preMaxDocid)
						return
					}
					r.markFailed(task,
						fmt.Sprintf("engine.IndexInfo failed %d consecutive times: %v; engine.RebuildIndex: %v",
							failureStreak, infoErr, err))
					return
				case <-timer.C:
				}
				r.markFailed(task,
					fmt.Sprintf("engine.IndexInfo failed %d consecutive times: %v; CGO rebuild did not exit within %s",
						failureStreak, infoErr, shutdownGrace))
				return
			}
			continue
		}
		failureStreak = 0

		// Track progress monotonically.
		progress := computeProgress(indexedNum, maxDocid)
		r.updateProgress(task, progress)

		switch status {
		case engineIndexStatusUnindexed, engineIndexStatusIndexing:
			observedNonIndexed = true
			log.Debug("rebuild in progress pid=%d status=%d indexed=%d/%d (%d%%)",
				task.PartitionID, status, indexedNum, maxDocid, progress)
		case engineIndexStatusIndexed:
			// Fast path: the synchronous engine.RebuildIndex has already
			// returned success, so INDEXED is authoritative.
			if rebuildReturned {
				r.markCompleted(task, indexedNum, maxDocid)
				log.Info("rebuild task completed for partition %d (rebuild returned, indexed=%d, maxDocid=%d)",
					task.PartitionID, indexedNum, maxDocid)
				return
			}
			if observedNonIndexed {
				// We saw the engine actually do work before settling on
				// INDEXED. The CGO goroutine should be just about to
				// publish on doneCh — wait for it briefly. Marking the
				// task completed while CGO is still touching engine
				// state has caused partition-close crashes (P1-#8).
				switch r.waitDoneShort(doneCh, &rebuildReturned, task) {
				case settleSuccess:
					r.markCompleted(task, indexedNum, maxDocid)
					log.Info("rebuild task completed for partition %d (indexed=%d, maxDocid=%d, 100%%)",
						task.PartitionID, indexedNum, maxDocid)
					return
				case settleFailed:
					// markFailed already happened inside waitDoneShort.
					return
				case settlePending:
					// CGO did not return within the short window;
					// keep polling. Next iteration will retry.
					continue
				}
			}
			// Never observed a non-INDEXED state and rebuild has not
			// returned yet. Keep polling — the engine should transition
			// to INDEXING/UNINDEXED soon. If the rebuild call returns
			// with an error (e.g. UNINDEXED rejected), we will catch
			// it via doneCh above.
		default:
			log.Warn("unknown engine IndexStatus %d for pid=%d, continuing", status, task.PartitionID)
		}
	}
}

// waitDoneShort blocks for at most cgoSettleWindow waiting for the in-flight
// CGO rebuild to publish its terminal status on doneCh. The tri-state return
// captures all three outcomes:
//
//   - settleSuccess: CGO returned nil (or rebuildReturned was already set).
//     Caller may proceed to markCompleted.
//   - settleFailed:  CGO returned an error. Caller MUST NOT proceed; the
//     task has already been marked failed by this helper. Caller should
//     return from monitorRebuild immediately.
//   - settlePending: CGO is still running. Caller should continue the
//     outer poll loop (do NOT markCompleted yet).
//
// This is the safety belt for P1-#8: every "looks indexed" completion path
// must first see CGO actually finish before flipping the task to Completed.
// Without this gate a partition close on the heels of a "stable INDEXED"
// decision can race the still-running RebuildIndex. Equally important, the
// helper itself consumes from doneCh, so we cannot let an error simply fall
// through — once consumed, the outer select can never see it again. We
// resolve that by terminating the task here when an error is observed.
const cgoSettleWindow = 1 * time.Second

type settleResult int

const (
	settleSuccess settleResult = iota
	settleFailed
	settlePending
)

func (r *PSRebuildManager) waitDoneShort(doneCh <-chan error, rebuildReturned *bool, task *RebuildTask) settleResult {
	if *rebuildReturned {
		return settleSuccess
	}
	timer := time.NewTimer(cgoSettleWindow)
	defer timer.Stop()
	select {
	case err, ok := <-doneCh:
		if !ok || err == nil {
			*rebuildReturned = true
			return settleSuccess
		}
		// CGO surfaced an error here. We MUST not let it disappear:
		// channel was consumed, the outer select will never see it
		// again. Mark the task failed inline so the caller can stop
		// the loop without a phantom "looked completed" state.
		log.Error("CGO rebuild failed during IndexInfo confirmation pid=%d: %v",
			task.PartitionID, err)
		r.markFailed(task, fmt.Sprintf("engine.RebuildIndex: %v", err))
		return settleFailed
	case <-timer.C:
		return settlePending
	}
}

// computeProgress returns 0..100. Returns 0 when totals are unknown.
func computeProgress(indexedNum, maxDocid int) int {
	if maxDocid <= 0 {
		return 0
	}
	p := indexedNum * 100 / maxDocid
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

// ctxDone returns a receive-only channel that fires when ctx is cancelled,
// or a never-firing channel when ctx is nil. Used to make a nil server
// context (e.g. in tests where r.server isn't wired up) behave the same as
// "no shutdown signal yet" inside a select.
func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func (r *PSRebuildManager) updateProgress(task *RebuildTask, progress int) {
	r.mu.Lock()
	if progress > task.Progress {
		task.Progress = progress
	}
	r.mu.Unlock()
}

func (r *PSRebuildManager) markCompleted(task *RebuildTask, indexedNum, maxDocid int) {
	r.mu.Lock()
	task.Status = RebuildStatusCompleted
	task.Progress = 100
	task.CompleteTime = time.Now().Unix()
	task.ErrorMessage = ""
	r.mu.Unlock()
	_ = indexedNum
	_ = maxDocid
	// Keep terminal state in memory for terminalRetentionPeriod so the
	// master scheduler has at least one polling window to observe success
	// before the slot is evicted. There is no persistence anymore — a PS
	// crash before the master polls means the next status query will
	// return Exists=false and the master will treat the replica as failed,
	// which is what we want (see the design note at the top of this file).
	r.scheduleTerminalEviction(task)
}

func (r *PSRebuildManager) markFailed(task *RebuildTask, msg string) {
	r.mu.Lock()
	task.Status = RebuildStatusFailed
	task.ErrorMessage = msg
	task.CompleteTime = time.Now().Unix()
	r.mu.Unlock()
	log.Error("rebuild task failed for partition %d: %s", task.PartitionID, msg)
	// Same retention rule as markCompleted: the master needs at least one
	// successful poll to observe the failure before we drop the record.
	r.scheduleTerminalEviction(task)
}

// scheduleTerminalEviction removes a terminal task from the in-memory map
// after terminalRetentionPeriod. The timer is fire-and-forget; a PS crash
// before it fires is harmless because manager state is in-memory only —
// there is nothing to clean up across processes.
func (r *PSRebuildManager) scheduleTerminalEviction(task *RebuildTask) {
	taskKey := r.getTaskKey(task.SpaceKey, task.FieldName, task.IndexType, task.PartitionID)
	time.AfterFunc(terminalRetentionPeriod, func() {
		r.mu.Lock()
		if existing, ok := r.tasks[taskKey]; ok && existing == task {
			delete(r.tasks, taskKey)
		}
		r.mu.Unlock()
	})
}

func (r *PSRebuildManager) GetRebuildTaskStatus(spaceKey, fieldName, indexType string,
	partitionID uint32) (status int, errorMsg string, exists bool, progress int) {
	taskKey := r.getTaskKey(spaceKey, fieldName, indexType, partitionID)

	r.mu.RLock()
	task, found := r.tasks[taskKey]
	r.mu.RUnlock()

	if !found || task == nil {
		return 0, "", false, 0
	}

	return int(task.Status), task.ErrorMessage, true, task.Progress
}

// SetRebuildManager allows tests or alternative wirings to inject a custom
// RebuildManager. Production code should rely on GetRebuildManager's lazy
// initialization (sync.Once) instead of calling this.
func (s *Server) SetRebuildManager(manager RebuildManager) {
	s.rebuildManager = manager
}

// GetRebuildManager returns the PS-side rebuild manager, lazily creating the
// default PSRebuildManager on first access. The sync.Once gate guarantees that
// concurrent handlers (RebuildIndexHandler + RebuildStatusHandler) cannot each
// install a different manager and lose tasks via a last-writer-wins overwrite.
func (s *Server) GetRebuildManager() RebuildManager {
	s.rebuildOnce.Do(func() {
		if s.rebuildManager == nil {
			s.rebuildManager = NewPSRebuildManager(s)
		}
	})
	return s.rebuildManager
}
