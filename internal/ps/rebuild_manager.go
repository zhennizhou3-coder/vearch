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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ps

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/ps/engine"
)

// Gamma engine IndexStatus values, stringified into EngineStatus.IndexStatuses
// by IndexStatusToString in internal/engine/index/index_state.h. The per-index
// path (IndexStatusOf) emits these tokens directly; the numeric engine-wide
// status from IndexInfo is mapped onto the same tokens by indexStatusToString.
const (
	indexStatusUnindexed = "UNINDEXED"
	indexStatusIndexing  = "INDEXING"
	indexStatusIndexed   = "INDEXED"
	// Per-index-only terminal state reported through EngineStatus.IndexStatuses; the
	// engine-wide IndexStatus never reports this value.
	indexStatusFailed = "FAILED"
)

// indexStatusToString maps the numeric engine-wide index status returned by
// Engine.IndexInfo (0/1/2, never FAILED) onto the same string tokens the
// per-index path uses, so the monitor can compare both sources uniformly.
func indexStatusToString(status int) string {
	switch status {
	case 1:
		return indexStatusIndexing
	case 2:
		return indexStatusIndexed
	default:
		return indexStatusUnindexed
	}
}

// Rebuild monitor knobs.
const (
	rebuildPollInterval = 500 * time.Millisecond
	// rebuildMaxDuration caps how long PS waits for one rebuild.
	rebuildMaxDuration = 24 * time.Hour
	// indexInfoFailureBudget caps consecutive IndexInfo failures.
	indexInfoFailureBudget = 5
	// terminalRetentionPeriod keeps terminal tasks visible to master polls.
	terminalRetentionPeriod = 2 * time.Hour
)

// RebuildTask aliases the shared entity type; master and PS use the same
// struct with each side populating its own fields (see entity.RebuildTask).
type RebuildTask = entity.RebuildTask

// RebuildTaskManager registers PS tasks and exposes their status.
type RebuildTaskManager interface {
	StartRebuildTask(dbName, spaceName, indexName, fieldName, indexType string, partitionID uint32,
		dropBefore int, limitCPU int, describe int, roundID string, isTrainer bool, trainerAddr string) error
	GetRebuildTaskStatus(dbName, spaceName, indexName string, partitionID uint32) (
		status entity.RebuildStatus, errorMsg string, exists bool, progress int)
}

type RebuildManager struct {
	mu     sync.RWMutex
	tasks  map[string]*RebuildTask
	server *Server
}

func NewRebuildManager(server *Server) *RebuildManager {
	return &RebuildManager{
		tasks:  make(map[string]*RebuildTask),
		server: server,
	}
}

// getTaskKey returns the in-memory task identity key.
// IndexName is globally unique within a space, so (dbName, spaceName,
// partitionID, indexName) is sufficient — FieldName / IndexType are not part
// of the key.
func (r *RebuildManager) getTaskKey(dbName, spaceName, indexName string, partitionID uint32) string {
	return fmt.Sprintf("%s-%s|%d|%s", dbName, spaceName, partitionID, indexName)
}

// StartRebuildTask registers a task and starts its monitor goroutine.
//
// When roundID != "", this rebuild is part
// of a coordinated round. The trainer (isTrainer) trains + dumps its model; a
// follower (trainerAddr != "") pulls that model and injects it instead of
// training. Empty roundID keeps the plain train-in-place behavior.
func (r *RebuildManager) StartRebuildTask(dbName, spaceName, indexName, fieldName, indexType string,
	partitionID uint32, dropBefore int, limitCPU int, describe int, roundID string, isTrainer bool, trainerAddr string) error {
	taskKey := r.getTaskKey(dbName, spaceName, indexName, partitionID)

	r.mu.Lock()
	if existing, ok := r.tasks[taskKey]; ok && existing.Status == entity.RebuildStatusRunning {
		r.mu.Unlock()
		// Duplicate dispatch is idempotent.
		log.Info("rebuild task already running for %s-%s pid=%d indexName=%s, ignoring duplicate start",
			dbName, spaceName, partitionID, indexName)
		return nil
	}
	task := &RebuildTask{
		PartitionID: entity.PartitionID(partitionID),
		DBName:      dbName,
		SpaceName:   spaceName,
		IndexName:   indexName,
		FieldName:   fieldName,
		IndexType:   indexType,
		Status:      entity.RebuildStatusRunning,
		Progress:    0,
		StartTime:   time.Now(),
		DropBefore:  dropBefore,
		LimitCPU:    limitCPU,
		Describe:    describe,
		IsTrainer:   isTrainer,
	}
	r.tasks[taskKey] = task
	r.mu.Unlock()

	go r.executeRebuild(task, dropBefore, limitCPU, describe, roundID, trainerAddr)

	return nil
}

// executeRebuild triggers the engine rebuild and monitors it to terminal state.
func (r *RebuildManager) executeRebuild(task *RebuildTask, dropBefore int, limitCPU int, describe int, roundID string, trainerAddr string) {
	defer func() {
		if p := recover(); p != nil {
			r.markFailed(task, fmt.Sprintf("panic: %v", p))
		}
	}()

	partitionID := task.PartitionID
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

	// Pre-flight: decide whether to dispatch a fresh build or only monitor.
	// shouldDispatchBuild==false means a build is running / about to run, or the
	// index is already at a state the monitor can complete without rebuilding —
	// dispatching would interrupt or race it. Whole-partition rebuild
	// (FieldName == "") reads the engine-wide status; field-level reads the named
	// index's status (and its per-index SupportIncrement flag).
	var preStatus string
	var preSupportIncrement = true // whole-partition path has no per-index flag; assume incremental
	var err error
	if task.FieldName == "" {
		st, _, _ := engine.IndexInfo()
		preStatus = indexStatusToString(st)
	} else {
		info, serr := engine.IndexStatusOf(task.IndexName)
		preStatus, preSupportIncrement, err = info.Status, info.SupportIncrement, serr
	}
	if err != nil {
		r.markFailed(task, fmt.Sprintf("pre-rebuild status read for %q: %v", task.IndexName, err))
		return
	}
	if !shouldDispatchBuild(preStatus, preSupportIncrement) {
		task.AwaitTransition = false
		log.Info("rebuild monitoring existing build (no dispatch): pid=%d indexName=%q status=%q",
			task.PartitionID, task.IndexName, preStatus)
		r.monitorRebuild(task, store, nil)
		return
	}
	task.AwaitTransition = true

	// Replica consistency roles for this round (empty roundID = plain train, both
	// paths stay empty):
	//   - follower: pull this round's model from a source replica (trainer or an
	//     already-installed sibling) into its fixed slot and load it instead of
	//     training. The pulled files are kept, so this replica becomes a valid
	//     pull source for later followers (multi-source).
	//   - trainer: hand the engine a <model>.tmp to dump its freshly trained
	//     artifacts into during RebuildIndex; committed to the slot afterward.
	trainingArtifactsPath := ""
	dumpArtifactsPath := ""
	if roundID != "" && task.IsTrainer {
		tmp, terr := r.trainerDumpTmpPath(store, task.IndexName)
		if terr != nil {
			r.markFailed(task, fmt.Sprintf("prepare training-artifacts dump path (round=%s): %v", roundID, terr))
			return
		}
		dumpArtifactsPath = tmp
	} else if roundID != "" && !task.IsTrainer && trainerAddr != "" {
		modelPath, perr := r.pullAndInstallTrainingArtifacts(trainerAddr, store, task.IndexName, roundID)
		if perr != nil {
			r.markFailed(task, fmt.Sprintf("pull training artifacts from %s (round=%s): %v", trainerAddr, roundID, perr))
			return
		}
		trainingArtifactsPath = modelPath
		log.Info("rebuild follower installed training artifacts: pid=%d index=%s round=%s -> %s",
			task.PartitionID, task.IndexName, roundID, modelPath)
	}

	// engine.RebuildIndex is synchronous: it returns only after the C++
	// engine has built, trained, and atomically swapped the new index in. Run
	// it on a goroutine so the monitor can also honor server shutdown; its
	// result on doneCh is the authoritative completion / failure signal.
	doneCh := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				doneCh <- fmt.Errorf("RebuildIndex panic: %v", p)
			}
		}()
		// A non-empty trainingArtifactsPath makes a follower load that model
		// instead of training; a non-empty dumpArtifactsPath makes the trainer
		// dump its trained artifacts there; both empty means train in place.
		err := engine.RebuildIndex(task.IndexName, task.FieldName, task.IndexType,
			dropBefore, limitCPU, describe, trainingArtifactsPath, dumpArtifactsPath)
		// Trainer: the engine wrote the raw artifacts to dumpArtifactsPath during
		// the rebuild; commit them (sha256/rename/.meta) before reporting success
		// so followers can pull this round's model. A commit failure fails the
		// round, since followers would otherwise have nothing to pull.
		if err == nil && dumpArtifactsPath != "" {
			if _, ferr := r.finalizeTrainerArtifacts(store, task.IndexName, roundID, dumpArtifactsPath); ferr != nil {
				err = fmt.Errorf("commit training artifacts (round=%s): %v", roundID, ferr)
			}
		}
		doneCh <- err
	}()
	log.Info("rebuild engine.RebuildIndex dispatched: pid=%d indexName=%s field=%s indexType=%s dropBefore=%d limitCPU=%d describe=%d isTrainer=%v hasModel=%v",
		task.PartitionID, task.IndexName, task.FieldName, task.IndexType,
		dropBefore, limitCPU, describe, task.IsTrainer, trainingArtifactsPath != "")

	// Monitor until the rebuild reaches a terminal state.
	r.monitorRebuild(task, store, doneCh)
}

// monitorRebuild waits for the rebuilt index to be swapped in, then for an
// incremental index to backfill to its frozen document frontier. Non-incremental
// and untrained indexes are complete at the swap because they cannot backfill.
func (r *RebuildManager) monitorRebuild(task *RebuildTask, store PartitionStore,
	doneCh <-chan error) {
	var serverCtx context.Context
	if r.server != nil {
		serverCtx = r.server.ctx
	}
	deadline := time.Now().Add(rebuildMaxDuration)
	ticker := time.NewTicker(rebuildPollInterval)
	defer ticker.Stop()

	failureStreak := 0
	// engineReturned records the dispatch path's authoritative signal: the
	// synchronous engine.RebuildIndex has returned nil (train+swap done). The
	// monitor-only path leaves it false and confirms via polled status instead.
	engineReturned := false
	// inBackfillPhase is the single phase marker: false while phase 1 (waiting
	// for THIS rebuild to reach INDEXED), true once it has. targetDocCount is the
	// frozen backfill frontier, meaningful only after the transition.
	inBackfillPhase := false
	targetDocCount := 0

	for {
		select {
		case err := <-doneCh:
			if err != nil {
				r.markFailed(task, fmt.Sprintf("engine.RebuildIndex: %v", err))
				return
			}
			engineReturned = true
			doneCh = nil // stop the drained receive from firing
		case <-ticker.C:
		case <-ctxDone(serverCtx):
			r.markFailed(task, "PS server shutting down")
			return
		}

		if time.Now().After(deadline) {
			r.markFailed(task, fmt.Sprintf("rebuild monitor timeout after %s", rebuildMaxDuration))
			return
		}

		engine := store.GetEngine()
		if engine == nil || engine.HasClosed() {
			r.markFailed(task, "engine closed during rebuild")
			return
		}

		info, infoErr := r.pollStatus(task, engine)
		if infoErr != nil {
			failureStreak++
			log.Warn("rebuild status poll failed for pid=%d (streak=%d/%d): %v",
				task.PartitionID, failureStreak, indexInfoFailureBudget, infoErr)
			if failureStreak >= indexInfoFailureBudget {
				r.markFailed(task,
					fmt.Sprintf("status poll failed %d consecutive times: %v",
						failureStreak, infoErr))
				return
			}
			continue
		}
		failureStreak = 0
		r.updateProgress(task, computeProgress(info.IndexedNum, info.MaxDocid))

		if info.Status == indexStatusFailed {
			r.markFailed(task, "engine reported index status=FAILED")
			return
		}

		// Phase 1 → 2 transition: wait until this rebuild reaches INDEXED, then
		// classify + freeze the frontier exactly once.
		if !inBackfillPhase {
			if !r.reachedIndexed(task, info, engineReturned) {
				continue // still building / awaiting our swap
			}
			inBackfillPhase = true

			if !info.SupportIncrement {
				r.markCompleted(task)
				log.Info("rebuild task completed for partition %d (non-incremental index, swap is terminal)",
					task.PartitionID)
				return
			}
			if !info.IsTrained {
				// Terminal, but a degradation worth surfacing: the new index is
				// untrained because the live doc count is below training_threshold.
				// Queries fall back to a full brute-force scan (correct results,
				// slower) instead of the ANN index. Typical cause: an index that
				// was trained earlier, then had enough docs deleted that the live
				// count dropped below the threshold, so a non-drop rebuild swaps in
				// an untrained index. Not a failure (data is fully queryable, and a
				// retry would deterministically reproduce this since the doc count
				// is unchanged), so complete rather than hang or trigger self-heal
				// — but WARN so operators see the degradation.
				r.markCompleted(task)
				log.Warn("rebuild task completed for partition %d with an UNTRAINED index "+
					"(live doc count below training_threshold); queries use full brute-force "+
					"scan until enough docs exist to train", task.PartitionID)
				return
			}
			// Freeze the frontier this rebuild owns; writes beyond it are the
			// realtime indexer's job, so completion is measured against a fixed target.
			targetDocCount = info.MaxDocid + 1
		}

		// Phase 2: complete once the backfill catches up to the frozen frontier.
		if info.IndexedNum >= targetDocCount {
			r.markCompleted(task)
			log.Info("rebuild task completed for partition %d (indexed=%d >= target=%d)",
				task.PartitionID, info.IndexedNum, targetDocCount)
			return
		}
	}
}

// reachedIndexed reports whether THIS rebuild has settled into a state the
// phase-2 classifier can act on. Two confirmation paths:
//
//   - Dispatch path: engine.RebuildIndex is synchronous, so its nil return
//     (engineReturned) is authoritative — the build finished, even if too fast to
//     observe an intermediate INDEXING status. Trusted over the polled state,
//     which may lag the cgo return by a tick.
//   - Monitor-only path (engineReturned=false): confirm via the polled status.
//     A rebuild dispatched from a prior terminal state must first observe a
//     non-terminal state (UNINDEXED/INDEXING) before a settled reading is
//     attributed to it — otherwise a stale status from the previous build would
//     be mistaken for this one's result. task.AwaitTransition tracks that gate.
//
// A settled state is INDEXED (the normal case: a trained, swapped-in index — this
// now includes a non-incremental index such as DISKANN, whose Load path publishes
// INDEXED once its on-disk structure is ready; see VectorManager::Load). The one
// remaining non-INDEXED settled state is an untrained index (IsTrained=false)
// resting at UNINDEXED: it never trains (live doc count below training_threshold),
// so it would otherwise starve to the deadline. It is completed (with a WARN) in
// phase 2. Crucially, that flag is honored ONLY when status != INDEXING: while an
// index is actively training, IsTrained is transiently false, and settling then
// would report a still-building rebuild as complete.
func (r *RebuildManager) reachedIndexed(task *RebuildTask, info engine.IndexStatusInfo,
	engineReturned bool) bool {
	if engineReturned {
		return true
	}
	awaiting := observeRebuildStatus(task.AwaitTransition, info.Status)
	if awaiting != task.AwaitTransition {
		r.mu.Lock()
		task.AwaitTransition = awaiting
		r.mu.Unlock()
	}
	if awaiting {
		return false // still awaiting this rebuild's own build to start
	}
	// INDEXED is the normal settled state. An untrained index rests at UNINDEXED
	// and is settled too — but only when it is not mid-build (status != INDEXING),
	// so an actively-training index (transiently IsTrained=false) is not mistaken
	// for a terminal untrained one.
	return info.Status == indexStatusIndexed ||
		(!info.IsTrained && info.Status != indexStatusIndexing)
}

// shouldDispatchBuild reports whether this rebuild request must issue a fresh
// engine.RebuildIndex. False means monitor only — do NOT dispatch — because a
// build is running, about to run, or the index is already at a state the monitor
// can complete without rebuilding:
//
//   - INDEXING → do not dispatch. A build is genuinely mid-flight (a restart's
//     Load build, a drop-path rebuild still training, or a RebuildVectorIndex in
//     progress). Dispatching would call StopIndexingThread and restart it — an
//     interrupt + duplicate. Just observe it to completion.
//   - UNINDEXED + incremental → do not dispatch. Two sub-cases both resolve to
//     "don't": (a) a Load-triggered BuildIndex that has CAS'd indexing_state_ to
//     STARTING but not yet flipped status to INDEXING — dispatching would race a
//     build about to run (StopIndexingThread only stops a RUNNING build, so it
//     would NOT stop a STARTING one → two concurrent builds); (b) a settled
//     untrained index (doc count below training_threshold) that the monitor
//     completes via the !IsTrained gate — rebuilding would only re-swap an
//     equally-untrained index.
//   - UNINDEXED + non-incremental (DISKANN) → dispatch. It is never driven by the
//     background indexing thread (auto-build gates skip SupportIncrement()==false),
//     so UNINDEXED means "nothing is or will be building"; only a dispatch builds
//     the on-disk index.
//   - INDEXED (steady) / FAILED (recover) → dispatch a fresh build.
func shouldDispatchBuild(preStatus string, supportIncrement bool) bool {
	if preStatus == indexStatusIndexing {
		return false
	}
	if preStatus == indexStatusUnindexed && supportIncrement {
		return false
	}
	return true
}

// observeRebuildStatus reports whether the monitor is still awaiting the engine
// to start this rebuild. A rebuild dispatched from a terminal state (INDEXED or
// FAILED) must first observe a non-terminal state (UNINDEXED/INDEXING) before a
// terminal reading is attributed to it — otherwise a stale status from the
// previous build would be mistaken for this rebuild's result.
func observeRebuildStatus(awaitTransition bool, status string) bool {
	if !awaitTransition {
		return false
	}
	if status == indexStatusUnindexed || status == indexStatusIndexing {
		return false // this rebuild has started; terminal readings now count
	}
	return true // stale INDEXED/FAILED from a prior build; keep waiting
}

// pollStatus returns this rebuild's per-index status info, including the
// engine-wide doc frontier (info.MaxDocid). Field-level rebuild reads the named
// index's info in a single GetEngineStatus (IndexStatusOf now also carries
// MaxDocid), so a poll makes one cgo call, not two.
//
// The whole-partition branch (FieldName == "") is defensive: in production a
// rebuild task always carries a non-empty FieldName — Space.AllVectorIndexes
// only lists indexes with FieldName != "" (space.go), and the PS handler
// resolves the request via GetIndexByName before dispatch (handler_admin.go),
// so the engine's own whole-partition RebuildIndex path is never taken here.
// It reports the engine-wide status with both classification flags defaulted to
// true (plain "wait for indexed_num to catch up"); were it ever reached for a
// non-incremental index, that index's indexed_num could not reach the frontier
// and the monitor would wait to rebuildMaxDuration — acceptable only because the
// path is unreachable.
func (r *RebuildManager) pollStatus(task *RebuildTask, eng engine.Engine) (
	info engine.IndexStatusInfo, err error) {
	if task.FieldName == "" {
		st, idx, md := eng.IndexInfo()
		return engine.IndexStatusInfo{
			Status:           indexStatusToString(st),
			IndexedNum:       idx,
			IsTrained:        true,
			SupportIncrement: true,
			MaxDocid:         md,
		}, nil
	}
	// info.MaxDocid is engine-wide (the doc frontier), independent of which index
	// we are rebuilding; info.IndexedNum is this index's own count so a lagging
	// sibling index does not gate this rebuild's completion.
	return eng.IndexStatusOf(task.IndexName)
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

// ctxDone returns ctx.Done(), or nil when ctx is nil.
func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func (r *RebuildManager) updateProgress(task *RebuildTask, progress int) {
	r.mu.Lock()
	if progress > task.Progress {
		task.Progress = progress
	}
	r.mu.Unlock()
}

func (r *RebuildManager) markCompleted(task *RebuildTask) {
	r.mu.Lock()
	task.Status = entity.RebuildStatusCompleted
	task.Progress = 100
	task.CompleteTime = time.Now()
	task.ErrorMessage = ""
	r.mu.Unlock()
}

func (r *RebuildManager) markFailed(task *RebuildTask, msg string) {
	r.mu.Lock()
	task.Status = entity.RebuildStatusFailed
	task.ErrorMessage = msg
	task.CompleteTime = time.Now()
	r.mu.Unlock()
	log.Error("rebuild task failed for partition %d: %s", task.PartitionID, msg)
}

// Terminal tasks are evicted lazily by GetRebuildTaskStatus (and by
// StartRebuildTask on task reuse) once CompleteTime is older than
// terminalRetentionPeriod. This avoids leaking long-lived time.AfterFunc
// timers that hold references to the manager across PS shutdown.

// terminalExpired reports whether a task has stayed in terminal state
// past the retention window.
func terminalExpired(task *RebuildTask) bool {
	if task == nil || task.CompleteTime.IsZero() {
		return false
	}
	if !task.Status.IsTerminal() {
		return false
	}
	return time.Since(task.CompleteTime) > terminalRetentionPeriod
}

func (r *RebuildManager) GetRebuildTaskStatus(dbName, spaceName, indexName string,
	partitionID uint32) (status entity.RebuildStatus, errorMsg string, exists bool, progress int) {
	taskKey := r.getTaskKey(dbName, spaceName, indexName, partitionID)

	// Hold the lock across the field reads: task fields are mutated under
	// r.mu by the monitor goroutine (markCompleted/markFailed/updateProgress),
	// so the return-value expression must be evaluated before Unlock runs.
	r.mu.Lock()
	defer r.mu.Unlock()

	task, found := r.tasks[taskKey]
	if !found || task == nil {
		return "", "", false, 0
	}
	// Lazy eviction: drop retention-expired terminal tasks on read.
	if terminalExpired(task) {
		delete(r.tasks, taskKey)
		return "", "", false, 0
	}
	return task.Status, task.ErrorMessage, true, task.Progress
}

// SetRebuildManager injects a custom manager for tests or alternate wiring.
func (s *Server) SetRebuildManager(manager RebuildTaskManager) {
	s.rebuildManager = manager
}

// GetRebuildManager lazily creates the PS-side rebuild manager once.
func (s *Server) GetRebuildManager() RebuildTaskManager {
	s.rebuildOnce.Do(func() {
		if s.rebuildManager == nil {
			s.rebuildManager = NewRebuildManager(s)
		}
	})
	return s.rebuildManager
}
