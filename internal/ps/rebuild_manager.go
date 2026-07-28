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
		dropBefore int, limitCPU int, describe int) error
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
func (r *RebuildManager) StartRebuildTask(dbName, spaceName, indexName, fieldName, indexType string,
	partitionID uint32, dropBefore int, limitCPU int, describe int) error {
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
	}
	r.tasks[taskKey] = task
	r.mu.Unlock()

	go r.executeRebuild(task, dropBefore, limitCPU, describe)

	return nil
}

// executeRebuild triggers the engine rebuild and monitors it to terminal state.
func (r *RebuildManager) executeRebuild(task *RebuildTask, dropBefore int, limitCPU int, describe int) {
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

	// Pre-flight: adopt vs dispatch. A build still in flight (UNINDEXED/INDEXING)
	// — a restart's Load build, or a drop-path rebuild still training — is
	// adopted, not dispatched again; the monitor just waits. INDEXED (steady) or
	// FAILED (recover) is dispatched. Whole-partition rebuild (FieldName == "")
	// reads the engine-wide status; field-level reads the named index's status.
	var preStatus string
	var err error
	if task.FieldName == "" {
		st, _, _ := engine.IndexInfo()
		preStatus = indexStatusToString(st)
	} else {
		preStatus, err = engine.IndexStatusOf(task.IndexName)
	}
	if err != nil {
		r.markFailed(task, fmt.Sprintf("pre-rebuild status read for %q: %v", task.IndexName, err))
		return
	}
	if preStatus == indexStatusUnindexed || preStatus == indexStatusIndexing {
		task.AwaitTransition = false
		log.Info("adopting in-flight index build: pid=%d indexName=%q", task.PartitionID, task.IndexName)
		r.monitorRebuild(task, store, nil)
		return
	}
	task.AwaitTransition = true

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
		// Empty FieldName means whole-partition rebuild in the engine.
		doneCh <- engine.RebuildIndex(task.IndexName, task.FieldName, task.IndexType,
			dropBefore, limitCPU, describe)
	}()
	log.Info("rebuild engine.RebuildIndex dispatched: pid=%d indexName=%s field=%s indexType=%s dropBefore=%d limitCPU=%d describe=%d",
		task.PartitionID, task.IndexName, task.FieldName, task.IndexType,
		dropBefore, limitCPU, describe)

	// Monitor until the rebuild reaches a terminal state.
	r.monitorRebuild(task, store, doneCh)
}

// monitorRebuild polls engine per-index status until the rebuild is terminal.
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

	for {
		select {
		case err := <-doneCh:
			if err != nil {
				r.markFailed(task, fmt.Sprintf("engine.RebuildIndex: %v", err))
				return
			}
			// gammacb.RebuildIndex is synchronous: a nil result means the
			// C++ engine finished building + training and atomically swapped the
			// new index in (now INDEXED). That is the authoritative completion
			// signal — no need to observe the transient status via polling
			// (the whole rebuild can finish inside a single poll interval).
			// The adopt-in-flight path passes doneCh=nil, so this case never
			// fires there and that path keeps polling below.
			r.updateProgress(task, 100)
			r.markCompleted(task)
			log.Info("rebuild task completed for partition %d (engine returned success)",
				task.PartitionID)
			return
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

		state, indexedNum, maxDocid, infoErr := r.pollStatus(task, engine)
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

		r.updateProgress(task, computeProgress(indexedNum, maxDocid))

		awaitTransition := observeRebuildStatus(task.AwaitTransition, state)
		if awaitTransition != task.AwaitTransition {
			r.mu.Lock()
			task.AwaitTransition = awaitTransition
			r.mu.Unlock()
		}
		if awaitTransition {
			// The engine has not started this rebuild yet; the reading is a
			// stale terminal state from a prior build. Ignore it, keep polling.
			log.Debug("rebuild awaiting engine build start pid=%d indexName=%s (stale status=%s)",
				task.PartitionID, task.IndexName, state)
			continue
		}

		switch state {
		case indexStatusFailed:
			r.markFailed(task, "engine reported index status=FAILED")
			return
		case indexStatusIndexed:
			r.markCompleted(task)
			log.Info("rebuild task completed for partition %d (indexed=%d, maxDocid=%d)",
				task.PartitionID, indexedNum, maxDocid)
			return
		case indexStatusUnindexed, indexStatusIndexing:
			log.Debug("rebuild in progress pid=%d status=%s indexed=%d/%d",
				task.PartitionID, state, indexedNum, maxDocid)
		default:
			log.Warn("unknown engine status %s for pid=%d, continuing", state, task.PartitionID)
		}
	}
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

func (r *RebuildManager) pollStatus(task *RebuildTask, engine engine.Engine) (string, int, int, error) {
	if task.FieldName == "" {
		st, indexed, maxDocid := engine.IndexInfo()
		return indexStatusToString(st), indexed, maxDocid, nil
	}
	status, err := engine.IndexStatusOf(task.IndexName)
	if err != nil {
		return "", 0, 0, err
	}
	_, indexed, maxDocid := engine.IndexInfo()
	return status, indexed, maxDocid, nil
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

	// Lazy eviction: drop retention-expired terminal tasks on read.
	r.mu.Lock()
	task, found := r.tasks[taskKey]
	if found && task != nil && terminalExpired(task) {
		delete(r.tasks, taskKey)
		r.mu.Unlock()
		return "", "", false, 0
	}
	r.mu.Unlock()

	if !found || task == nil {
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
