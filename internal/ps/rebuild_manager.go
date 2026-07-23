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

// Gamma engine IndexStatus values from internal/engine/search/engine.h.
const (
	engineIndexStatusUnindexed = 0
	engineIndexStatusIndexing  = 1
	engineIndexStatusIndexed   = 2
	// Per-index-only terminal state reported through EngineStatus.IndexStatuses; the
	// engine-wide IndexStatus never reports this value.
	engineIndexStatusFailed = 3
)

// Rebuild monitor knobs.
const (
	rebuildPollInterval = 2 * time.Second
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
	StartRebuildTask(spaceKey, indexName, fieldName, indexType string, partitionID uint32,
		dropBefore int, limitCPU int, describe int) error
	GetRebuildTaskStatus(spaceKey, indexName string, partitionID uint32) (
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
// IndexName is globally unique within a space, so (spaceKey, partitionID,
// indexName) is sufficient — FieldName / IndexType are not part of the key.
func (r *RebuildManager) getTaskKey(spaceKey, indexName string, partitionID uint32) string {
	return fmt.Sprintf("%s|%d|%s", spaceKey, partitionID, indexName)
}

// StartRebuildTask registers a task and starts its monitor goroutine.
func (r *RebuildManager) StartRebuildTask(spaceKey, indexName, fieldName, indexType string,
	partitionID uint32, dropBefore int, limitCPU int, describe int) error {
	taskKey := r.getTaskKey(spaceKey, indexName, partitionID)

	r.mu.Lock()
	if existing, ok := r.tasks[taskKey]; ok && existing.Status == entity.RebuildStatusRunning {
		r.mu.Unlock()
		// Duplicate dispatch is idempotent.
		log.Info("rebuild task already running for %s pid=%d indexName=%s, ignoring duplicate start",
			spaceKey, partitionID, indexName)
		return nil
	}
	task := &RebuildTask{
		PartitionID: entity.PartitionID(partitionID),
		SpaceKey:    spaceKey,
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

	// Pre-flight: refuse to rebuild an index that does not exist. For field-
	// level rebuild the authoritative check is per-index status; for whole-
	// partition rebuild (FieldName == "") the engine-wide IndexStatus is
	// what matters.
	if task.FieldName == "" {
		preStatus, _, _, err := engine.IndexInfoWithErr()
		if err != nil {
			r.markFailed(task, fmt.Sprintf("pre-rebuild engine.IndexInfo: %v", err))
			return
		}
		if preStatus == engineIndexStatusUnindexed {
			r.markFailed(task, "cannot rebuild: index does not exist (status=UNINDEXED)")
			return
		}
	} else {
		preStatus, err := engine.IndexStatusOf(task.IndexName)
		if err != nil {
			r.markFailed(task, fmt.Sprintf("pre-rebuild engine.IndexStatusOf(%q): %v", task.IndexName, err))
			return
		}
		if preStatus == engineIndexStatusUnindexed {
			r.markFailed(task, fmt.Sprintf("cannot rebuild: index %q does not exist (status=UNINDEXED)", task.IndexName))
			return
		}
	}

	// engine.RebuildFieldIndex is async at the gammacb layer for field-level
	// rebuild — it kicks off a goroutine under indexLocker and returns
	// immediately. doneCh therefore reports pre-flight errors (partition
	// closed) and closes almost right away; the authoritative completion /
	// failure signal is per-index status, polled in monitorRebuild.
	doneCh := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				doneCh <- fmt.Errorf("RebuildIndex panic: %v", p)
			}
		}()
		// Empty FieldName means whole-partition rebuild in the engine.
		doneCh <- engine.RebuildFieldIndex(task.IndexName, task.FieldName, task.IndexType,
			dropBefore, limitCPU, describe)
	}()
	log.Info("rebuild engine.RebuildFieldIndex dispatched: pid=%d indexName=%s field=%s indexType=%s dropBefore=%d limitCPU=%d describe=%d",
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
			// gammacb dispatch reported a synchronous error (partition
			// closed, panic during dispatch). Failure is terminal.
			if err != nil {
				r.markFailed(task, fmt.Sprintf("engine.RebuildIndex: %v", err))
				return
			}
			// nil means dispatch succeeded — NOT that the rebuild is
			// finished. Keep polling per-index status.
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

		// Field-level rebuild reads per-index status; whole-partition
		// rebuild (FieldName == "") still reads the engine-wide status
		// because the target is the whole engine, not one index.
		status, indexedNum, maxDocid, infoErr := r.pollStatus(task, engine)
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

		switch status {
		case engineIndexStatusFailed:
			r.markFailed(task, "engine reported per-index status=FAILED")
			return
		case engineIndexStatusIndexed:
			r.markCompleted(task)
			log.Info("rebuild task completed for partition %d (indexed=%d, maxDocid=%d)",
				task.PartitionID, indexedNum, maxDocid)
			return
		case engineIndexStatusUnindexed, engineIndexStatusIndexing:
			log.Debug("rebuild in progress pid=%d status=%d indexed=%d/%d",
				task.PartitionID, status, indexedNum, maxDocid)
		default:
			log.Warn("unknown engine status %d for pid=%d, continuing", status, task.PartitionID)
		}
	}
}

func (r *RebuildManager) pollStatus(task *RebuildTask, engine engine.Engine) (int, int, int, error) {
	if task.FieldName == "" {
		return engine.IndexInfoWithErr()
	}
	status, err := engine.IndexStatusOf(task.IndexName)
	if err != nil {
		return 0, 0, 0, err
	}
	_, indexed, maxDocid, mdErr := engine.IndexInfoWithErr()
	if mdErr != nil {
		// Not fatal — treat as unknown counters; caller sees 0% progress.
		indexed = 0
		maxDocid = 0
	}
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

func (r *RebuildManager) GetRebuildTaskStatus(spaceKey, indexName string,
	partitionID uint32) (status entity.RebuildStatus, errorMsg string, exists bool, progress int) {
	taskKey := r.getTaskKey(spaceKey, indexName, partitionID)

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
