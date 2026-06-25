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
)

type RebuildStatus int

// PS-side rebuild task status; wire values must stay stable.
const (
	RebuildStatusRunning   RebuildStatus = 1
	RebuildStatusCompleted RebuildStatus = 2
	RebuildStatusFailed    RebuildStatus = 3
)

// Gamma engine IndexStatus values from internal/engine/search/engine.h.
const (
	engineIndexStatusUnindexed = 0
	engineIndexStatusIndexing  = 1
	engineIndexStatusIndexed   = 2
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

// RebuildTask is the in-memory state of a PS-side rebuild task.
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

	// Rebuild parameters retained for status and retry context.
	DropBefore int `json:"drop_before,omitempty"`
	LimitCPU   int `json:"limit_cpu,omitempty"`
	Describe   int `json:"describe,omitempty"`
}

// RebuildManager registers PS tasks and exposes their status.
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

// getTaskKey returns the in-memory task identity key.
func (r *PSRebuildManager) getTaskKey(spaceKey, fieldName, indexType string, partitionID uint32) string {
	return fmt.Sprintf("%s|%d|%s|%s", spaceKey, partitionID, fieldName, indexType)
}

// StartRebuildTask registers a task and starts its monitor goroutine.
func (r *PSRebuildManager) StartRebuildTask(spaceKey, fieldName, indexType string,
	partitionID uint32, dropBefore int, limitCPU int, describe int) error {
	taskKey := r.getTaskKey(spaceKey, fieldName, indexType, partitionID)

	r.mu.Lock()
	if existing, ok := r.tasks[taskKey]; ok && existing.Status == RebuildStatusRunning {
		r.mu.Unlock()
		// Duplicate dispatch is idempotent.
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

// executeRebuild triggers the engine rebuild and monitors it to terminal state.
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

	// Snapshot status before rebuild to detect real progress later.
	preStatus, preIndexed, preMaxDocid, preErr := engine.IndexInfoWithErr()
	if preErr != nil {
		// Fail fast if the baseline status is unavailable.
		r.markFailed(task, fmt.Sprintf("pre-rebuild engine.IndexInfo: %v", preErr))
		return
	}

	// Rebuild requires an existing index.
	if preStatus == engineIndexStatusUnindexed {
		r.markFailed(task, "cannot rebuild: index does not exist (status=UNINDEXED)")
		return
	}

	// Run rebuild in a goroutine so monitorRebuild can observe its result.
	doneCh := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				doneCh <- fmt.Errorf("RebuildIndex panic: %v", p)
			}
		}()
		// Empty FieldName means whole-partition rebuild in the engine.
		doneCh <- engine.RebuildFieldIndex(task.FieldName, task.IndexType,
			dropBefore, limitCPU, describe)
	}()
	log.Info("rebuild engine.RebuildFieldIndex dispatched: pid=%d field=%s indexType=%s preStatus=%d preIndexed=%d preMaxDocid=%d dropBefore=%d limitCPU=%d describe=%d",
		task.PartitionID, task.FieldName, task.IndexType,
		preStatus, preIndexed, preMaxDocid, dropBefore, limitCPU, describe)

	// Monitor until the rebuild reaches a terminal state.
	r.monitorRebuild(task, store, doneCh, preStatus, preIndexed, preMaxDocid)
}

// monitorRebuild polls engine status until the rebuild is terminal.
func (r *PSRebuildManager) monitorRebuild(task *RebuildTask, store PartitionStore,
	doneCh <-chan error, preStatus, preIndexed, preMaxDocid int) {
	// serverCtx lets shutdown interrupt the poll loop.
	var serverCtx context.Context
	if r.server != nil {
		serverCtx = r.server.ctx
	}
	deadline := time.Now().Add(rebuildMaxDuration)
	ticker := time.NewTicker(rebuildPollInterval)
	defer ticker.Stop()
	// shutdownGrace bounds how long we wait for in-flight CGO to exit.
	const shutdownGrace = 5 * time.Second
	// drain waits briefly for the CGO goroutine to publish its result.
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

	// observedNonIndexed proves the engine entered rebuild work.
	observedNonIndexed := preStatus != engineIndexStatusIndexed

	// rebuildReturned makes an INDEXED reading authoritative.
	rebuildReturned := false

	failureStreak := 0

	for {
		select {
		case err := <-doneCh:
			// CGO returned; confirm final status through IndexInfo.
			if err != nil {
				r.markFailed(task, fmt.Sprintf("engine.RebuildIndex: %v", err))
				return
			}
			rebuildReturned = true
			log.Info("engine.RebuildIndex returned success for pid=%d, awaiting IndexInfo confirmation",
				task.PartitionID)
		case <-ticker.C:
		case <-ctxDone(serverCtx):
			// Give in-flight CGO a short shutdown grace window.
			r.markFailed(task, drain("PS server shutting down"))
			return
		}

		if time.Now().After(deadline) {
			r.markFailed(task, fmt.Sprintf("rebuild monitor timeout after %s", rebuildMaxDuration))
			return
		}

		engine := store.GetEngine()
		if engine == nil || engine.HasClosed() {
			// Avoid racing partition close with in-flight CGO.
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
			// Rebuild returned; INDEXED is authoritative.
			if rebuildReturned {
				r.markCompleted(task, indexedNum, maxDocid)
				log.Info("rebuild task completed for partition %d (rebuild returned, indexed=%d, maxDocid=%d)",
					task.PartitionID, indexedNum, maxDocid)
				return
			}
			if observedNonIndexed {
				// Wait briefly for CGO before marking completed.
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
					// CGO is still running; keep polling.
					continue
				}
			}
			// Rebuild has not visibly started or returned yet; keep polling.
		default:
			log.Warn("unknown engine IndexStatus %d for pid=%d, continuing", status, task.PartitionID)
		}
	}
}

// waitDoneShort waits briefly for in-flight CGO before completion.
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
		// The channel is consumed here, so fail the task inline.
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

// ctxDone returns ctx.Done(), or nil when ctx is nil.
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
	// Keep terminal state long enough for the master to poll it.
	r.scheduleTerminalEviction(task)
}

func (r *PSRebuildManager) markFailed(task *RebuildTask, msg string) {
	r.mu.Lock()
	task.Status = RebuildStatusFailed
	task.ErrorMessage = msg
	task.CompleteTime = time.Now().Unix()
	r.mu.Unlock()
	log.Error("rebuild task failed for partition %d: %s", task.PartitionID, msg)
	// Keep failure visible long enough for the master to poll it.
	r.scheduleTerminalEviction(task)
}

// scheduleTerminalEviction removes terminal tasks after the retention period.
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
	defer r.mu.RUnlock()
	task, found := r.tasks[taskKey]
	if !found || task == nil {
		return 0, "", false, 0
	}
	return int(task.Status), task.ErrorMessage, true, task.Progress
}

// SetRebuildManager injects a custom manager for tests or alternate wiring.
func (s *Server) SetRebuildManager(manager RebuildManager) {
	s.rebuildManager = manager
}

// GetRebuildManager lazily creates the PS-side rebuild manager once.
func (s *Server) GetRebuildManager() RebuildManager {
	s.rebuildOnce.Do(func() {
		if s.rebuildManager == nil {
			s.rebuildManager = NewPSRebuildManager(s)
		}
	})
	return s.rebuildManager
}
