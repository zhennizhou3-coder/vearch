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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package entity

import (
	"time"
)

// RebuildStatus is the shared lifecycle status for both per-replica tasks
// (Master↔PS RPC) and the space-level scheduling record (etcd).
//
// The wire representation is the string value; the JSON encoder emits e.g.
// "running" for either use. Values are stable and must not be renamed.
//
// Tasks only ever take Running / Completed / Failed. Pending / Cancelled are
// space-level record states. Callers that need to distinguish "record does
// not exist" from these lifecycle states should look at the API error code
// (vearchpb.ErrorEnum_REBUILD_RECORD_NOT_EXIST) instead of the status
// field — record existence is orthogonal to lifecycle state.
type RebuildStatus string

const (
	RebuildStatusPending   RebuildStatus = "pending"
	RebuildStatusRunning   RebuildStatus = "running"
	RebuildStatusCompleted RebuildStatus = "completed"
	RebuildStatusFailed    RebuildStatus = "failed"
	RebuildStatusCancelled RebuildStatus = "cancelled"
)

// IsTerminal reports whether the scheduler is done with a record in this state.
func (s RebuildStatus) IsTerminal() bool {
	switch s {
	case RebuildStatusCompleted,
		RebuildStatusFailed,
		RebuildStatusCancelled:
		return true
	default:
		return false
	}
}

// CancelRebuildResponse describes one cancel attempt.
type CancelRebuildResponse struct {
	DBName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	// Cancelled reports whether this call transitioned the record itself
	// to Cancelled (only possible while the record is Pending).
	Cancelled bool `json:"cancelled"`
	// CancelledTasks counts per-task cancellations applied by this call:
	//   - When the record is Pending, this is 0 (the record itself is
	//     cancelled; task list may be empty at that point).
	//   - When the record is Running, this is the number of tasks that
	//     were still in the plan-but-not-dispatched state and were
	//     transitioned to Cancelled by this call. Already-dispatched
	//     tasks are left running and will finish naturally.
	CancelledTasks int           `json:"cancelled_tasks,omitempty"`
	Reason         string        `json:"reason,omitempty"`
	Status         RebuildStatus `json:"status"` // the record's status at the time of cancellation
}

// RebuildTask is a single (partition, replica) index rebuild task.
//
// The same struct is used on both master and PS: each side populates the
// fields it owns and treats the rest as informational. `omitempty` on all
// side-exclusive fields keeps the wire payload compact:
//
//   - Identity, runtime state, and rebuild parameters are the shared
//     contract between master (etcd record + status polls) and PS
//     (in-memory task + status responses).
//   - Master-only scheduling metadata (NodeID, ReplicaIndex, PSNodeAddr,
//     DispatchAt, DispatchAttempts, PollRetryCount, RetryCount) is zero
//     on PS.
//   - PS-only CGo parameters (FieldName, IndexType) are zero on master.
type RebuildTask struct {
	// Identity — both sides.
	PartitionID PartitionID   `json:"partition_id"`
	DBName      string        `json:"db_name"`
	SpaceName   string        `json:"space_name"`
	IndexName   string        `json:"index_name"`
	Status      RebuildStatus `json:"status"`

	// Runtime state — PS is authoritative, master caches the latest poll.
	Progress     int       `json:"progress,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	StartTime    time.Time `json:"start_time,omitempty"`
	CompleteTime time.Time `json:"complete_time,omitempty"`

	// Rebuild parameters — master fills, PS consumes.
	DropBefore int `json:"drop_before,omitempty"` // 1: drop before rebuild
	LimitCPU   int `json:"limit_cpu,omitempty"`
	Describe   int `json:"describe,omitempty"`

	// Master-only scheduling metadata (zero on PS).
	NodeID           NodeID    `json:"node_id,omitempty"`
	ReplicaIndex     int       `json:"replica_index,omitempty"`
	PSNodeAddr       string    `json:"ps_node_addr,omitempty"`
	DispatchAt       time.Time `json:"dispatch_at,omitempty"`
	DispatchAttempts int       `json:"dispatch_attempts,omitempty"`
	PollRetryCount   int       `json:"poll_retry_count,omitempty"`
	RetryCount       int       `json:"retry_count,omitempty"`

	// PS-only CGo call parameters (zero on master).
	FieldName string `json:"field_name,omitempty"`
	IndexType string `json:"index_type,omitempty"`

	// IsTrainer marks the one replica that
	// trains this round and dumps its model; the other replicas pull that model
	// instead of training. Master-authoritative; zero when the scheme is off.
	IsTrainer bool `json:"is_trainer,omitempty"`

	// AwaitTransition is PS-local monitor state. A task dispatched while the
	// target is already INDEXED must observe a later non-INDEXED state before
	// another INDEXED can be attributed to this rebuild.
	AwaitTransition bool `json:"-"`
}

// RebuildRequest is the API payload for starting a rebuild.
type RebuildRequest struct {
	DBName      string `json:"db_name"`
	SpaceName   string `json:"space_name"`
	IndexName   string `json:"index_name,omitempty"`
	PartitionId uint32 `json:"partition_id,omitempty"` // Optional: specific partition to rebuild, 0 means all
	DropBefore  bool   `json:"drop_before_rebuild,omitempty"`
	LimitCPU    int    `json:"limit_cpu,omitempty"`
	Describe    int    `json:"describe,omitempty"`
	MaxRetries  int    `json:"max_retries,omitempty"`
}

// RebuildProgressResponse rebuild progress response
type RebuildProgressResponse struct {
	SpaceKey string `json:"space_key"`

	// Indexes lists all target index names; CurrentTarget is the active one.
	Indexes       []string `json:"indexes,omitempty"`
	CurrentIndex  int      `json:"current_index,omitempty"`
	CurrentTarget string   `json:"current_target,omitempty"`

	TotalTasks     int            `json:"total_tasks"`
	CompletedTasks int            `json:"completed_tasks"`
	FailedTasks    int            `json:"failed_tasks"`
	RunningTasks   int            `json:"running_tasks"`
	PendingTasks   int            `json:"pending_tasks"`   // planned but not yet dispatched
	SuccessRatio   float64        `json:"success_ratio"`   // Success ratio (0.0-1.0)
	OverallPercent int            `json:"overall_percent"` // 0..100, completed tasks over non-cancelled total
	Status         RebuildStatus  `json:"status"`          // overall status: running, completed, failed
	ErrorMsg       string         `json:"error_msg,omitempty"`
	EnqueuedAt     time.Time      `json:"enqueued_at,omitempty"`
	StartedAt      time.Time      `json:"started_at,omitempty"`
	FinishedAt     time.Time      `json:"finished_at,omitempty"`
	RetryCount     int            `json:"retry_count,omitempty"`
	MaxRetries     int            `json:"max_retries,omitempty"`
	Tasks          []*RebuildTask `json:"tasks,omitempty"` // detailed task list
	VersionID      string         `json:"version_id,omitempty"`
}

// RebuildSummaryResponse summarizes rebuild progress across spaces.
type RebuildSummaryResponse struct {
	Results []*RebuildProgressResponse `json:"results"`
	Total   int                        `json:"total"` // total spaces in the result set
	// Per-status counts derived from the snapshot
	CompletedCount int     `json:"completed_count"`
	FailedCount    int     `json:"failed_count"`
	CancelledCount int     `json:"cancelled_count"`
	RunningCount   int     `json:"running_count"`
	PendingCount   int     `json:"pending_count"`
	SuccessRatio   float64 `json:"success_ratio"` // (completed) / (completed + failed + cancelled + running + pending), 0 if no records
}

// RebuildStatusQuery is the rebuild status poll payload.
type RebuildStatusQuery struct {
	DBName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	IndexName string `json:"index_name"`
}

// RebuildStatusResponse describes the status of one rebuild task.
//
// When Exists=false the task has no in-memory record on PS (never registered,
// or already evicted after terminalRetentionPeriod); Status/ErrorMessage/
// Progress are then their zero values ("", "", 0).
type RebuildStatusResponse struct {
	Exists       bool          `json:"exists"`
	Status       RebuildStatus `json:"status"` // running|completed|failed; empty when Exists=false
	ErrorMessage string        `json:"error_message"`
	Progress     int           `json:"progress"` // 0-100
}

// RebuildParam is the rebuild start payload.
type RebuildParam struct {
	DBName     string `json:"db_name"`
	SpaceName  string `json:"space_name"`
	IndexName  string `json:"index_name"`
	DropBefore int    `json:"drop_before"`
	LimitCPU   int    `json:"limit_cpu"`
	Describe   int    `json:"describe"`

	// RoundID is this rebuild round's ID.
	// (SpaceRebuildRecord.RebuildID). IsTrainer marks the trainer replica (which
	// trains + dumps). For a follower, TrainerAddr is where to pull the model
	// from; empty TrainerAddr means "train locally" (scheme off).
	RoundID     string `json:"round_id,omitempty"`
	IsTrainer   bool   `json:"is_trainer,omitempty"`
	TrainerAddr string `json:"trainer_addr,omitempty"`
}

// PullTrainingArtifactsReq fetches training artifacts from a source replica.
// It is sent JSON-encoded in PartitionData.Data.
// Offset < 0 is a stat call (trainer replies with TrainingArtifactsMeta); Offset >= 0
// requests the raw chunk starting at that byte offset. The trainer is stateless
// (each request is an independent pread), so the follower drives progress.
type PullTrainingArtifactsReq struct {
	PartitionID uint32 `json:"pid"`
	IndexName   string `json:"index"`
	RoundID     string `json:"round"`  // rebuild round; trainer only serves a match
	Offset      int64  `json:"offset"` // <0 = stat; >=0 = fetch the chunk at this offset
}

// TrainingArtifactsMeta identifies one round's training model — its byte size,
// sha256, and rebuild round — and serves two roles with the same shape:
//   - the trainer's stat reply (Offset < 0): the follower uses Size to drive chunk
//     requests and SHA256 for an end-to-end integrity check;
//   - the on-disk `.meta` sidecar next to the training-artifacts file, written
//     atomically AFTER the model bytes so its presence commits "model ready for
//     this round"; a puller only trusts a model whose sidecar round/sha256 match.
type TrainingArtifactsMeta struct {
	RoundID string `json:"round"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
}

// SpaceRebuildRecord is the etcd-persisted scheduling unit for one space.
type SpaceRebuildRecord struct {
	DBName    string        `json:"db_name"`
	SpaceName string        `json:"space_name"`
	Status    RebuildStatus `json:"status"` // pending|running|completed|failed|cancelled

	// RebuildID is this round's identity, generated at StartRebuild and persisted
	// in etcd. It doubles as the puller's RoundID and anchors training-artifacts
	// identity; round isolation rides on it, not a monotonic version.
	RebuildID string `json:"rebuild_id,omitempty"`

	// Rebuild parameters propagated to PS.
	DropBefore  int    `json:"drop_before,omitempty"`
	LimitCPU    int    `json:"limit_cpu,omitempty"`
	Describe    int    `json:"describe,omitempty"`
	PartitionID uint32 `json:"partition_id,omitempty"` // 0 == all partitions

	// Indexes is the full list of index names targeted by this rebuild.
	Indexes         []string `json:"indexes"`
	CurrentIndexIdx int      `json:"current_index_idx"`

	EnqueuedAt time.Time `json:"enqueued_at"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`

	TotalTasks     int `json:"total_replicas"`
	CompletedTasks int `json:"completed_replicas"`
	FailedTasks    int `json:"failed_replicas"`

	// MaxRetries is the per-task retry budget: each replica task may be
	// retried up to MaxRetries times before it is marked failed. Per-task
	// retry counts live on RebuildTask.RetryCount.
	MaxRetries int `json:"max_retries,omitempty"`

	// Tasks is the per-replica plan for the current target.
	Tasks []*RebuildTask `json:"tasks,omitempty"`

	// CancelRequested indicates the user has asked to cancel the whole rebuild
	CancelRequested bool `json:"cancel_requested,omitempty"`
}

// SpaceKey returns the dbName-spaceName composite identifier.
func (r *SpaceRebuildRecord) SpaceKey() string {
	return r.DBName + "-" + r.SpaceName
}

// CurrentTarget returns the active index name, or empty string when done.
func (r *SpaceRebuildRecord) CurrentTarget() string {
	if r.CurrentIndexIdx >= 0 && r.CurrentIndexIdx < len(r.Indexes) {
		return r.Indexes[r.CurrentIndexIdx]
	}
	return ""
}

// HasMoreTargets reports whether another target remains after this one.
func (r *SpaceRebuildRecord) HasMoreTargets() bool {
	return r.CurrentIndexIdx+1 < len(r.Indexes)
}

// MergeCancelledFrom merges task-level Cancelled markers from persistedRecord
// into r, and propagates a CancelRequested=true flag from persistedRecord.
// Called by persistRecord to preserve a concurrent CancelRebuild's writes
// across the scheduler tick's read-modify-write cycle.
//
// Returns the number of task-level cancels merged (the flag propagation is
// not counted; it is a boolean).
func (r *SpaceRebuildRecord) MergeCancelledFrom(persistedRecord *SpaceRebuildRecord) int {
	if persistedRecord == nil {
		return 0
	}
	if persistedRecord.CancelRequested {
		r.CancelRequested = true
	}
	if len(persistedRecord.Tasks) == 0 || len(r.Tasks) == 0 {
		return 0
	}
	type taskKey struct {
		pid PartitionID
		nid NodeID
	}
	byKey := make(map[taskKey]*RebuildTask, len(r.Tasks))
	for _, t := range r.Tasks {
		if t == nil {
			continue
		}
		byKey[taskKey{t.PartitionID, t.NodeID}] = t
	}
	merged := 0
	for _, ct := range persistedRecord.Tasks {
		if ct == nil || ct.Status != RebuildStatusCancelled {
			continue
		}
		rt, ok := byKey[taskKey{ct.PartitionID, ct.NodeID}]
		if !ok {
			continue
		}
		if rt.Status != RebuildStatusPending {
			continue
		}
		rt.Status = RebuildStatusCancelled
		rt.ErrorMessage = ct.ErrorMessage
		rt.CompleteTime = ct.CompleteTime
		merged++
	}
	return merged
}
