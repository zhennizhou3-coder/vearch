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
// space-level record states; NotFound is a synthetic value produced by GET
// when no record exists and is never persisted.
type RebuildStatus string

const (
	RebuildStatusPending   RebuildStatus = "pending"
	RebuildStatusRunning   RebuildStatus = "running"
	RebuildStatusCompleted RebuildStatus = "completed"
	RebuildStatusFailed    RebuildStatus = "failed"
	RebuildStatusCancelled RebuildStatus = "cancelled"
	RebuildStatusNotFound  RebuildStatus = "not_found"
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

// CancelRebuildRequest cancels rebuild work for a whole space.
type CancelRebuildRequest struct {
	DBName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	IndexName string `json:"index_name,omitempty"`
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
//     Dispatched, DispatchAt, DispatchAttempts, PollFailureStreak,
//     RetryCount) is zero on PS.
//   - PS-only CGo parameters (FieldName, IndexType) are zero on master.
type RebuildTask struct {
	// Identity — both sides.
	PartitionID PartitionID   `json:"partition_id"`
	SpaceKey    string        `json:"space_key"` // dbName-spaceName
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
	NodeID            NodeID    `json:"node_id,omitempty"`
	ReplicaIndex      int       `json:"replica_index,omitempty"`
	PSNodeAddr        string    `json:"ps_node_addr,omitempty"`
	Dispatched        bool      `json:"dispatched,omitempty"`
	DispatchAt        time.Time `json:"dispatch_at,omitempty"`
	DispatchAttempts  int       `json:"dispatch_attempts,omitempty"`
	PollFailureStreak int       `json:"poll_failure_streak,omitempty"`
	RetryCount        int       `json:"retry_count,omitempty"`

	// PS-only CGo call parameters (zero on master).
	FieldName string `json:"field_name,omitempty"`
	IndexType string `json:"index_type,omitempty"`
}

// RebuildRequest is the API payload for starting a rebuild.
type RebuildRequest struct {
	DBName      string `json:"db_name"`
	SpaceName   string `json:"space_name"`
	PartitionId uint32 `json:"partition_id,omitempty"` // Optional: specific partition to rebuild, 0 means all
	IndexName   string `json:"index_name,omitempty"`
	DropBefore  bool   `json:"drop_before_rebuild,omitempty"`
	LimitCPU    int    `json:"limit_cpu,omitempty"`
	Describe    int    `json:"describe,omitempty"`
	MaxRetries  int    `json:"max_retries,omitempty"` // Optional: max retry times for the whole space, 0 == use default
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
	OverallPercent int            `json:"overall_percent"` // 0..100, weighted across all tasks
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
	NotFoundCount  int     `json:"not_found_count"`
	SuccessRatio   float64 `json:"success_ratio"` // (completed) / (completed + failed + cancelled + running + pending), 0 if no records
}

// PSRebuildStatusQuery is the master-to-PS status poll payload.
type PSRebuildStatusQuery struct {
	SpaceKey  string `json:"space_key"`
	IndexName string `json:"index_name"`
}

// PSRebuildStatusResponse rebuild status response.
//
// When Exists=false the task has no in-memory record on PS (never registered,
// or already evicted after terminalRetentionPeriod); Status/ErrorMessage/
// Progress are then their zero values ("", "", 0).
type PSRebuildStatusResponse struct {
	Exists       bool          `json:"exists"`
	Status       RebuildStatus `json:"status"` // running|completed|failed; empty when Exists=false
	ErrorMessage string        `json:"error_message"`
	Progress     int           `json:"progress"` // 0-100
}

// PSRebuildParam is the master-to-PS rebuild start payload.
type PSRebuildParam struct {
	SpaceKey   string `json:"space_key"`
	IndexName  string `json:"index_name"`
	DropBefore int    `json:"drop_before"`
	LimitCPU   int    `json:"limit_cpu"`
	Describe   int    `json:"describe"`
}

// SpaceRebuildRecord is the etcd-persisted scheduling unit for one space.
type SpaceRebuildRecord struct {
	DBName    string        `json:"db_name"`
	SpaceName string        `json:"space_name"`
	Status    RebuildStatus `json:"status"` // pending|running|completed|failed|cancelled

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

	// Retry control is partition-scoped.
	RetryCount       int                 `json:"retry_count,omitempty"`
	MaxRetries       int                 `json:"max_retries,omitempty"`
	PartitionRetries map[PartitionID]int `json:"partition_retries,omitempty"`

	// Tasks is the per-replica plan for the current target.
	Tasks []*RebuildTask `json:"tasks,omitempty"`
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
