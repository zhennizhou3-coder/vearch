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

// PSRebuildTaskStatus is the per-replica rebuild status on PS.
// Pending dispatch is a master-side state; PS sees tasks as Running once
// registered. Values are wire-stable and must not be reordered.
type PSRebuildTaskStatus int

const (
	PSRebuildTaskStatusRunning   PSRebuildTaskStatus = 1
	PSRebuildTaskStatusCompleted PSRebuildTaskStatus = 2
	PSRebuildTaskStatusFailed    PSRebuildTaskStatus = 3
)

// Master record lifecycle statuses, persisted in etcd.
const (
	RebuildStatusPending   = "pending"
	RebuildStatusRunning   = "running"
	RebuildStatusCompleted = "completed"
	RebuildStatusFailed    = "failed"
	RebuildStatusCancelled = "cancelled"
	RebuildStatusNotFound  = "not_found"
)

// IsRebuildTerminalStatus reports whether the scheduler is done with a status.
func IsRebuildTerminalStatus(status string) bool {
	switch status {
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
	// Cancelled is true only when a pending record became cancelled.
	Cancelled bool   `json:"cancelled"`
	Reason    string `json:"reason,omitempty"`
	Status    string `json:"status"` // the record's status at the time of cancellation
}

// PartitionRebuildTask is one replica rebuild task persisted in a space record.
type PartitionRebuildTask struct {
	PartitionID  PartitionID         `json:"partition_id"`
	NodeID       NodeID              `json:"node_id"`
	ReplicaIndex int                 `json:"replica_index"` // Replica index (0, 1, 2, ...)
	PSNodeAddr   string              `json:"ps_node_addr"`
	SpaceKey     string              `json:"space_key"` // dbName-spaceName
	TaskType     string              `json:"task_type"` // task type: rebuild
	IndexName    string              `json:"index_name"`
	Status       PSRebuildTaskStatus `json:"status"`
	// Dispatched is true after the master sends ExecuteRebuildIndex.
	Dispatched bool      `json:"dispatched,omitempty"`
	DispatchAt time.Time `json:"dispatch_at,omitempty"`
	// DispatchAttempts caps retries before PS registers the task.
	DispatchAttempts int `json:"dispatch_attempts,omitempty"`
	// PollFailureStreak tracks consecutive failed status polls.
	PollFailureStreak int       `json:"poll_failure_streak,omitempty"`
	RetryCount        int       `json:"retry_count"`
	MaxRetries        int       `json:"max_retries"`
	LastError         error     `json:"-"`
	LastErrorMsg      string    `json:"last_error,omitempty"`
	StartTime         time.Time `json:"start_time"`
	CompleteTime      time.Time `json:"complete_time"`
	DropBefore        int       `json:"drop_before"` // 1: drop before rebuild, 0: not drop
	LimitCPU          int       `json:"limit_cpu"`   // CPU limit
	Describe          int       `json:"describe"`    // Describe level
	// Progress is the latest 0..100 percentage reported by PS.
	Progress int `json:"progress,omitempty"`
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

	TotalTasks     int                     `json:"total_tasks"`
	CompletedTasks int                     `json:"completed_tasks"`
	FailedTasks    int                     `json:"failed_tasks"`
	RunningTasks   int                     `json:"running_tasks"`
	PendingTasks   int                     `json:"pending_tasks"`   // planned but not yet dispatched
	SuccessRatio   float64                 `json:"success_ratio"`   // Success ratio (0.0-1.0)
	OverallPercent int                     `json:"overall_percent"` // 0..100, weighted across all tasks
	Status         string                  `json:"status"`          // overall status: running, completed, failed
	ErrorMsg       string                  `json:"error_msg,omitempty"`
	EnqueuedAt     time.Time               `json:"enqueued_at,omitempty"`
	StartedAt      time.Time               `json:"started_at,omitempty"`
	FinishedAt     time.Time               `json:"finished_at,omitempty"`
	RetryCount     int                     `json:"retry_count,omitempty"`
	MaxRetries     int                     `json:"max_retries,omitempty"`
	Tasks          []*PartitionRebuildTask `json:"tasks,omitempty"` // detailed task list
	VersionID      string                  `json:"version_id,omitempty"`
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

// PSRebuildStatusResponse rebuild status response
type PSRebuildStatusResponse struct {
	Exists       bool   `json:"exists"`
	Status       int    `json:"status"` // 0=init, 1=running, 2=completed, 3=failed (PSRebuildTaskStatus)
	ErrorMessage string `json:"error_message"`
	Progress     int    `json:"progress"` // 0-100
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
	DBName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	Status    string `json:"status"` // pending|running|completed|failed|cancelled

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

	TotalReplicas     int `json:"total_replicas"`
	CompletedReplicas int `json:"completed_replicas"`
	FailedReplicas    int `json:"failed_replicas"`

	// Retry control is partition-scoped.
	RetryCount       int                 `json:"retry_count,omitempty"`
	MaxRetries       int                 `json:"max_retries,omitempty"`
	PartitionRetries map[PartitionID]int `json:"partition_retries,omitempty"`

	// Tasks is the per-replica plan for the current target.
	Tasks []*PartitionRebuildTask `json:"tasks,omitempty"`
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
