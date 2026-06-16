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
	"fmt"
	"strings"
	"time"
)

// RebuildTaskStatus is the per-replica rebuild task status, persisted inside
// SpaceRebuildRecord.Tasks and exchanged on the master <-> PS wire.
//
// Only three real states exist:
//
//	Running   - dispatched to PS and not yet terminal
//	Completed - PS reported successful rebuild
//	Failed    - PS reported failure (or PS itself unreachable beyond retries)
//
// "Pending dispatch" (master built the plan but hasn't issued the RPC yet) is
// represented by Status=Running + PartitionRebuildTask.Dispatched=false, so a
// dedicated Inited state is not needed.
type RebuildTaskStatus int

const (
	RebuildStatusRunning   RebuildTaskStatus = 1
	RebuildStatusCompleted RebuildTaskStatus = 2
	RebuildStatusFailed    RebuildTaskStatus = 3
)

// Rebuild task string constants
const (
	RebuildStatusStringCompleted = "completed"
	RebuildStatusStringFailed    = "failed"
	RebuildStatusStringRunning   = "running"
	RebuildStatusStringPending   = "pending"
	RebuildStatusStringCancelled = "cancelled"
	RebuildStatusStringNotFound  = "not_found"
)

// IsRebuildTerminalStatus reports whether a space-level status is terminal
// (i.e. the scheduler will no longer process the record).
func IsRebuildTerminalStatus(status string) bool {
	switch status {
	case RebuildStatusStringCompleted,
		RebuildStatusStringFailed,
		RebuildStatusStringCancelled:
		return true
	default:
		return false
	}
}

// CancelRebuildRequest is the API-level request payload for cancelling
// in-progress rebuild operations. Cancellation operates at the space
// granularity; field_name and index_type are accepted for URL consistency
// but are ignored — the whole space record is cancelled.
type CancelRebuildRequest struct {
	Database  string `json:"database"`
	Space     string `json:"space"`
	FieldName string `json:"field_name,omitempty"`
	IndexType string `json:"index_type,omitempty"`
}

// CancelRebuildResponse describes the outcome of a cancel request for a
// single space.
type CancelRebuildResponse struct {
	DBName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	// Cancelled is true when the pending record was successfully transitioned
	// to "cancelled" status. Running/completed/failed/cancelled records return
	// Cancelled=false (running is not cancellable; terminal records are
	// already done).
	Cancelled bool   `json:"cancelled"`
	Reason    string `json:"reason,omitempty"`
	Status    string `json:"status"` // the record's status at the time of cancellation
}

// IndexTarget is the minimum rebuild unit: a (field, index_type) pair on
// a space. Today one field carries at most one index, so the pair is
// unambiguous; tomorrow when a field may have multiple indexes (e.g. HNSW
// + IVFPQ on the same vector column) the pair still suffices as long as
// no field has two indexes of the same type — enforced upstream by space
// schema validation. We persist the pair rather than an opaque Index.Name
// because every other layer (PS task identity key, engine call site,
// status query) reasons in (field, type) terms instead of a synthetic name.
type IndexTarget struct {
	FieldName string `json:"field_name"`
	IndexType string `json:"index_type"`
}

// Equal compares two targets. IndexType is matched case-insensitively
// because user-supplied input may arrive lower/mixed case while the
// engine canonicalises to upper (HNSW, IVFPQ, SCALAR, ...).
func (t IndexTarget) Equal(other IndexTarget) bool {
	return t.FieldName == other.FieldName &&
		strings.EqualFold(t.IndexType, other.IndexType)
}

// IsZero reports whether the target is the empty target (used to mean
// "cursor past the end" inside SpaceRebuildRecord).
func (t IndexTarget) IsZero() bool {
	return t.FieldName == "" && t.IndexType == ""
}

// String returns a stable "field:indexType" rendering used in logs and as
// the per-task identity suffix on the PS side. Both halves are kept verbatim
// (no upper/lowercasing) so that round-tripping through the wire never
// mutates the user-visible name; case-insensitive comparison is the
// responsibility of Equal.
func (t IndexTarget) String() string {
	return t.FieldName + ":" + t.IndexType
}

// NormalizeRebuildTarget validates the (fieldName, indexType) pair carried on
// API/service payloads. The two values must either be both empty (meaning
// "fan-out across every index defined on the space") or both non-empty
// (meaning "rebuild exactly this target"). Mixed input is almost always a
// caller bug and is rejected up front so we don't silently rebuild a wrong
// target. The returned (field, indexType) preserves whatever the user sent
// — case canonicalisation, if any, is performed downstream by the schema
// matcher.
func NormalizeRebuildTarget(fieldName, indexType string) (string, string, error) {
	hasField := fieldName != ""
	hasType := indexType != ""
	if hasField != hasType {
		return "", "", fmt.Errorf("field_name and index_type must be specified together (got field_name=%q, index_type=%q)",
			fieldName, indexType)
	}
	return fieldName, indexType, nil
}

// PartitionRebuildTask partition rebuild task. Persisted inside
// SpaceRebuildRecord.Tasks so that the scheduler is fully stateless and can
// reconstruct its decisions from etcd alone.
//
// FieldName + IndexType identify the specific (field, index_type) this
// task rebuilds. Both are part of the PS-side task identity key so that
// future per-field rebuild (when gamma exposes the API) can run
// concurrent tasks on the same partition without naming collision.
// Today the engine still does whole-partition rebuilds (see
// gammacb.RebuildFieldIndex), so PSRebuildManager rejects overlapping
// (field, indexType) on the same pid as a defensive measure.
type PartitionRebuildTask struct {
	PartitionID  PartitionID       `json:"partition_id"`
	NodeID       NodeID            `json:"node_id"`
	ReplicaIndex int               `json:"replica_index"` // Replica index (0, 1, 2, ...)
	PSNodeAddr   string            `json:"ps_node_addr"`
	SpaceKey     string            `json:"space_key"` // dbName-spaceName
	TaskType     string            `json:"task_type"` // task type: rebuild
	FieldName    string            `json:"field_name"`
	IndexType    string            `json:"index_type"`
	Status       RebuildTaskStatus `json:"status"`
	// Dispatched is true once the master has issued ExecuteRebuildIndex RPC
	// to the PS. The monitor only polls dispatched-but-not-terminal tasks.
	Dispatched bool      `json:"dispatched,omitempty"`
	DispatchAt time.Time `json:"dispatch_at,omitempty"`
	// DispatchAttempts counts how many times we have (re)issued the
	// ExecuteRebuildIndex RPC for this task. Used to cap retries when
	// dispatching repeatedly fails before the PS can register the task.
	DispatchAttempts int `json:"dispatch_attempts,omitempty"`
	// PollFailureStreak counts consecutive failed GetRebuildStatus RPCs to
	// the PS. It is reset to zero on any successful poll. P1-#11 uses this
	// to convert "PS unreachable forever" from a silent hang into a real
	// terminal failure: once the streak crosses maxPollFailureStreak we
	// stop trying and mark the task failed instead of looping forever.
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
	// Progress is the latest 0..100 percentage reported by the PS for this
	// replica. It is updated on every successful GetRebuildStatus poll and
	// persisted with the task so a master restart does not zero out the
	// last known progress. Terminal Completed tasks are pinned to 100.
	Progress int `json:"progress,omitempty"`
}

// Target returns the (field, index_type) tuple this task rebuilds.
func (t *PartitionRebuildTask) Target() IndexTarget {
	return IndexTarget{FieldName: t.FieldName, IndexType: t.IndexType}
}

// RebuildRequest is the API-level request payload. FieldName + IndexType
// together specify the rebuild target; passing both empty means "rebuild
// every index defined on this space" (the fan-out path). Passing just one
// is rejected at the API layer to avoid silently doing the wrong thing.
type RebuildRequest struct {
	Database    string `json:"database"`
	Space       string `json:"space"`
	PartitionId uint32 `json:"partition_id,omitempty"` // Optional: specific partition to rebuild, 0 means all
	FieldName   string `json:"field_name,omitempty"`
	IndexType   string `json:"index_type,omitempty"`
	DropBefore  bool   `json:"drop_before_rebuild,omitempty"`
	LimitCPU    int    `json:"limit_cpu,omitempty"`
	Describe    int    `json:"describe,omitempty"`
	MaxRetries  int    `json:"max_retries,omitempty"` // Optional: max retry times for the whole space, 0 == use default
}

// RebuildProgressResponse rebuild progress response
type RebuildProgressResponse struct {
	SpaceKey string `json:"space_key"`

	// Indexes lists every (field, index_type) target this record covers.
	// CurrentIndex is the 1-based cursor into Indexes; when CurrentIndex
	// == len(Indexes) the record is on its final target. CurrentTarget
	// is the entry at the cursor for callers that don't want to index
	// into the list themselves.
	Indexes       []IndexTarget `json:"indexes,omitempty"`
	CurrentIndex  int           `json:"current_index,omitempty"`
	CurrentTarget IndexTarget   `json:"current_target,omitempty"`

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

// RebuildSummaryResponse is the response for the list rebuild progress API.
// It aggregates per-space rebuild statuses from etcd and provides summary
// statistics. Because each space's status is updated independently and
// asynchronously, the summary is an eventually-consistent snapshot — spaces
// may have transitioned between states by the time the response is consumed.
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

// RebuildStatusQuery is master -> PS RPC payload for status polling.
// FieldName + IndexType identify which specific in-flight task we're
// asking about — required because PS may host multiple tasks for the
// same (spaceKey, pid) once gamma supports concurrent per-field rebuild.
type RebuildStatusQuery struct {
	SpaceKey  string `json:"space_key"`
	FieldName string `json:"field_name"`
	IndexType string `json:"index_type"`
}

// RebuildStatusResponse rebuild status response
type RebuildStatusResponse struct {
	Exists       bool   `json:"exists"`
	Status       int    `json:"status"` // 0=init, 1=running, 2=completed, 3=failed
	ErrorMessage string `json:"error_message"`
	Progress     int    `json:"progress"` // 0-100
}

// RebuildIndexParam is master -> PS RPC payload to start a rebuild task.
// SpaceKey ("dbName-spaceName") + FieldName + IndexType is the full
// identity that PS uses to register the task and master uses to poll it.
type RebuildIndexParam struct {
	SpaceKey   string `json:"space_key"`
	FieldName  string `json:"field_name"`
	IndexType  string `json:"index_type"`
	DropBefore int    `json:"drop_before"`
	LimitCPU   int    `json:"limit_cpu"`
	Describe   int    `json:"describe"`
}

// SpaceRebuildRecord is the durable, etcd-persisted scheduling unit.
// One record per space, keyed by /rebuild/index/space/{dbName}/{spaceName}.
type SpaceRebuildRecord struct {
	DBName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	Status    string `json:"status"` // pending|running|completed|failed

	// rebuild parameters propagated to PS
	DropBefore  int    `json:"drop_before,omitempty"`
	LimitCPU    int    `json:"limit_cpu,omitempty"`
	Describe    int    `json:"describe,omitempty"`
	PartitionID uint32 `json:"partition_id,omitempty"` // 0 == all partitions

	// Indexes is the full target list
	Indexes         []IndexTarget `json:"indexes"`
	CurrentIndexIdx int           `json:"current_index_idx"`

	EnqueuedAt time.Time `json:"enqueued_at"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`

	TotalReplicas     int `json:"total_replicas"`
	CompletedReplicas int `json:"completed_replicas"`
	FailedReplicas    int `json:"failed_replicas"`

	// Retry control is partition-scoped
	RetryCount       int                 `json:"retry_count,omitempty"`
	MaxRetries       int                 `json:"max_retries,omitempty"`
	PartitionRetries map[PartitionID]int `json:"partition_retries,omitempty"`

	// Tasks is the per-replica plan for the CURRENT target only
	Tasks []*PartitionRebuildTask `json:"tasks,omitempty"`
}

// SpaceKey returns the dbName-spaceName composite identifier.
func (r *SpaceRebuildRecord) SpaceKey() string {
	return r.DBName + "-" + r.SpaceName
}

// CurrentTarget returns the (field, index_type) currently being processed,
// or the zero IndexTarget when the cursor has run off the end of Indexes
// (i.e. the record is ready to be finalized).
func (r *SpaceRebuildRecord) CurrentTarget() IndexTarget {
	if r.CurrentIndexIdx >= 0 && r.CurrentIndexIdx < len(r.Indexes) {
		return r.Indexes[r.CurrentIndexIdx]
	}
	return IndexTarget{}
}

// HasMoreTargets reports whether the cursor still has work after the
// current target. Used by finalize to decide between advancing the
// cursor and finishing the whole record.
func (r *SpaceRebuildRecord) HasMoreTargets() bool {
	return r.CurrentIndexIdx+1 < len(r.Indexes)
}
