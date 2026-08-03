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

package entity

// IndexStatus describes the state of one named vector index.
type IndexStatus struct {
	IndexName string `json:"index_name"`
	// Status is the engine's stringified IndexStatus:
	// "UNINDEXED"/"INDEXING"/"INDEXED"/"FAILED".
	Status string `json:"status"`
	// IndexedNum is the number of vectors actually added to this index so far.
	// The rebuild monitor gates completion on this catching up to the doc count
	// snapshot, so the index is not reported complete while the background
	// AddRTVecsToIndex pass is still backfilling it. Absent (0) from engines
	// predating this field, in which case the monitor falls back to MinIndexedNum.
	// Named to match the sibling MinIndexedNum / min_indexed_num surface field.
	IndexedNum int64 `json:"indexed_num,omitempty"`
}

type EngineStatus struct {
	IndexStatus   int32         `json:"index_status,omitempty"`
	BackupStatus  int32         `json:"backup_status,omitempty"`
	DocNum        int32         `json:"doc_num,omitempty"`
	MinIndexedNum int32         `json:"min_indexed_num,omitempty"`
	MaxDocid      int32         `json:"max_docid,omitempty"`
	IndexStatuses []IndexStatus `json:"index_statuses,omitempty"`
	// Per-index build state (index name → "BUILDING"/"READY"/"FAILED") for
	// dynamically-added scalar/composite indexes. Absent for older engines.
	IndexBuildState map[string]string `json:"index_build_state,omitempty"`
}
