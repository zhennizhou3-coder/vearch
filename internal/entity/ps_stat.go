// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package entity

import "time"

/*
PSStat is the load snapshot of a PS node.
etcd path: /ps_stat/<node_id>, TTL 90s.
*/
type PSStat struct {
	NodeID     NodeID `json:"node_id"`
	UpdateTime int64  `json:"update_time"` // unix milliseconds
	// Capacity (used for AssignedScore weighting)
	MemCapacityBytes  uint64 `json:"mem_capacity_bytes"`
	DiskCapacityBytes uint64 `json:"disk_capacity_bytes"`
	DiskFreeBytes     uint64 `json:"disk_free_bytes"`
	// Resource usage; only disk is a hard constraint in Phase 1
	DiskUsage float64 `json:"disk_usage"` // 0~1
	// Partition / data volume aggregates (core fields)
	PartitionCount int    `json:"partition_count"`
	LeaderCount    int    `json:"leader_count"` // observed only; no leader balancing in Phase 1
	TotalDocNum    uint64 `json:"total_doc_num"`
	TotalDataBytes uint64 `json:"total_data_bytes"`
	// Per-space breakdown (used for two-level scoring / signature merging)
	SpaceStats map[SpaceID]*SpaceLoadOnPS `json:"space_stats,omitempty"`
}

// SpaceLoadOnPS is the load slice of one space on one PS.
type SpaceLoadOnPS struct {
	SpaceID        SpaceID       `json:"space_id"`
	PartitionCount int           `json:"partition_count"`
	DataBytes      uint64        `json:"data_bytes"`
	DocNum         uint64        `json:"doc_num"`
	PartitionIDs   []PartitionID `json:"partition_ids,omitempty"`
	// Partition-level data volume for exact victim selection.
	PartitionBytes map[PartitionID]uint64 `json:"partition_bytes,omitempty"`
	// Partition-level doc count
	PartitionDocNums map[PartitionID]uint64 `json:"partition_doc_nums,omitempty"`
}

// IsStale reports whether the snapshot is older than staleSec.
func (s *PSStat) IsStale(staleSec int64) bool {
	if s.UpdateTime == 0 {
		return true
	}
	return (time.Now().UnixMilli() - s.UpdateTime) > staleSec*1000
}
