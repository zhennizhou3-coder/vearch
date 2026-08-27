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

package services

import (
	"testing"

	"github.com/vearch/vearch/v3/internal/entity"
)

func TestPartitionReplicaCount(t *testing.T) {
	rec := &SpaceRebuildRecord{Tasks: []*entity.RebuildTask{
		{PartitionID: 1, NodeID: 10},
		{PartitionID: 1, NodeID: 11},
		{PartitionID: 1, NodeID: 12},
		{PartitionID: 2, NodeID: 10},
	}}
	if got := partitionReplicaCount(rec, 1); got != 3 {
		t.Errorf("pid=1 count: got %d want 3", got)
	}
	if got := partitionReplicaCount(rec, 2); got != 1 {
		t.Errorf("pid=2 count: got %d want 1", got)
	}
	if got := partitionReplicaCount(rec, 99); got != 0 {
		t.Errorf("pid=99 count: got %d want 0", got)
	}
}

func TestTrainerTaskOf(t *testing.T) {
	trainer := &entity.RebuildTask{PartitionID: 1, NodeID: 11, IsTrainer: true}
	rec := &SpaceRebuildRecord{Tasks: []*entity.RebuildTask{
		{PartitionID: 1, NodeID: 10},
		trainer,
		{PartitionID: 1, NodeID: 12},
		{PartitionID: 2, NodeID: 10, IsTrainer: true}, // different partition
	}}
	if got := trainerTaskOf(rec, 1); got != trainer {
		t.Errorf("pid=1 trainer: got %+v want %+v", got, trainer)
	}
	// No trainer designated yet for a partition → nil.
	if got := trainerTaskOf(rec, 3); got != nil {
		t.Errorf("pid=3 trainer: got %+v want nil", got)
	}
}
