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

import (
	"testing"

	"github.com/vearch/vearch/v3/internal/pkg/vjson"
)

// Training-artifacts transfer structs must round-trip through vjson unchanged.
func TestPullTrainingArtifactsReqRoundTrip(t *testing.T) {
	for _, want := range []PullTrainingArtifactsReq{
		{PartitionID: 7, IndexName: "vec_ivfpq", RoundID: "round-abc", Offset: -1}, // stat
		{PartitionID: 7, IndexName: "vec_ivfpq", RoundID: "round-abc", Offset: 0},  // first chunk
		{PartitionID: 42, IndexName: "vec_ivfpq", RoundID: "round-xyz", Offset: 10 << 20},
	} {
		b, err := vjson.Marshal(&want)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var got PullTrainingArtifactsReq
		if err := vjson.Unmarshal(b, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got != want {
			t.Errorf("round-trip mismatch: got %+v want %+v", got, want)
		}
	}
}

func TestTrainingArtifactsMetaRoundTrip(t *testing.T) {
	want := TrainingArtifactsMeta{RoundID: "round-abc", SHA256: "cafebabe", Size: 201326592}
	b, err := vjson.Marshal(&want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got TrainingArtifactsMeta
	if err := vjson.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != want {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, want)
	}
}

// RebuildParam carries the round orchestration fields to PS; they must survive
// the wire so a follower learns where to pull and which round.
func TestRebuildParamCarriesRoundFields(t *testing.T) {
	want := RebuildParam{
		DBName: "db", SpaceName: "sp", IndexName: "idx",
		RoundID: "r1", IsTrainer: false, TrainerAddr: "10.0.0.2:8081",
	}
	b, err := vjson.Marshal(&want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RebuildParam
	if err := vjson.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.RoundID != want.RoundID || got.IsTrainer != want.IsTrainer || got.TrainerAddr != want.TrainerAddr {
		t.Errorf("round fields lost: got %+v want %+v", got, want)
	}
}
