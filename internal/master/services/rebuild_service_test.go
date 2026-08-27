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

package services

import (
	"testing"

	"github.com/vearch/vearch/v3/internal/entity"
)

// task builds a RebuildTask with the given status, in-flight progress, and
// per-task retry count. Only the fields rebuildProgressFromRecord reads matter.
func task(status entity.RebuildStatus, progress, retries int) *entity.RebuildTask {
	return &entity.RebuildTask{Status: status, Progress: progress, RetryCount: retries}
}

// TestRebuildProgressFromRecord_OverallPercent pins the overall_percent
// contract: it is completed tasks over the non-cancelled total, so running
// (even at in-flight progress 100) and pending tasks contribute 0, cancelled
// tasks leave the denominator, and failed tasks stay in it to hold the bar
// below 100.
func TestRebuildProgressFromRecord_OverallPercent(t *testing.T) {
	cases := []struct {
		name    string
		rec     *entity.SpaceRebuildRecord
		percent int
		ratio   float64
		running int
		pending int
		retries int
	}{
		{
			name: "all completed reaches 100",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 3, CompletedTasks: 3,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusCompleted, 100, 0),
					task(entity.RebuildStatusCompleted, 100, 0),
					task(entity.RebuildStatusCompleted, 100, 0),
				},
			},
			percent: 100, ratio: 1.0,
		},
		{
			// drop=true reports the pre-existing index's indexed_num as
			// progress=100 while still training; a running task must not
			// inflate the bar.
			name: "running at progress 100 contributes 0",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 2, CompletedTasks: 0,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusRunning, 100, 0),
					task(entity.RebuildStatusRunning, 100, 0),
				},
			},
			percent: 0, ratio: 0.0, running: 2,
		},
		{
			name: "partial completion",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 4, CompletedTasks: 2,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusCompleted, 100, 0),
					task(entity.RebuildStatusCompleted, 100, 0),
					task(entity.RebuildStatusRunning, 100, 0),
					task(entity.RebuildStatusPending, 0, 0),
				},
			},
			percent: 50, ratio: 0.5, running: 1, pending: 1,
		},
		{
			name: "failed task holds bar below 100",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 2, CompletedTasks: 1, FailedTasks: 1,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusCompleted, 100, 0),
					task(entity.RebuildStatusFailed, 40, 0),
				},
			},
			percent: 50, ratio: 0.5,
		},
		{
			name: "cancelled task leaves the denominator",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 2, CompletedTasks: 1,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusCompleted, 100, 0),
					task(entity.RebuildStatusCancelled, 0, 0),
				},
			},
			percent: 100, ratio: 0.5,
		},
		{
			name: "all cancelled yields zero denominator, percent 0",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 2, CompletedTasks: 0,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusCancelled, 0, 0),
					task(entity.RebuildStatusCancelled, 0, 0),
				},
			},
			percent: 0, ratio: 0.0,
		},
		{
			name:    "no tasks yields percent 0",
			rec:     &entity.SpaceRebuildRecord{TotalTasks: 0},
			percent: 0, ratio: 0.0,
		},
		{
			name: "retry counts aggregate across tasks",
			rec: &entity.SpaceRebuildRecord{
				TotalTasks: 2, CompletedTasks: 1, FailedTasks: 1,
				Tasks: []*entity.RebuildTask{
					task(entity.RebuildStatusCompleted, 100, 2),
					task(entity.RebuildStatusFailed, 0, 3),
				},
			},
			percent: 50, ratio: 0.5, retries: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := rebuildProgressFromRecord(tc.rec)
			if resp.OverallPercent != tc.percent {
				t.Errorf("OverallPercent = %d, want %d", resp.OverallPercent, tc.percent)
			}
			if resp.SuccessRatio != tc.ratio {
				t.Errorf("SuccessRatio = %v, want %v", resp.SuccessRatio, tc.ratio)
			}
			if resp.RunningTasks != tc.running {
				t.Errorf("RunningTasks = %d, want %d", resp.RunningTasks, tc.running)
			}
			if resp.PendingTasks != tc.pending {
				t.Errorf("PendingTasks = %d, want %d", resp.PendingTasks, tc.pending)
			}
			if resp.RetryCount != tc.retries {
				t.Errorf("RetryCount = %d, want %d", resp.RetryCount, tc.retries)
			}
			if resp.OverallPercent < 0 || resp.OverallPercent > 100 {
				t.Errorf("OverallPercent = %d out of [0,100]", resp.OverallPercent)
			}
		})
	}
}
