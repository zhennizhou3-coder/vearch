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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/ps/engine"
)

// ---------------------------------------------------------------------------
// Pure decision functions
// ---------------------------------------------------------------------------

func TestComputeProgress(t *testing.T) {
	cases := []struct {
		name       string
		indexedNum int
		maxDocid   int
		want       int
	}{
		{"unknown-total", 10, 0, 0},
		{"negative-total", 10, -1, 0},
		{"half", 50, 100, 50},
		{"full", 100, 100, 100},
		{"over-clamps-to-100", 120, 100, 100},
		{"zero-indexed", 0, 100, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := computeProgress(c.indexedNum, c.maxDocid); got != c.want {
				t.Fatalf("computeProgress(%d,%d)=%d, want %d",
					c.indexedNum, c.maxDocid, got, c.want)
			}
		})
	}
}

func TestObserveRebuildStatus(t *testing.T) {
	cases := []struct {
		name            string
		awaitTransition bool
		status          string
		want            bool
	}{
		// Not awaiting: any reading counts, never awaiting again.
		{"not-awaiting-indexed", false, indexStatusIndexed, false},
		{"not-awaiting-unindexed", false, indexStatusUnindexed, false},
		// Awaiting: a non-terminal reading means this rebuild started -> stop awaiting.
		{"awaiting-sees-indexing", true, indexStatusIndexing, false},
		{"awaiting-sees-unindexed", true, indexStatusUnindexed, false},
		// Awaiting: a stale terminal reading from the prior build -> keep awaiting.
		{"awaiting-sees-stale-indexed", true, indexStatusIndexed, true},
		{"awaiting-sees-stale-failed", true, indexStatusFailed, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := observeRebuildStatus(c.awaitTransition, c.status); got != c.want {
				t.Fatalf("observeRebuildStatus(%v,%q)=%v, want %v",
					c.awaitTransition, c.status, got, c.want)
			}
		})
	}
}

func TestShouldDispatchBuild(t *testing.T) {
	cases := []struct {
		name             string
		preStatus        string
		supportIncrement bool
		want             bool // true = dispatch a fresh build; false = monitor only
	}{
		// INDEXING: a build is genuinely mid-flight -> monitor only, regardless of kind.
		{"indexing-incremental", indexStatusIndexing, true, false},
		{"indexing-nonincremental", indexStatusIndexing, false, false},
		// UNINDEXED incremental: may be a background Load build (or a settled
		// untrained index) -> monitor only, do not dispatch.
		{"unindexed-incremental", indexStatusUnindexed, true, false},
		// UNINDEXED non-incremental (DISKANN): no background build exists ->
		// must dispatch. This is the L1 fix.
		{"unindexed-nonincremental-dispatches", indexStatusUnindexed, false, true},
		// Terminal states: dispatch a fresh build.
		{"indexed-incremental", indexStatusIndexed, true, true},
		{"indexed-nonincremental", indexStatusIndexed, false, true},
		{"failed-incremental", indexStatusFailed, true, true},
		{"failed-nonincremental", indexStatusFailed, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := shouldDispatchBuild(c.preStatus, c.supportIncrement); got != c.want {
				t.Fatalf("shouldDispatchBuild(%q, support=%v)=%v, want %v",
					c.preStatus, c.supportIncrement, got, c.want)
			}
		})
	}
}

func TestReachedIndexed(t *testing.T) {
	trained := engine.IndexStatusInfo{Status: indexStatusIndexed, IsTrained: true, SupportIncrement: true}
	building := engine.IndexStatusInfo{Status: indexStatusIndexing, IsTrained: true, SupportIncrement: true}
	untrained := engine.IndexStatusInfo{Status: indexStatusUnindexed, IsTrained: false, SupportIncrement: true}
	// Non-incremental (DISKANN) after Load reconciles it to INDEXED: settles via
	// the plain INDEXED gate, no longer via a support_increment short-circuit.
	nonIncr := engine.IndexStatusInfo{Status: indexStatusIndexed, IsTrained: true, SupportIncrement: false}
	// An IVF actively training on the monitor-only path: status INDEXING while
	// faiss is_trained is still transiently false. Must NOT settle — the classifier
	// would otherwise complete a rebuild whose training has not finished.
	trainingIVF := engine.IndexStatusInfo{Status: indexStatusIndexing, IsTrained: false, SupportIncrement: true}

	cases := []struct {
		name            string
		awaitTransition bool
		info            engine.IndexStatusInfo
		engineReturned  bool
		want            bool
	}{
		// Dispatch path: synchronous return is authoritative regardless of state.
		{"dispatch-return-wins-over-building", true, building, true, true},
		// Monitor-only path, not awaiting: INDEXED settles.
		{"monitoronly-indexed-settles", false, trained, false, true},
		// Monitor-only path: still building -> not settled.
		{"monitoronly-building-not-settled", false, building, false, false},
		// Monitor-only path: untrained index rests at UNINDEXED -> settled (terminal).
		{"monitoronly-untrained-settles", false, untrained, false, true},
		// Monitor-only path: non-incremental index reconciled to INDEXED -> settled.
		{"monitoronly-nonincr-settles", false, nonIncr, false, true},
		// Monitor-only path: an IVF mid-training (INDEXING + is_trained=false) must NOT
		// be mistaken for a terminal untrained index. Regression guard for the
		// window where the training flag is transiently false.
		{"monitoronly-training-ivf-not-settled", false, trainingIVF, false, false},
		// Monitor-only path: awaiting a transition away from a stale INDEXED -> not settled.
		{"monitoronly-awaiting-stale-indexed", true, trained, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &RebuildManager{tasks: map[string]*RebuildTask{}}
			task := &RebuildTask{AwaitTransition: c.awaitTransition}
			if got := r.reachedIndexed(task, c.info, c.engineReturned); got != c.want {
				t.Fatalf("reachedIndexed(await=%v, info=%+v, engineReturned=%v)=%v, want %v",
					c.awaitTransition, c.info, c.engineReturned, got, c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Fakes for monitorRebuild integration.
//
// Both interfaces are large; embed them so only the handful of methods the
// monitor actually calls are implemented. Any unimplemented method panics if
// called — which flags an unexpected dependency rather than silently passing.
// ---------------------------------------------------------------------------

type fakeEngine struct {
	engine.Engine // embedded: unimplemented methods panic if the monitor calls them

	mu sync.Mutex
	// info drives IndexStatusOf; maxDoc is the engine-wide frontier, surfaced
	// through info.MaxDocid on each poll. onPoll is a hook to advance state
	// across polls (nil = static).
	info   engine.IndexStatusInfo
	maxDoc int
	closed bool
	// onPoll, if set, is invoked before each IndexStatusOf read so a test can
	// evolve indexed_num / status over successive polls.
	onPoll func(*fakeEngine)
}

func (f *fakeEngine) HasClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeEngine) IndexStatusOf(indexName string) (engine.IndexStatusInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onPoll != nil {
		f.onPoll(f)
	}
	info := f.info
	info.MaxDocid = f.maxDoc // engine-wide frontier, filled from the same read
	return info, nil
}

type fakeStore struct {
	PartitionStore // embedded: only GetEngine + GetPartition are used by the monitor
	eng            engine.Engine
	path           string             // partition data path; where training-artifacts files land
	id             entity.PartitionID // partition id; 0 defaults to 1
}

func (s *fakeStore) GetEngine() engine.Engine { return s.eng }

func (s *fakeStore) GetPartition() *entity.Partition {
	id := s.id
	if id == 0 {
		id = 1
	}
	return &entity.Partition{Id: id, Path: s.path}
}

// newFieldTask returns a field-level rebuild task (FieldName != "" so pollStatus
// takes the per-index IndexStatusOf branch).
func newFieldTask() *RebuildTask {
	return &RebuildTask{
		PartitionID: 1,
		DBName:      "db",
		SpaceName:   "sp",
		IndexName:   "idx",
		FieldName:   "vec",
		IndexType:   "IVFPQ",
		Status:      entity.RebuildStatusRunning,
	}
}

// runMonitor runs monitorRebuild to completion (it always terminates via
// mark*), guarded by a timeout so a hang fails the test instead of blocking.
func runMonitor(t *testing.T, r *RebuildManager, task *RebuildTask,
	store PartitionStore, doneCh <-chan error) {
	t.Helper()
	fin := make(chan struct{})
	go func() {
		r.monitorRebuild(task, store, doneCh)
		close(fin)
	}()
	select {
	case <-fin:
	case <-time.After(10 * time.Second):
		t.Fatal("monitorRebuild did not terminate within 10s")
	}
}

func doneChNil(err error) <-chan error {
	ch := make(chan error, 1)
	ch <- err
	return ch
}

// ---------------------------------------------------------------------------
// monitorRebuild integration
// ---------------------------------------------------------------------------

// Dispatch path, trained+incremental: completes only once indexed_num reaches
// the frozen frontier (maxDocid+1), not at the engine's synchronous return.
func TestMonitorRebuild_TrainedIncremental_WaitsForBackfill(t *testing.T) {
	polls := 0
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusIndexed, IndexedNum: 0, IsTrained: true, SupportIncrement: true},
		maxDoc: 99, // frontier = 100
		onPoll: func(f *fakeEngine) {
			polls++
			// Backfill climbs; reaches target (100) on the 3rd poll.
			f.info.IndexedNum = polls * 40
		},
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
	if polls < 3 {
		t.Fatalf("expected >=3 polls to catch up, got %d", polls)
	}
}

// Non-incremental (DISKANN): terminal at swap, completes immediately even though
// indexed_num never reaches the frontier.
func TestMonitorRebuild_NonIncremental_CompletesImmediately(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusIndexed, IndexedNum: 5, IsTrained: true, SupportIncrement: false},
		maxDoc: 9999, // frontier far above indexed_num; a "wait for catch-up" would hang
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
}

// Empty non-incremental partition (post-M2): with no live data the engine leaves
// do_train=false, so RebuildVectorIndex swaps in a fresh, unbuilt DISKANN index —
// status UNINDEXED, IsTrained=false (disk_index_ready_ never set), SupportIncrement
// =false. engine.RebuildIndex still returns nil (clean no-op), so the dispatch path
// settles via engineReturned and phase 2 completes on !SupportIncrement. Must NOT
// hang waiting for a backfill that will never come. Regression guard for the M2 fix
// (empty-partition rebuild completes instead of FAILED / retry-storming).
func TestMonitorRebuild_EmptyNonIncremental_Completes(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusUnindexed, IndexedNum: 0, IsTrained: false, SupportIncrement: false},
		maxDoc: -1, // empty partition: frontier = 0
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
}

// Untrained IVF: rests at UNINDEXED with indexed_num=0, completes (terminal) —
// a blind wait-for-catch-up would hang forever.
func TestMonitorRebuild_Untrained_Completes(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusUnindexed, IndexedNum: 0, IsTrained: false, SupportIncrement: true},
		maxDoc: 50,
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
}

// Monitor-only path (no engine dispatch, doneCh=nil): an IVF that is still training
// must NOT complete while it reports INDEXING with a transiently-false
// is_trained. It completes only once it transitions to INDEXED and the backfill
// catches up. Regression guard for H1: before the fix, reachedIndexed treated a
// false is_trained as "settled" regardless of INDEXING, completing a rebuild
// whose training had not finished.
func TestMonitorRebuild_MonitorOnly_TrainingIVF_WaitsForIndexed(t *testing.T) {
	polls := 0
	eng := &fakeEngine{
		// Starts mid-training: INDEXING + is_trained=false.
		info:   engine.IndexStatusInfo{Status: indexStatusIndexing, IndexedNum: 0, IsTrained: false, SupportIncrement: true},
		maxDoc: 9, // frontier = 10
		onPoll: func(f *fakeEngine) {
			polls++
			// Training finishes on the 3rd poll: flip to INDEXED and backfill past
			// the frozen frontier in the same step.
			if polls >= 3 {
				f.info.Status = indexStatusIndexed
				f.info.IsTrained = true
				f.info.IndexedNum = 100
			}
		},
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	// Monitor-only entry: AwaitTransition=false (a non-terminal preStatus was
	// observed), doneCh=nil (no engine dispatch to wait on).
	task.AwaitTransition = false
	runMonitor(t, r, task, &fakeStore{eng: eng}, nil)

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
	if polls < 3 {
		t.Fatalf("completed before training finished (polls=%d); must wait past INDEXING", polls)
	}
}

// Monitor-only path: a reconciled non-incremental index (DISKANN) that Load published
// as INDEXED completes without hanging on backfill (indexed_num never reaches
// the frontier). Mirrors the post-fix restart state: status is INDEXED (not the
// pre-fix UNINDEXED), so it settles via the plain INDEXED gate.
func TestMonitorRebuild_MonitorOnly_NonIncrementalIndexed_Completes(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusIndexed, IndexedNum: 5, IsTrained: true, SupportIncrement: false},
		maxDoc: 9999, // frontier far above indexed_num; wait-for-catch-up would hang
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	task.AwaitTransition = false
	runMonitor(t, r, task, &fakeStore{eng: eng}, nil)

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
}

// Empty partition: maxDocid=-1 -> frontier=0, indexed_num(0) >= 0 -> completes
// on the first post-swap poll.
func TestMonitorRebuild_EmptyPartition_Completes(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusIndexed, IndexedNum: 0, IsTrained: true, SupportIncrement: true},
		maxDoc: -1,
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusCompleted {
		t.Fatalf("status=%q, want completed", task.Status)
	}
}

// Engine reports FAILED -> task fails.
func TestMonitorRebuild_EngineFailed_MarksFailed(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusFailed, IsTrained: true, SupportIncrement: true},
		maxDoc: 100,
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusFailed {
		t.Fatalf("status=%q, want failed", task.Status)
	}
}

// Engine.RebuildIndex returns an error on doneCh -> task fails.
func TestMonitorRebuild_DoneChError_MarksFailed(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusIndexed, IsTrained: true, SupportIncrement: true},
		maxDoc: 100,
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng},
		doneChNil(errors.New("fake rebuild error")))

	if task.Status != entity.RebuildStatusFailed {
		t.Fatalf("status=%q, want failed", task.Status)
	}
}

// Engine closed mid-rebuild -> task fails.
func TestMonitorRebuild_EngineClosed_MarksFailed(t *testing.T) {
	eng := &fakeEngine{
		info:   engine.IndexStatusInfo{Status: indexStatusIndexed, IsTrained: true, SupportIncrement: true},
		maxDoc: 100,
		closed: true,
	}
	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	task := newFieldTask()
	runMonitor(t, r, task, &fakeStore{eng: eng}, doneChNil(nil))

	if task.Status != entity.RebuildStatusFailed {
		t.Fatalf("status=%q, want failed", task.Status)
	}
}
