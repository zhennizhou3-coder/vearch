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

package raftstore

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

// Snapshot implements the raft interface.
func (s *Store) Snapshot() (proto.Snapshot, error) {
	return s.GetEngine().NewSnapshot()
}

// ApplySnapshot implements the raft interface.
func (s *Store) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) (err error) {
	// Defer the snapshot install while a long index train (rebuild / build) is
	// in flight. The install below is destructive — it Closes the engine, waits
	// for it to stop, then RemoveDataPath() before streaming in the new data.
	// Close() blocks until the train finishes (a rebuild pins the engine; a build
	// makes the destructor join the indexing thread), so during a train we would
	// stall here for the whole train, blow past raft's snapshot transport
	// timeout, and risk sitting with local data already removed. Returning an
	// error makes the follower reject this snapshot; the leader retries after a
	// heartbeat interval and re-sends a fresh snapshot, so once the train
	// completes a later attempt installs cleanly. The engine data is left intact
	// here (Close/RemoveDataPath are skipped), so the replica just stays behind
	// until then instead of losing data.
	//
	// NOTE: the raft library truncates this follower's raft log *before* calling
	// us (wal.Storage.ApplySnapshot(empty) -> TruncateAll), so a reject cannot
	// prevent the log truncation — only the far costlier engine data removal.
	// The truncated log is rebuilt from the leader's next successful snapshot.
	if s.Engine.IndexTrainInFlight() {
		log.Info("defer apply snapshot for partition[%d]: index train in flight", s.Partition.Id)
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR,
			fmt.Errorf("apply snapshot deferred: index train in flight, partition:[%d]", s.Partition.Id))
	}

	s.Engine.Close()

	log.Debug("Close engine")
	i := 0
	// wait engine close
	for {
		if s.Engine.HasClosed() {
			break
		}
		time.Sleep(1 * time.Second)
		i++
		log.Debug("Wait stop engine times:[%d]", i)
	}
	log.Debug("Engine has stop, begin remove engine data.")
	// remove engine data dir
	err = s.RemoveDataPath()
	if err != nil {
		log.Error("Remove engine data error:[%v]", err)
		return err
	}
	log.Debug("Remove engine data path")
	// apply snapshot
	err = s.GetEngine().ApplySnapshot(peers, iter)
	if err != nil {
		log.Error("Apply snapshot error:[%v]", err)
	}
	log.Debug("Store info is [%+v]", s)
	err = s.ReBuildEngine()
	if err != nil {
		log.Error("Rebuild engine error:[%v]", err)
		return err
	}
	log.Debug("Rebuild engine after store info is [%+v]", s)
	return err
}
