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

package client

import (
	"fmt"
	"testing"

	cache "github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/entity/request"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

// newTestClient builds a Client with only a psClient (no master). The routing
// selector under test uses client.PS().TestFaulty; the master is unused for the
// non-LeastConnection paths and for LeastConnection paths where every candidate
// is skipped before an RPC client is created.
func newTestClient() *Client {
	c := &Client{}
	_ = c.initPsClient()
	return c
}

func serversWith(ids ...entity.NodeID) *cache.Cache {
	c := cache.New(cache.NoExpiration, cache.NoExpiration)
	for _, id := range ids {
		c.Set(cast.ToString(id), &entity.Server{ID: id}, cache.NoExpiration)
	}
	return c
}

func setRaftConsistent(t *testing.T, v bool) {
	prev := config.Conf()
	config.SetConf(&config.Config{Global: &config.GlobalCfg{RaftConsistent: v}})
	t.Cleanup(func() { config.SetConf(prev) })
}

// A Leader-typed read cannot fall back to a follower, so a rebuild-failed leader
// must fail with PARTITION_LEADER_REBUILDING rather than serve a broken index.
func TestSelectNodeByClientType_LeaderRebuildFailed(t *testing.T) {
	SetRebuildBusyNode(0)
	setRaftConsistent(t, true)
	c := newTestClient()
	servers := serversWith(1, 2, 3)

	p := &entity.Partition{
		Id: 100, LeaderID: 1, Replicas: []entity.NodeID{1, 2, 3},
		ReStatusMap: map[uint64]uint32{
			1: entity.ReplicasRebuildFailed,
			2: entity.ReplicasOK,
			3: entity.ReplicasOK,
		},
	}
	node, err := SelectNodeByClientType(request.Leader, p, servers, c)
	if node != 0 || err == nil {
		t.Fatalf("failed leader must not be selected: got node=%d err=%v", node, err)
	}
	vErr, ok := err.(*vearchpb.VearchErr)
	if !ok || vErr.GetError().Code != vearchpb.ErrorEnum_PARTITION_LEADER_REBUILDING {
		t.Fatalf("expected PARTITION_LEADER_REBUILDING, got %v", err)
	}

	// A healthy leader is returned unchanged.
	p.ReStatusMap[1] = entity.ReplicasOK
	node, err = SelectNodeByClientType(request.Leader, p, servers, c)
	if node != 1 || err != nil {
		t.Fatalf("healthy leader: got node=%d err=%v, want node=1 nil", node, err)
	}
}

// A rebuild-failed replica is kept off reads in every selector and under both
// consistency modes. With only a failed replica present, selection yields 0
// (no eligible node) rather than routing to the broken replica.
func TestSelectNodeByClientType_FailedReplicaNeverSelected(t *testing.T) {
	clientTypes := []string{
		request.NotLeader, request.Random, "",
		request.LeastConnection, request.NearestConnection, "some_default",
	}
	for _, raftConsistent := range []bool{false, true} {
		for _, ct := range clientTypes {
			t.Run(fmt.Sprintf("ct=%q/raftConsistent=%v", ct, raftConsistent), func(t *testing.T) {
				SetRebuildBusyNode(0)
				setRaftConsistent(t, raftConsistent)
				c := newTestClient()
				servers := serversWith(7)

				p := &entity.Partition{
					Id: 200, LeaderID: 7, Replicas: []entity.NodeID{7},
					ReStatusMap: map[uint64]uint32{7: entity.ReplicasRebuildFailed},
				}
				node, err := SelectNodeByClientType(ct, p, servers, c)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if node != 0 {
					t.Fatalf("rebuild-failed replica must not be selected, got node=%d", node)
				}
			})
		}
	}
}

// Among healthy and rebuild-failed replicas, selection only ever returns a
// healthy node. LeastConnection is excluded because its scoring opens an RPC
// client to each healthy candidate, which the master-less test Client cannot do.
func TestSelectNodeByClientType_SkipsFailedAmongHealthy(t *testing.T) {
	clientTypes := []string{
		request.NotLeader, request.Random, "",
		request.NearestConnection, "some_default",
	}
	for _, raftConsistent := range []bool{false, true} {
		for _, ct := range clientTypes {
			t.Run(fmt.Sprintf("ct=%q/raftConsistent=%v", ct, raftConsistent), func(t *testing.T) {
				SetRebuildBusyNode(0)
				setRaftConsistent(t, raftConsistent)
				c := newTestClient()
				servers := serversWith(1, 2, 3)

				p := &entity.Partition{
					Id: 300, LeaderID: 1, Replicas: []entity.NodeID{1, 2, 3},
					ReStatusMap: map[uint64]uint32{
						1: entity.ReplicasOK,
						2: entity.ReplicasRebuildFailed,
						3: entity.ReplicasOK,
					},
				}
				// Round-robin cycles across candidates; sample enough times to
				// pass the failed replica's slot had it not been excluded.
				for i := 0; i < 12; i++ {
					node, err := SelectNodeByClientType(ct, p, servers, c)
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					if node == 2 {
						t.Fatalf("rebuild-failed replica 2 was selected by ct=%q", ct)
					}
					if node == 0 {
						t.Fatalf("expected a healthy replica, got 0 for ct=%q", ct)
					}
				}
			})
		}
	}
}
