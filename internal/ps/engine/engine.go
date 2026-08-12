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

package engine

import (
	"context"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
)

// Reader is the read interface to an engine's data.
type Reader interface {
	GetDoc(ctx context.Context, doc *vearchpb.Document, getByDocId bool, next bool) error

	ReadSN(ctx context.Context) (int64, error)

	DocCount(ctx context.Context) (uint64, error)

	Capacity(ctx context.Context) (int64, error)

	Search(ctx context.Context, request *vearchpb.SearchRequest, resp *vearchpb.SearchResponse) error

	Query(ctx context.Context, request *vearchpb.QueryRequest, resp *vearchpb.SearchResponse) error
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// use do by single cmd , support create update replace or delete
	Write(ctx context.Context, docCmd *vearchpb.DocCmd) error

	//this update will merge documents
	// Update(ctx context.Context, docCmd *vearchpb.DocCmd) error

	// flush memory to segment, new reader will read the newest data
	Flush(ctx context.Context, sn int64) error

	// commit is renew a memory block, return a chan to client, client get the chan to wait the old memory flush to segment
	Commit(ctx context.Context, sn int64) (chan error, error)
}

// IndexStatusInfo is the per-index status a rebuild poll needs. Named fields
// (not a positional tuple) so the two same-typed flags cannot be swapped at a
// call site without the compiler noticing.
type IndexStatusInfo struct {
	// Status is the engine's stringified IndexStatus:
	// "UNINDEXED"/"INDEXING"/"INDEXED"/"FAILED".
	Status string
	// IndexedNum is vectors actually added to this index so far.
	IndexedNum int
	// IsTrained / SupportIncrement classify whether the index backfills after a
	// swap. Both default to true on engines predating these flags (the safe
	// "will backfill, wait for catch-up" classification).
	IsTrained        bool
	SupportIncrement bool
	// MaxDocid is the engine-wide doc frontier (max_docid), not a per-index
	// value — it is identical for every index in the partition. It is carried
	// here so a single GetEngineStatus read serves the whole poll: the monitor
	// needs both this index's fields and the frontier it gates completion on.
	MaxDocid int
}

// Engine is the interface that wraps the core operations of a document store.
type Engine interface {
	Reader() Reader
	Writer() Writer
	//return three value, field to vearchpb.Field , new Schema info , error
	NewSnapshot() (proto.Snapshot, error)
	ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error
	Optimize() error

	// RebuildIndex rebuilds the index identified by indexName. `field`
	// and `indexType` are used engine-side to resolve the RawVector and
	// index parameters.
	RebuildIndex(indexName, field, indexType string, dropBefore, limitCPU, describe int) error
	Load() error
	IndexInfo() (int, int, int)

	// IndexStatusOf returns the per-index status (status string, indexed count,
	// the isTrained / supportIncrement classification flags, and the engine-wide
	// MaxDocid) of the vector index whose physical name (field::type) matches
	// indexName. MaxDocid comes from the same EngineStatus read, so one call
	// serves a whole rebuild poll.
	IndexStatusOf(indexName string) (IndexStatusInfo, error)
	GetEngineStatus(status *entity.EngineStatus) error
	Close()
	HasClosed() bool

	UpdateMapping(space *entity.Space) error
	GetMapping() *mapping.IndexMapping

	// Apply an explicit index change delivered by a raft INDEXCHANGE command.
	// AddIndexes adds each index (scalar/composite/vector); RemoveIndex drops
	// one by name. These replace the old "diff the whole space in UpdateMapping"
	// path — the caller already knows the exact operation.
	AddIndexes(indexes []*entity.Index) error
	RemoveIndex(indexName string) error

	GetSpace() *entity.Space
	GetPartitionID() entity.PartitionID

	SetEngineCfg(configJson []byte) error
	GetEngineCfg(config *entity.SpaceConfig) error

	BackupSpace(command string) error
}
