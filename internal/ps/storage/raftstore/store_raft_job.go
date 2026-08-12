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
	"runtime/debug"
	"time"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

const (
	TruncateTicket             = 5 * time.Minute
	FlushTicket                = 1 * time.Second
	FlushRetryBase             = 60 * time.Second
	FlushBackoffFactor         = 3
	MaxFlushBackoffMultiplies  = 30
	DefaultFlushTimeInterval   = 600 // 10 minutes
	DefaultFlushCountThreshold = 200000
)

var fti int32 // flush time interval
var fct int32 // flush count threshold

// truncate is raft log truncate.
func (s *Store) startTruncateJob(initLastFlushIndex int64) {
	truncateFunc := func(truncIndex int64) error {
		// get raft peers status
		snapPeers := s.RaftServer.GetPendingReplica(uint64(s.Partition.Id))
		if len(snapPeers) > 0 {
			return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("peer %v is snapShot", snapPeers))
		}
		s.RaftServer.Truncate(uint64(s.Partition.Id), uint64(truncIndex))
		return nil
	}

	if config.Conf().PS.RaftTruncateCount <= 0 {
		config.Conf().PS.RaftTruncateCount = 100000
	}

	log.Info("start truncate job for partition[%d]! truncate count: %d, initLastFlushIndex: %d", s.Partition.Id, config.Conf().PS.RaftTruncateCount, initLastFlushIndex)
	appTruncateIndex := initLastFlushIndex
	go func() {
		defer func() {
			if i := recover(); i != nil {
				log.Error(string(debug.Stack()))
				log.Error(cast.ToString(i))
			}
		}()
		for {
			time.Sleep(TruncateTicket)
			select {
			case <-s.Ctx.Done():
				return
			default:
			}
			// counts condition
			if s.Engine == nil {
				log.Error("store is empty so stop truncate job, dbID:[%d] space:[%d,%s] partitionID:[%d]", s.Space.DBId, s.Space.Id, s.Space.Name, s.Partition.Id)
				return
			}

			flushSn, err := s.Engine.Reader().ReadSN(s.Ctx)
			if err != nil {
				log.Error("truncate for partition[%d] get sn: %s", s.Partition.Id, err.Error())
				continue
			}
			if (flushSn - appTruncateIndex - config.Conf().PS.RaftTruncateCount) > 0 {
				newTrucIndex := flushSn - config.Conf().PS.RaftTruncateCount
				if err = truncateFunc(newTrucIndex); err != nil {
					log.Warn("truncate partition[%d]: %s", s.Partition.Id, err.Error())
					continue
				}
				log.Info("truncate for partition[%d] raft success! current sn: %d, last sn:%d", s.Partition.Id, newTrucIndex, appTruncateIndex)
				appTruncateIndex = newTrucIndex
				continue
			}
		}
	}()
}

// flushRetryBackoff returns how long to wait before the next flush retry after
// `failures` consecutive failures (failures >= 1). It grows geometrically from
// base by FlushBackoffFactor and is clamped to capDur — the operator-configured
// flush interval, past which retrying slower buys nothing. Fast growth is
// intentional: a flush failure is almost never transient and every retry holds
// the engine write lock for a full gamma.Dump.
func flushRetryBackoff(failures int, base, capDur time.Duration) time.Duration {
	backoff := base
	// backoff < capDur is the real overflow guard (capDur derives from an int32
	// fti, so backoff stays well under MaxInt64); MaxFlushBackoffMultiplies is a
	// defensive loop bound that never actually binds.
	for i := 0; i < failures-1 && i < MaxFlushBackoffMultiplies && backoff < capDur; i++ {
		backoff *= FlushBackoffFactor
	}
	if backoff > capDur {
		backoff = capDur
	}
	return backoff
}

// start flush job
func (s *Store) startFlushJob() {
	go func() {
		defer func() {
			if i := recover(); i != nil {
				log.Error(string(debug.Stack()))
				log.Error(cast.ToString(i))
			}
		}()

		fti = int32(config.Conf().PS.FlushTimeInterval)
		if fti <= 0 {
			fti = DefaultFlushTimeInterval
		}
		fct = int32(config.Conf().PS.FlushCountThreshold)
		if fct <= 0 {
			fct = DefaultFlushCountThreshold
		}

		// init last min indexed num and doc num
		var engineStatus entity.EngineStatus
		s.Engine.GetEngineStatus(&engineStatus)
		lastIndexNum := engineStatus.MinIndexedNum
		lastMaxDocid := engineStatus.MaxDocid
		lastCheckTime := s.LastFlushTime
		// consecutive flush failures, drives the retry backoff below
		flushFailures := 0
		// zero value means no backoff in effect
		var nextRetry time.Time

		log.Info("start flush job for partition[%d], flush time interval=%d, count threshold=%d, min index num=%d, max docid=%d", s.Partition.Id, fti, fct, lastIndexNum, lastMaxDocid)
		flushFunc := func() {
			if s.Sn == 0 {
				return
			}
			// counts condition
			if s.Engine == nil {
				log.Error("store is empty so stop flush job, dbID:[%d] space:[%d,%s] partitionID:[%d]",
					s.Space.DBId, s.Space.Id, s.Space.Name, s.Partition.Id)
				return
			}

			t := time.Now()
			// after a failed flush, stay quiet until the backoff window elapses so
			// we don't even issue the per-tick GetEngineStatus cgo call, let alone
			// hammer the engine write lock with a Flush, every FlushTicket.
			if t.Before(nextRetry) {
				return
			}
			var status entity.EngineStatus
			s.Engine.GetEngineStatus(&status)
			tempSn := s.Sn
			if t.Sub(s.LastFlushTime).Seconds() > float64(fti) && (tempSn-s.LastFlushSn > int64(fct) || status.MinIndexedNum-lastIndexNum > fct || status.MaxDocid-lastMaxDocid > fct) {
				log.Info("begin to flush for partition[%d], current time: %s, sn: %d, min indexed num=%d, max docid=%d",
					s.Partition.Id, t.Format(time.RFC3339), tempSn, status.MinIndexedNum, status.MaxDocid)
				if err := s.Engine.Writer().Flush(s.Ctx, tempSn); err != nil {
					flushFailures++
					backoff := flushRetryBackoff(flushFailures, FlushRetryBase, time.Duration(fti)*time.Second)
					nextRetry = t.Add(backoff)
					log.Error("flush partition[%d] failed (attempt %d), next retry after %s: %v",
						s.Partition.Id, flushFailures, backoff, err.Error())
					return
				}
				flushFailures = 0
				nextRetry = time.Time{}
				s.LastFlushSn = tempSn
				s.LastFlushTime = t
				lastIndexNum = status.MinIndexedNum
				lastMaxDocid = status.MaxDocid
				lastCheckTime = t
			} else {
				if t.Sub(lastCheckTime).Seconds() > float64(fti) {
					log.Debug("skip flush for partition[%d], last flush time: %s, sn: %d, LastFlushSn: %d, min indexed num=%d, max docid=%d",
						s.Partition.Id, s.LastFlushTime.Format(time.RFC3339), tempSn, s.LastFlushSn, status.MinIndexedNum, status.MaxDocid)
					lastCheckTime = t
				}
			}
		}

		ticker := time.NewTicker(FlushTicket)
		for {
			select {
			case <-s.Ctx.Done():
				return
			case <-ticker.C:
				flushFunc()
			}
		}
	}()
}
