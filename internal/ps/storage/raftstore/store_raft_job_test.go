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
	"testing"
	"time"
)

func TestFlushRetryBackoff(t *testing.T) {
	const s = time.Second

	tests := []struct {
		name     string
		failures int
		base     time.Duration
		capDur   time.Duration
		want     time.Duration
	}{
		// Production default fti=7200s (2h). Geometric 60s x3, capping at the 6th
		// failure. NOTE: the 5th failure is 81m (4860s), NOT the cap.
		{"prod fti=7200 fail=1", 1, FlushRetryBase, 7200 * s, 60 * s},
		{"prod fti=7200 fail=2", 2, FlushRetryBase, 7200 * s, 180 * s},
		{"prod fti=7200 fail=3", 3, FlushRetryBase, 7200 * s, 540 * s},
		{"prod fti=7200 fail=4", 4, FlushRetryBase, 7200 * s, 1620 * s},
		{"prod fti=7200 fail=5", 5, FlushRetryBase, 7200 * s, 4860 * s},
		{"prod fti=7200 fail=6 (capped)", 6, FlushRetryBase, 7200 * s, 7200 * s},
		{"prod fti=7200 fail=7 (stays capped)", 7, FlushRetryBase, 7200 * s, 7200 * s},

		// Repo default fti=600s (10m). Caps at the 4th failure.
		{"repo fti=600 fail=1", 1, FlushRetryBase, 600 * s, 60 * s},
		{"repo fti=600 fail=2", 2, FlushRetryBase, 600 * s, 180 * s},
		{"repo fti=600 fail=3", 3, FlushRetryBase, 600 * s, 540 * s},
		{"repo fti=600 fail=4 (capped)", 4, FlushRetryBase, 600 * s, 600 * s},
		{"repo fti=600 fail=5 (stays capped)", 5, FlushRetryBase, 600 * s, 600 * s},

		// Large failure count never exceeds cap and never overflows to negative.
		{"huge failure count clamps to cap", 1_000_000, FlushRetryBase, 7200 * s, 7200 * s},

		// Defensive: the real caller always passes failures >= 1, but failures <= 1
		// must return exactly base (no multiplication).
		{"fail=1 returns base", 1, FlushRetryBase, 7200 * s, FlushRetryBase},
		{"fail=0 returns base (defensive)", 0, FlushRetryBase, 7200 * s, FlushRetryBase},

		// Base already at/above cap collapses to cap on the first retry.
		{"base equals cap", 1, 600 * s, 600 * s, 600 * s},
		{"base exceeds cap clamps down", 3, 600 * s, 300 * s, 300 * s},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := flushRetryBackoff(tt.failures, tt.base, tt.capDur)
			if got != tt.want {
				t.Errorf("flushRetryBackoff(%d, %s, %s) = %s, want %s",
					tt.failures, tt.base, tt.capDur, got, tt.want)
			}
		})
	}
}

// TestFlushRetryBackoffProperties asserts the invariants the flush loop relies
// on: the backoff never exceeds the cap, is always positive (a non-positive
// value would make nextRetry fire immediately, defeating the backoff), and is
// monotonically non-decreasing in the failure count.
func TestFlushRetryBackoffProperties(t *testing.T) {
	const s = time.Second
	caps := []time.Duration{60 * s, 600 * s, 7200 * s, 24 * time.Hour}

	for _, capDur := range caps {
		var prev time.Duration
		for failures := 1; failures <= 40; failures++ {
			got := flushRetryBackoff(failures, FlushRetryBase, capDur)
			if got <= 0 {
				t.Fatalf("cap=%s failures=%d: backoff must be positive, got %s", capDur, failures, got)
			}
			if got > capDur {
				t.Fatalf("cap=%s failures=%d: backoff %s exceeds cap", capDur, failures, got)
			}
			if got < prev {
				t.Fatalf("cap=%s failures=%d: backoff %s decreased from %s", capDur, failures, got, prev)
			}
			prev = got
		}
	}
}
