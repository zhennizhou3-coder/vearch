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
	"os"
	"path/filepath"
	"testing"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
)

// finalizeTrainerArtifacts commits the raw bytes the engine dumped to <model>.tmp
// during RebuildIndex: sha256 → rename onto the slot → .meta sidecar (written
// after the bytes). It exercises the P6 commit machinery without cgo.
func TestFinalizeTrainerArtifacts(t *testing.T) {
	dir := t.TempDir()
	modelPath, metaPath := trainingArtifactsPathsAt(dir, "idx")
	tmpPath := modelPath + ".tmp"
	payload := []byte("centroids+codebook raw bytes")

	// Simulate the engine's in-rebuild dump landing in the .tmp slot.
	if err := os.MkdirAll(filepath.Dir(tmpPath), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(tmpPath, payload, 0644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	store := &fakeStore{path: dir}
	sha, err := r.finalizeTrainerArtifacts(store, "idx", "round-1", tmpPath)
	if err != nil {
		t.Fatalf("finalizeTrainerArtifacts: %v", err)
	}

	// Model file holds exactly the dumped bytes; the .tmp was renamed away.
	got, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("model bytes mismatch: got %q want %q", got, payload)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("tmp not cleaned up: stat err=%v", err)
	}

	// Sidecar records round, sha256 and size matching the committed file.
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	var meta entity.TrainingArtifactsMeta
	if err := vjson.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	if meta.RoundID != "round-1" {
		t.Errorf("meta round: got %q want %q", meta.RoundID, "round-1")
	}
	if meta.SHA256 != sha {
		t.Errorf("meta sha256: got %q want %q", meta.SHA256, sha)
	}
	if meta.Size != int64(len(payload)) {
		t.Errorf("meta size: got %d want %d", meta.Size, len(payload))
	}
	wantSha, _, err := sha256OfFile(modelPath)
	if err != nil {
		t.Fatalf("sha256OfFile: %v", err)
	}
	if wantSha != sha {
		t.Errorf("independent hash mismatch: %s vs %s", wantSha, sha)
	}
}

// When the engine skipped the dump (index family without separable training
// artifacts), no .tmp exists: finalize publishes nothing and does not error, so
// the round simply carries no artifacts.
func TestFinalizeTrainerArtifacts_NoDumpNoPublish(t *testing.T) {
	dir := t.TempDir()
	modelPath, metaPath := trainingArtifactsPathsAt(dir, "idx")
	tmpPath := modelPath + ".tmp" // never created

	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	store := &fakeStore{path: dir}
	sha, err := r.finalizeTrainerArtifacts(store, "idx", "round-1", tmpPath)
	if err != nil {
		t.Fatalf("finalizeTrainerArtifacts: %v", err)
	}
	if sha != "" {
		t.Errorf("sha: got %q, want empty (nothing published)", sha)
	}
	if _, err := os.Stat(modelPath); !os.IsNotExist(err) {
		t.Errorf("model must not be written: stat err=%v", err)
	}
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Errorf(".meta must not be written: stat err=%v", err)
	}
}

// trainerDumpTmpPath returns the <model>.tmp slot and clears any stale .tmp so a
// skipped dump cannot leave a previous round's file to be mistaken for this one.
func TestTrainerDumpTmpPath_ClearsStale(t *testing.T) {
	dir := t.TempDir()
	modelPath, _ := trainingArtifactsPathsAt(dir, "idx")
	staleTmp := modelPath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(staleTmp), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(staleTmp, []byte("stale prior round"), 0644); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	r := &RebuildManager{tasks: map[string]*RebuildTask{}}
	store := &fakeStore{path: dir}
	got, err := r.trainerDumpTmpPath(store, "idx")
	if err != nil {
		t.Fatalf("trainerDumpTmpPath: %v", err)
	}
	if got != staleTmp {
		t.Fatalf("tmp path: got %q want %q", got, staleTmp)
	}
	if _, err := os.Stat(staleTmp); !os.IsNotExist(err) {
		t.Fatalf("stale tmp not cleared: stat err=%v", err)
	}
}

// readTrainingArtifactsChunk must return exactly the requested window, and a short
// tail at EOF (not an error), so the follower's offset-driven pull loop
// terminates correctly regardless of how server/client chunk sizes line up.
func TestReadTrainingArtifactsChunkBoundaries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.bin")
	payload := make([]byte, 25) // 25 bytes: not a multiple of the 10-byte chunk
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := os.WriteFile(path, payload, 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cases := []struct {
		name      string
		offset    int64
		size      int
		wantLen   int
		wantFirst byte
	}{
		{"full-first-chunk", 0, 10, 10, 0},
		{"mid-chunk", 10, 10, 10, 10},
		{"short-tail", 20, 10, 5, 20}, // only 5 bytes left → partial tail
		{"exact-eof", 25, 10, 0, 0},   // at EOF → empty, no error
		{"beyond-eof", 100, 10, 0, 0}, // past EOF → empty, no error
		{"size-larger-than-file", 0, 1000, 25, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			chunk, err := readTrainingArtifactsChunk(path, c.offset, c.size)
			if err != nil {
				t.Fatalf("readTrainingArtifactsChunk: %v", err)
			}
			if len(chunk) != c.wantLen {
				t.Fatalf("len: got %d want %d", len(chunk), c.wantLen)
			}
			if c.wantLen > 0 && chunk[0] != c.wantFirst {
				t.Errorf("first byte: got %d want %d", chunk[0], c.wantFirst)
			}
		})
	}

	// Reassembling all chunks with a 10-byte window reproduces the file (the
	// exact loop the follower runs, advancing by bytes actually returned).
	var got []byte
	for off := int64(0); off < int64(len(payload)); {
		chunk, err := readTrainingArtifactsChunk(path, off, 10)
		if err != nil {
			t.Fatalf("reassemble read: %v", err)
		}
		if len(chunk) == 0 {
			t.Fatal("unexpected empty chunk before EOF")
		}
		got = append(got, chunk...)
		off += int64(len(chunk))
	}
	if string(got) != string(payload) {
		t.Errorf("reassembled bytes differ from original")
	}
}
