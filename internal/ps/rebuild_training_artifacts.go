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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/fileutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
)

// writeTrainingArtifactsMeta atomically writes the `.meta` sidecar (the "ready for
// this round" commit marker). Written AFTER the model bytes so a puller never
// sees a marker without matching bytes. Shared by the trainer (dump) and a
// follower, which persists the model so it can serve later followers.
func writeTrainingArtifactsMeta(metaPath, roundID, sha256Hex string, size int64) error {
	metaBytes, err := vjson.Marshal(&entity.TrainingArtifactsMeta{
		RoundID: roundID,
		SHA256:  sha256Hex,
		Size:    size,
	})
	if err != nil {
		return err
	}
	return fileutil.WriteFileAtomic(metaPath, metaBytes, 0644)
}

// sha256OfFile streams the file through sha256, returning the hex digest and size.
func sha256OfFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// trainingArtifactsPathsAt returns the fixed training-artifacts file and its `.meta`
// sidecar under a partition's data path:
// <dataPath>/rebuild_training_artifacts/<index>.training_artifacts[.meta].
func trainingArtifactsPathsAt(dataPath, indexName string) (modelPath, metaPath string) {
	modelPath = filepath.Join(dataPath, "rebuild_training_artifacts", indexName+".training_artifacts")
	return modelPath, modelPath + ".meta"
}

// trainerDumpTmpPath returns the <model>.tmp path the engine writes the trainer's
// raw training artifacts to during RebuildIndex, clearing any stale .tmp left by a
// crashed prior round. Returning a non-empty path here (plus RebuildIndex writing
// to it) is what selects the trainer dump behavior in the engine; the commit
// (sha256/rename/.meta) happens afterward in finalizeTrainerArtifacts.
func (r *RebuildManager) trainerDumpTmpPath(store PartitionStore, indexName string) (string, error) {
	p := store.GetPartition()
	modelPath, _ := trainingArtifactsPathsAt(p.Path, indexName)
	if err := os.MkdirAll(filepath.Dir(modelPath), 0755); err != nil {
		return "", err
	}
	tmpModel := modelPath + ".tmp"
	// A leftover .tmp would otherwise be mistaken for this round's artifacts if
	// the engine skips the dump (index family with no separable artifacts).
	os.Remove(tmpModel)
	return tmpModel, nil
}

// finalizeTrainerArtifacts publishes the model before its atomic metadata marker.
// A missing temporary file means the index has no artifacts and is not an error.
func (r *RebuildManager) finalizeTrainerArtifacts(store PartitionStore, indexName, roundID, tmpPath string) (string, error) {
	p := store.GetPartition()
	modelPath, metaPath := trainingArtifactsPathsAt(p.Path, indexName)
	if _, statErr := os.Stat(tmpPath); os.IsNotExist(statErr) {
		log.Info("no training artifacts dumped: pid=%d index=%s round=%s (index has no separable artifacts), skip publish",
			p.Id, indexName, roundID)
		return "", nil
	}
	sha, size, err := sha256OfFile(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return "", err
	}
	if err := os.Rename(tmpPath, modelPath); err != nil {
		os.Remove(tmpPath)
		return "", err
	}
	if err := writeTrainingArtifactsMeta(metaPath, roundID, sha, size); err != nil {
		return "", err
	}
	log.Info("committed training artifacts: pid=%d index=%s round=%s sha256=%s size=%d -> %s",
		p.Id, indexName, roundID, sha, size, modelPath)
	return sha, nil
}

// pullAndInstallTrainingArtifacts pulls this round's model from sourceAddr into the
// partition's fixed slot and writes the .meta sidecar, so this replica both
// injects the model (returned path, fed to RebuildIndex) AND becomes a valid
// pull source for later followers. The files are kept.
func (r *RebuildManager) pullAndInstallTrainingArtifacts(sourceAddr string, store PartitionStore, indexName, roundID string) (string, error) {
	p := store.GetPartition()
	modelPath, metaPath := trainingArtifactsPathsAt(p.Path, indexName)
	meta, err := client.PullTrainingArtifacts(sourceAddr, p.Id, indexName, roundID, modelPath)
	if err != nil {
		return "", err
	}
	// Write the .meta marker AFTER the model bytes, mirroring the trainer, so
	// this replica is now a valid source.
	if err := writeTrainingArtifactsMeta(metaPath, roundID, meta.SHA256, meta.Size); err != nil {
		return "", err
	}
	return modelPath, nil
}
