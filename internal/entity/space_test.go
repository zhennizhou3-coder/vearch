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

package entity_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

func TestEngineSpaceString(t *testing.T) {
	space := &entity.Space{}
	str := `
	{
		"name": "ts_space",
		"partition_num": 1,
		"replica_num": 1,
		"fields": [
		  {
			"name": "string",
			"type": "string",
			"index": {
			  "name": "string",
			  "type": "SCALAR"
			}
		  },
		  {
			"name": "float",
			"type": "float",
			"index": {
			  "name": "float",
			  "type": "SCALAR"
			}
		  },
		  {
			"name": "int",
			"type": "integer",
			"index": {
			  "name": "int",
			  "type": "SCALAR"
			}
		  },
		  {
			"name": "double",
			"type": "double",
			"index": {
			  "name": "double",
			  "type": "SCALAR"
			}
		  },
		  {
			"name": "vector",
			"type": "vector",
			"dimension": 128,
			"store_type": "MemoryOnly",
			"format": "normalization",
			"index": {
			  "name": "gamma",
			  "type": "IVFPQ",
			  "params": {
				"nlinks": 16,
				"efSearch": 32,
				"efConstruction": 60,
				"metric_type": "InnerProduct",
				"ncentroids": 512,
				"nprobe": 15,
				"nsubvector": 16,
				"training_threshold": 100000
			  }
			},
			"store_param": {
			  "cache_size": 1024
			}
		  }
		]
	  }
	`
	err := json.Unmarshal([]byte(str), space)
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
}

func TestSpace_Validate(t *testing.T) {
	type fields struct {
		Id              entity.SpaceID
		Name            string
		ResourceName    string
		Version         entity.Version
		DBId            entity.DBID
		Enabled         *bool
		Partitions      []*entity.Partition
		PartitionNum    int
		ReplicaNum      uint8
		Fields          json.RawMessage
		Indexes         []*entity.Index
		SpaceProperties map[string]*entity.SpaceProperties
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Valid space name",
			fields: fields{
				Name: "validSpaceName",
			},
			wantErr: false,
		},
		{
			name: "Empty space name",
			fields: fields{
				Name: "",
			},
			wantErr: true,
		},
		{
			name: "Space name starts with number",
			fields: fields{
				Name: "1invalidSpaceName",
			},
			wantErr: true,
		},
		{
			name: "Space name starts with underscore",
			fields: fields{
				Name: "_invalidSpaceName",
			},
			wantErr: true,
		},
		{
			name: "Space name with invalid characters",
			fields: fields{
				Name: "invalid!Space@Name",
			},
			wantErr: true,
		},
		// TODO: Add more test cases as needed.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			space := &entity.Space{
				Id:              tt.fields.Id,
				Name:            tt.fields.Name,
				ResourceName:    tt.fields.ResourceName,
				Version:         tt.fields.Version,
				DBId:            tt.fields.DBId,
				Enabled:         tt.fields.Enabled,
				Partitions:      tt.fields.Partitions,
				PartitionNum:    tt.fields.PartitionNum,
				ReplicaNum:      tt.fields.ReplicaNum,
				Fields:          tt.fields.Fields,
				Indexes:         tt.fields.Indexes,
				SpaceProperties: tt.fields.SpaceProperties,
			}
			if err := space.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Space.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseIndex_TrainingThreshold(t *testing.T) {
	// Write-path validation (ValidateIndexes) requires
	// training_threshold >= max(MinTrainingThreshold(256),
	// ncentroids * DefaultMinPointsPerCentroid(39)).
	tests := []struct {
		name          string
		indexType     string
		ncentroids    int
		trainingValue int
		wantErr       bool
	}{
		{
			name:          "IVFPQ below ncentroids*39 should fail",
			indexType:     "IVFPQ",
			ncentroids:    128,
			trainingValue: 3999,
			wantErr:       true,
		},
		{
			name:          "IVFPQ exactly at ncentroids*39 should pass",
			indexType:     "IVFPQ",
			ncentroids:    128,
			trainingValue: 4992,
			wantErr:       false,
		},
		{
			name:          "small ncentroids at 256 floor should pass",
			indexType:     "IVFPQ",
			ncentroids:    1,
			trainingValue: 256,
			wantErr:       false,
		},
		{
			name:          "small ncentroids below 256 floor should fail",
			indexType:     "IVFPQ",
			ncentroids:    1,
			trainingValue: 100,
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := fmt.Sprintf(`{"metric_type":"L2","ncentroids":%d,"nsubvector":16,"training_threshold":%d}`,
				tt.ncentroids, tt.trainingValue)
			idx := &entity.Index{
				Name:      "gamma",
				Type:      tt.indexType,
				FieldName: "field_vector",
				Params:    json.RawMessage(params),
			}
			props := map[string]*entity.SpaceProperties{
				"field_vector": {FieldType: vearchpb.FieldType_VECTOR},
			}
			err := entity.ValidateIndexes([]*entity.Index{idx}, props)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateIndexes error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParseIndex_TrainingThreshold_ReloadCompat guards against H1: the
// training_threshold >= ncentroids*39 rule must NOT live in Index.UnmarshalJSON,
// because that path also runs when reloading an already-persisted space from
// etcd. A space created by an older version with training_threshold in
// [max(256, ncentroids), ncentroids*39) (e.g. ncentroids=128, tt=3999) must
// still deserialize, or it would be silently dropped from the cluster on upgrade.
func TestParseIndex_TrainingThreshold_ReloadCompat(t *testing.T) {
	raw := `{"name":"gamma","type":"IVFPQ","params":{"metric_type":"L2","ncentroids":128,"nsubvector":16,"training_threshold":3999}}`
	idx := &entity.Index{}
	if err := json.Unmarshal([]byte(raw), idx); err != nil {
		t.Fatalf("existing space with training_threshold=3999 must still unmarshal, got: %v", err)
	}
}
