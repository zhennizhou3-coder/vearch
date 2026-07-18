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

package entity

import "fmt"

/*
SpaceSignature is a space's config fingerprint.
Spaces with the same signature are treated as one class for scheduling
(data volumes can be compared directly).
*/
type SpaceSignature struct {
	IndexType    string `json:"index_type"`
	Dimension    int    `json:"dimension"`
	ResourceName string `json:"resource_name"`
}

func (sig SpaceSignature) String() string {
	return fmt.Sprintf("%s|%d|%s", sig.IndexType, sig.Dimension, sig.ResourceName)
}

// Signature extracts a space's signature.
func (s *Space) Signature() SpaceSignature {
	indexType := ""
	for _, idx := range s.Indexes {
		if idx == nil {
			continue
		}
		if !IsScalarIndexType(idx.Type) {
			indexType = idx.Type
			break
		}
	}
	dimension := 0
	for _, prop := range s.SpaceProperties {
		if prop == nil {
			continue
		}
		if prop.Dimension > 0 {
			dimension = prop.Dimension
			break
		}
	}
	resourceName := s.ResourceName
	if resourceName == "" {
		resourceName = "default"
	}
	return SpaceSignature{
		IndexType:    indexType,
		Dimension:    dimension,
		ResourceName: resourceName,
	}
}
