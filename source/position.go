// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package source

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

type Position struct {
	mu           sync.Mutex
	Measurements map[string]time.Time `json:"measurements"`
}

// NewPosition initializes a new position when sdk position is nil.
func NewPosition() *Position {
	return &Position{Measurements: make(map[string]time.Time)}
}

// ParseSDKPosition parses opencdc.Position and returns Position.
func ParseSDKPosition(position opencdc.Position) (*Position, error) {
	var pos Position

	if position == nil {
		return NewPosition(), nil
	}

	if err := json.Unmarshal(position, &pos); err != nil {
		return nil, fmt.Errorf("unmarshal opencdc.Position into Position: %w", err)
	}

	return &pos, nil
}

// marshal marshals Position and returns opencdc.Position or an error.
func (p *Position) marshal() (opencdc.Position, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}
	return positionBytes, nil
}

// update updates a measurement timestamp in the source position.
func (p *Position) update(index string, ts time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Measurements[index] = ts
}
