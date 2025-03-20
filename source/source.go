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
	"context"
	"fmt"
	"sync"

	"github.com/conduitio-labs/conduit-connector-influxdb/pkg/influxdb"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	ErrSourceClosed = fmt.Errorf("error source not opened for reading")
	ErrReadingData  = fmt.Errorf("error reading data")
)

type Source struct {
	sdk.UnimplementedSource

	config   Config
	position *Position
	client   influxdb.Client
	ch       chan opencdc.Record
	wg       *sync.WaitGroup
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an InfluxDB source...")

	var err error
	s.position, err = ParseSDKPosition(position)
	if err != nil {
		return err
	}

	s.client, err = influxdb.NewClient(s.config.URL, s.config.Token)
	if err != nil {
		return fmt.Errorf("error creating new client: %w", err)
	}

	s.ch = make(chan opencdc.Record, 1)
	s.wg = &sync.WaitGroup{}

	for m, keyField := range s.config.Measurements {
		s.wg.Add(1)
		var init bool
		lastTS, ok := s.position.Measurements[m]
		if !ok {
			// read from scratch
			init = true
		}

		// a new worker for a new index
		NewWorker(ctx, s.client, s.config.Org, s.config.Bucket, m, lastTS, keyField, init, s.config.PollingPeriod, s.wg, s.ch, s.position, s.config.Retries)
	}

	return nil
}

func (s *Source) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if s == nil || s.ch == nil {
		return []opencdc.Record{}, ErrSourceClosed
	}

	records := make([]opencdc.Record, 0, n)
	for len(records) < n {
		select {
		case <-ctx.Done():
			return []opencdc.Record{}, ctx.Err()
		case record, ok := <-s.ch:
			if !ok {
				if len(records) == 0 {
					return []opencdc.Record{}, ErrReadingData
				}
				return records, nil
			}
			records = append(records, record)
		}
	}

	return records, nil
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	sdk.Logger(ctx).Trace().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the InfluxDB Source")
	// wait for goroutines to finish
	s.wg.Wait()
	// close the read channel for write
	close(s.ch)
	// reset read channel to nil, to avoid reading buffered records
	s.ch = nil
	return nil
}
