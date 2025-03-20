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
	"encoding/json"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-influxdb/pkg/influxdb"
	"github.com/conduitio-labs/conduit-connector-influxdb/pkg/influxdb/api"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Worker struct {
	client        influxdb.Client
	organization  string
	bucket        string
	measurement   string
	lastTS        time.Time
	keyField      string
	init          bool
	pollingPeriod time.Duration
	wg            *sync.WaitGroup
	ch            chan opencdc.Record
	position      *Position
	retries       int
}

// NewWorker create a new worker goroutine and starts polling elasticsearch for new records.
func NewWorker(
	ctx context.Context,
	client influxdb.Client,
	organization string,
	bucket string,
	measurement string,
	lastTS time.Time,
	keyField string,
	init bool,
	pollingPeriod time.Duration,
	wg *sync.WaitGroup,
	ch chan opencdc.Record,
	position *Position,
	retries int,
) {
	worker := &Worker{
		client:        client,
		organization:  organization,
		bucket:        bucket,
		measurement:   measurement,
		lastTS:        lastTS,
		keyField:      keyField,
		init:          init,
		pollingPeriod: pollingPeriod,
		wg:            wg,
		ch:            ch,
		position:      position,
		retries:       retries,
	}

	go worker.start(ctx)
}

func (w *Worker) start(ctx context.Context) {
	defer w.wg.Done()
	for {
		request := &api.QueryRequest{
			Organization: w.organization,
			Bucket:       w.bucket,
			Measurement:  w.measurement,
			After:        w.lastTS,
		}

		result, err := w.client.Query(ctx, request)
		if err != nil || result == nil {
			select {
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("worker shutting down...")
				return

			case <-time.After(w.pollingPeriod):
				if err != nil {
					sdk.Logger(ctx).Err(err).Msg("error api call, retrying...")
				} else {
					sdk.Logger(ctx).Debug().Msg("no records found, continuing polling...")
				}
				continue
			}
		}

		for result.Next() {
			w.position.update(w.measurement, result.Record().Time())
			sdkPosition, err := w.position.marshal()
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("error marshal position")
				continue
			}

			metadata := opencdc.Metadata{
				opencdc.MetadataCollection: result.Record().Measurement(),
				"timestamp":                result.Record().Time().String(),
			}
			metadata.SetCreatedAt(result.Record().Time())

			key := make(opencdc.StructuredData)
			key[w.keyField] = result.Record().ValueByKey(w.keyField)

			payload, err := json.Marshal(result.Record().Values())
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("error marshal payload")
				continue
			}

			record := sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, opencdc.RawData(payload))

			select {
			case w.ch <- record:
				w.lastTS = result.Record().Time()
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("worker shutting down...")
				return
			}
		}
	}
}
