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
	retries := w.retries

	for {
		err := w.processRequest(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("worker shutting down...")
				return
			case <-time.After(w.pollingPeriod):
				if retries == 0 {
					sdk.Logger(ctx).Err(err).Msg("retries exhausted, worker shutting down...")
					return
				}
				retries--
			}
		} else {
			retries = w.retries
		}
	}
}

func (w *Worker) processRequest(ctx context.Context) error {
	request := &api.QueryRequest{
		Organization: w.organization,
		Bucket:       w.bucket,
		Measurement:  w.measurement,
		After:        w.lastTS,
	}

	response, err := w.client.Query(ctx, request)
	if err != nil {
		return fmt.Errorf("error client query: %w", err)
	}

	processed, err := w.handleResult(ctx, response)
	if err != nil || !processed {
		return err
	}

	return nil
}

func (w *Worker) handleResult(ctx context.Context, response *api.QueryResponse) (bool, error) {
	var processed bool
	for response.Result.Next() {
		processed = true
		w.position.update(w.measurement, response.Result.Record().Time())
		sdkPosition, err := w.position.marshal()
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("error marshal position")
			continue
		}

		key := opencdc.StructuredData{
			"measurement": response.Result.Record().Measurement(),
			w.keyField:    response.Result.Record().ValueByKey(w.keyField),
		}

		metadata, payload := influxdb.ParseRecord(response.Result.Record())
		record := sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, payload)

		w.ch <- record
		w.lastTS = response.Result.Record().Time()
	}

	return processed, fmt.Errorf("error handling response results: %w", response.Result.Err())
}
