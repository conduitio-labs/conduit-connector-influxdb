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

package destination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-influxdb/pkg/influxdb"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

const (
	prefixTag         = "tag."
	metadataTimestamp = "timestamp"
)

type measurementFn func(opencdc.Record) (string, error)

type Destination struct {
	sdk.UnimplementedDestination

	config   Config
	client   influxdb.Client
	writeAPI api.WriteAPIBlocking
	// Function to dynamically get measurement name for each data point.
	measurementFunc measurementFn
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(_ context.Context) error {
	measurementFn, err := d.config.measurementFunction()
	if err != nil {
		return fmt.Errorf("invalid table name or table function: %w", err)
	}
	d.measurementFunc = measurementFn

	d.client, err = influxdb.NewClient(d.config.URL, d.config.Token)
	if err != nil {
		return fmt.Errorf("error creating new client: %w", err)
	}
	d.writeAPI = d.client.WriteAPI(d.config.Org, d.config.Bucket)

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	points := []*write.Point{}

	for _, record := range records {
		measurement, err := d.measurementFunc(record)
		if err != nil {
			return 0, err
		}
		tags := extractTags(record.Metadata)

		fields, err := structurizeData(record.Payload.After)
		if err != nil {
			return 0, fmt.Errorf("failed to convert payload to fields: %w", err)
		}

		timestamp, err := extractTimestamp(record.Metadata)
		if err != nil {
			return 0, fmt.Errorf("failed to get timestamp: %w", err)
		}

		point := influxdb2.NewPoint(measurement, tags, fields, timestamp)
		points = append(points, point)
	}

	err := d.writeAPI.WritePoint(ctx, points...)
	if err != nil {
		return 0, fmt.Errorf("failed to write records: %w", err)
	}

	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the InfluxDB Destination")

	if d.client == nil {
		return nil
	}

	if err := d.writeAPI.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush remaining data: %w", err)
	}
	d.client.Close()

	return nil
}

// structurizeData converts opencdc.Data to opencdc.StructuredData.
func structurizeData(data opencdc.Data) (map[string]interface{}, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, errors.New("empty payload")
	}

	var structuredData map[string]interface{}
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	return structuredData, nil
}

// extractTags gets tags (if any) from metadata. Valid tags start with 'tag.'.
func extractTags(metadata opencdc.Metadata) map[string]string {
	tags := make(map[string]string)
	for k, v := range metadata {
		if strings.HasPrefix(k, prefixTag) {
			newKey := strings.TrimPrefix(k, prefixTag)
			tags[newKey] = v
		}
	}
	return tags
}

func extractTimestamp(metadata opencdc.Metadata) (time.Time, error) {
	if tsStr, ok := metadata[metadataTimestamp]; ok {
		// Try parsing as RFC3339
		parsedTime, err := time.Parse(time.RFC3339, tsStr)
		if err == nil {
			return parsedTime, nil
		}

		// Try parsing as Unix timestamp
		unixTime, err := strconv.ParseFloat(tsStr, 64)
		if err == nil {
			return time.Unix(int64(unixTime), 0), nil
		}

		return time.Time{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return time.Time{}, errors.New("timestamp not found in metadata")
}
