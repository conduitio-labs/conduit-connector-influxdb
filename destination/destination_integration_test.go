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

package destination_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	influxdb "github.com/conduitio-labs/conduit-connector-influxdb"
	"github.com/conduitio-labs/conduit-connector-influxdb/destination"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/matryer/is"
)

func TestDestination_Write_success(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
	)

	ctx := context.Background()
	con := destination.NewDestination()
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().DestinationParams)
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	// Write records to Influxdb
	timestamp := time.Now().Format(time.RFC3339)
	n, err := con.Write(ctx, []opencdc.Record{
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement",
				"timestamp":                timestamp,
				"tag.device":               "sensor-01",
				"tag.location":             "room-101",
			},
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte(`{"temperature": 24.5, "humidity": 65.2}`)),
			},
		},
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement1",
				"timestamp":                timestamp,
				"tag.device":               "sensor-02",
				"tag.location":             "room-102",
			},
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte(`{"temperature": 25.5, "humidity": 66.2}`)),
			},
		},
	})
	is.NoErr(err)
	is.Equal(n, 2)

	verifyRecordExists(t, cfg, "test_measurement", "sensor-01", "room-101", 24.5, 65.2)
	verifyRecordExists(t, cfg, "test_measurement1", "sensor-02", "room-102", 25.5, 66.2)

	// Write one more record with same timestamp, measurement, tags and fields with updated values.
	n, err = con.Write(ctx, []opencdc.Record{
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement",
				"timestamp":                timestamp,
				"tag.device":               "sensor-01",
				"tag.location":             "room-101",
			},
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte(`{"temperature": 50.5, "humidity": 30.2}`)),
			},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	verifyRecordExists(t, cfg, "test_measurement", "sensor-01", "room-101", 50.5, 30.2)
}

func TestDestination_Write_emptyPayload(t *testing.T) {
	is := is.New(t)
	cfg := prepareConfig(t)

	ctx := context.Background()
	con := destination.NewDestination()
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().DestinationParams)
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	// Write record with empty payload
	n, err := con.Write(ctx, []opencdc.Record{
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement",
				"timestamp":                time.Now().Format(time.RFC3339),
			},
			Payload: opencdc.Change{
				After: nil,
			},
		},
	})
	is.True(err != nil)
	is.Equal(err.Error(), "failed to convert payload to fields: empty payload")
	is.Equal(n, 0)
}

func TestDestination_Write_missingTimestamp(t *testing.T) {
	is := is.New(t)
	cfg := prepareConfig(t)

	ctx := context.Background()
	con := destination.NewDestination()
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().DestinationParams)
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	// Write record without timestamp
	n, err := con.Write(ctx, []opencdc.Record{
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement",
			},
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte(`{"temperature": 22.5}`)),
			},
		},
	})
	is.True(err != nil)
	is.Equal(err.Error(), "failed to get timestamp: timestamp not found in metadata or payload")
	is.Equal(n, 0)
}

func TestDestination_Write_timestampInPayload(t *testing.T) {
	is := is.New(t)
	cfg := prepareConfig(t)

	ctx := context.Background()
	con := destination.NewDestination()
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().DestinationParams)
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)

	currentTime := time.Now()
	timestampRFC3339 := currentTime.Format(time.RFC3339)
	timestampUnix := float64(currentTime.Unix())

	records := []opencdc.Record{
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement",
				"tag.device":               "sensor-01",
				"tag.location":             "room-101",
			},
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte(fmt.Sprintf(`{"temperature": 22.5, "humidity": 55.0, "timestamp": "%s"}`, timestampRFC3339))),
			},
		},
		{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "test_measurement1",
				"tag.device":               "sensor-01",
				"tag.location":             "room-101",
			},
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte(fmt.Sprintf(`{"temperature": 23.5, "humidity": 50.5, "timestamp": %f}`, timestampUnix))),
			},
		},
	}

	n, err := con.Write(ctx, records)
	is.NoErr(err)
	is.Equal(n, 2)

	verifyRecordExists(t, cfg, "test_measurement", "sensor-01", "room-101", 22.5, 55.0)
	verifyRecordExists(t, cfg, "test_measurement1", "sensor-01", "room-101", 23.5, 50.5)
}

// verifyRecordExists queries InfluxDB and checks if the expected point exists.
func verifyRecordExists(t *testing.T, cfg map[string]string, measurement, device, location string, expectedTemp, expectedHumidity float64) {
	t.Helper()

	query := fmt.Sprintf(`from(bucket: "%s")
		|> range(start: -10m)
		|> filter(fn: (r) => r._measurement == "%s" and r.device == "%s" and r.location == "%s")`,
		cfg["bucket"], measurement, device, location)

	records, err := queryInfluxDB(cfg, query)
	is := is.New(t)
	is.NoErr(err)

	// Validate expected values
	is.Equal(records["temperature"], expectedTemp)
	is.Equal(records["humidity"], expectedHumidity)
}

// queryInfluxDB executes an InfluxDB Flux query and returns the results as a map.
func queryInfluxDB(cfg map[string]string, fluxQuery string) (map[string]float64, error) {
	client := influxdb2.NewClient(cfg["url"], cfg["token"])
	defer client.Close()

	queryAPI := client.QueryAPI(cfg["org"])
	result, err := queryAPI.Query(context.Background(), fluxQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Store field values in a map
	records := make(map[string]float64)
	for result.Next() {
		if value, ok := result.Record().Value().(float64); ok {
			records[result.Record().Field()] = value
		}
	}

	if result.Err() != nil {
		return nil, fmt.Errorf("failed to parse response: %w", result.Err())
	}

	return records, nil
}

// prepareConfig returns integration test config.
func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	// default params, connects to Influxdb docker instance.
	return map[string]string{
		"url":    "http://localhost:8086",
		"token":  "my-super-secret-auth-token",
		"org":    "conduit",
		"bucket": "conduit_test",
	}
}
