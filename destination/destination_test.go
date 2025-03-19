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
	"strings"
	"testing"

	influxdb "github.com/conduitio-labs/conduit-connector-influxdb"
	"github.com/conduitio-labs/conduit-connector-influxdb/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := destination.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	cfg := map[string]string{
		"token":  "test-token",
		"url":    "http://localhost:8086",
		"org":    "myorg",
		"bucket": "mybucket",
	}

	con := destination.NewDestination()
	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().DestinationParams)
	is.NoErr(err)

	err = con.Open(ctx)
	is.NoErr(err)
}

func TestDestination_OpenWithInvalidMeasurement(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	cfg := map[string]string{
		"token":       "test-token",
		"url":         "http://localhost:8086",
		"org":         "myorg",
		"bucket":      "mybucket",
		"measurement": "{{ unknownFunc .Metadata }}",
	}

	con := destination.NewDestination()
	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().DestinationParams)
	is.NoErr(err)

	err = con.Open(ctx)
	is.True(strings.Contains(err.Error(), "measurement is neither a valid static measurement nor a valid Go template"))
}
