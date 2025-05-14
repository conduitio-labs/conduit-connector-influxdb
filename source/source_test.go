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

package source_test

import (
	"context"
	"testing"

	influxdb "github.com/conduitio-labs/conduit-connector-influxdb"
	"github.com/conduitio-labs/conduit-connector-influxdb/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := source.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	cfg := map[string]string{
		"token":                     "test-token",
		"url":                       "http://localhost:8086",
		"org":                       "myorg",
		"bucket":                    "mybucket",
		"measurements.measurement1": "field1",
	}

	con := source.NewSource()
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = con.Open(ctx, nil)
	is.NoErr(err)
}
