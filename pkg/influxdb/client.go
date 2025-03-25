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

package influxdb

import (
	"context"
	"fmt"
	"time"

	influxdbapi "github.com/conduitio-labs/conduit-connector-influxdb/pkg/influxdb/api"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

var ErrNilClient = fmt.Errorf("nil client")

type V2Client struct {
	client influxdb2.Client
}

func NewClient(url, token string) (*V2Client, error) {
	c := influxdb2.NewClient(url, token)
	if c == nil {
		return nil, ErrNilClient
	}
	return &V2Client{client: c}, nil
}

func (c *V2Client) Query(ctx context.Context, request *influxdbapi.QueryRequest) (*influxdbapi.QueryResponse, error) {
	queryAPI := c.client.QueryAPI(request.Organization)
	query := fmt.Sprintf(`from(bucket: "%s")
	        |> range(start: %s)
	        |> filter(fn: (r) => r._measurement == "%s")
			|> filter(fn: (r) => r._time > time(v: "%s"))
			|> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")`,
		request.Bucket,
		request.After.Format(time.RFC3339Nano),
		request.Measurement,
		request.After.Format(time.RFC3339Nano))

	results, err := queryAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error quering influx: %w", err)
	}
	return &influxdbapi.QueryResponse{Result: results}, nil
}

func (c *V2Client) WriteAPI(org, bucket string) api.WriteAPIBlocking {
	return c.client.WriteAPIBlocking(org, bucket)
}

func (c *V2Client) Close() {
	c.client.Close()
}
