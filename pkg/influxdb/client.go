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
	"strconv"
	"strings"
	"time"

	influxdbapi "github.com/conduitio-labs/conduit-connector-influxdb/pkg/influxdb/api"
	"github.com/conduitio/conduit-commons/opencdc"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/query"
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
			|> group(columns: ["_field"])`,
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

func (c *V2Client) Close() {
	c.client.Close()
}

func ParseRecord(record *query.FluxRecord) (opencdc.Metadata, opencdc.StructuredData) {
	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: record.Measurement(),
		"timestamp":                record.Time().Format(time.RFC3339Nano),
	}
	metadata.SetCreatedAt(record.Time())

	payload := opencdc.StructuredData{record.Field(): record.Value()}

	for field, value := range record.Values() {
		if field == "_field" {
			continue
		}
		if strings.Contains(field, "_") || field == "result" || field == "table" {
			switch val := value.(type) {
			case string:
				metadata[field] = val
			case int:
				metadata[field] = strconv.Itoa(val)
			case int64:
				metadata[field] = fmt.Sprintf("%d", val)
			case float64:
				metadata[field] = fmt.Sprintf("%f", val)
			case bool:
				metadata[field] = fmt.Sprintf("%v", val)
			case time.Time:
				metadata[field] = val.String()
			}
		} else {
			val, ok := value.(string)
			if ok {
				metadata[fmt.Sprintf("tag.%s", field)] = val
			}
		}
	}

	return metadata, payload
}
