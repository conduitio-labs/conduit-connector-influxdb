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
	"time"

	"github.com/conduitio-labs/conduit-connector-influxdb/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	sdk.DefaultSourceMiddleware
	config.Config
	// Bucket is the database name to access.
	Bucket string `json:"bucket" validate:"required"`
	// Measurement typically tracks one kind of metric over time similar to a table.
	// Here we have measurement and its unique key field in map.
	Measurements map[string]string `json:"measurements" validate:"required"`
	// This period is used by workers to poll for new data at regular intervals.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"5s"`
	// The maximum number of retries of failed operations.
	Retries int `json:"retries" default:"0"`
}
