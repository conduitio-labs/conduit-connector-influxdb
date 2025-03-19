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

package config

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
type Config struct {
	// Token is used to authenticate API access.
	Token string `json:"token" validate:"required"`
	// Url is the remote influxdb host for api calls.
	URL string `json:"url" validate:"required"`
	// Org is an organization name or ID.
	Org string `json:"org" validate:"required"`
	// Bucket specifies the InfluxDB bucket for reading or writing data.
	Bucket string `json:"bucket" validate:"required"`
}
