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
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/conduitio-labs/conduit-connector-influxdb/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	sdk.DefaultDestinationMiddleware
	config.Config
	// Measurement is the measurement name to insert data into.
	Measurement string `json:"measurement" default:"{{ index .Metadata \"opencdc.collection\" }}"`
}

// measurementFunction returns a function that determines the measurement for each record individually.
// The function might be returning a static measurement name.
// If the measurement is neither static nor a template, an error is returned.
func (c *Config) measurementFunction() (f measurementFn, err error) {
	// Not a template, i.e. it's a static measurement name
	if !c.isMeasurementTemplate() {
		return func(_ opencdc.Record) (string, error) {
			return c.Measurement, nil
		}, nil
	}

	// Try to parse the measurement
	t, err := template.New("measurement").Funcs(sprig.FuncMap()).Parse(c.Measurement)
	if err != nil {
		// The measurement is not a valid Go template.
		return nil, fmt.Errorf("measurement is neither a valid static measurement nor a valid Go template: %w", err)
	}

	// The measurement is a valid template, return MeasurementFn.
	var buf bytes.Buffer

	return func(r opencdc.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute measurement template: %w", err)
		}

		return buf.String(), nil
	}, nil
}

// isMeasurementTemplate returns true if "measurement" contains a template placeholder.
func (c Config) isMeasurementTemplate() bool {
	return strings.Contains(c.Measurement, "{{") && strings.Contains(c.Measurement, "}}")
}
