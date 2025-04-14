package source_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	influxdb "github.com/conduitio-labs/conduit-connector-influxdb"
	"github.com/conduitio-labs/conduit-connector-influxdb/source"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/matryer/is"
)

func TestSource_Read_success(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	cfg := map[string]string{
		"token":                     "my-super-secret-auth-token",
		"url":                       "http://localhost:8086",
		"org":                       "conduit",
		"bucket":                    "conduit_test",
		"measurements.measurement1": "field1",
	}

	con := source.NewSource()
	defer func() {
		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

	err := sdk.Util.ParseConfig(ctx, cfg, con.Config(), influxdb.Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	// create some records in influxdb
	err = generateData()
	is.NoErr(err)

	err = con.Open(ctx, nil)
	is.NoErr(err)

	var allRecords []opencdc.Record
	for len(allRecords) < 3 {
		records, err := con.ReadN(ctx, 3-len(allRecords))
		is.NoErr(err)

		allRecords = append(allRecords, records...)
	}

	is.Equal(len(allRecords), 3)

	cancel()
	err = con.Teardown(ctx)
	is.NoErr(err)
}

func generateData() error {
	token := "my-super-secret-auth-token"
	url := "http://localhost:8086"
	client := influxdb2.NewClient(url, token)
	org := "conduit"
	bucket := "conduit_test"

	writeAPI := client.WriteAPIBlocking(org, bucket)
	for value := 1; value <= 3; value++ {
		tags := map[string]string{"tag1": "tagvalue1"}
		fields := map[string]interface{}{"field1": value}
		point := write.NewPoint("measurement1", tags, fields, time.Now())
		if err := writeAPI.WritePoint(context.Background(), point); err != nil {
			return fmt.Errorf("error generating data: %w", err)
		}
	}

	return nil
}
