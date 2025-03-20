# Conduit Connector for <!-- readmegen:name -->Influxdb<!-- /readmegen:name -->

[Conduit](https://conduit.io) connector for <!-- readmegen:name -->Influxdb<!-- /readmegen:name -->.

<!-- readmegen:description -->
Source and destination connector for InfluxDB.<!-- /readmegen:description -->

## Source

A source connector pulls data from InfluxDB and pushes it to
downstream resources via Conduit.

### Configuration

zz xbbm readmegen:source.parameters.yaml -->
<!-- /readmegen:source.parameters.yaml -->

## Destination

A destination connector pushes data from upstream resources to InfluxDB via Conduit.

### Configuration

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "influxdb"
        settings:
          # Org is an organization name or ID.
          # Type: string
          # Required: yes
          org: ""
          # Token is used to authenticate API access.
          # Type: string
          # Required: yes
          token: ""
          # Url is the remote influxdb host for api calls.
          # Type: string
          # Required: yes
          url: ""
          # DestinationConfigParam must be either yes or no (defaults to yes).
          # Type: string
          # Required: no
          destinationConfigParam: "yes"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## Development

- To install the required tools, run `make install-tools`.
- To generate code (mocks, re-generate `connector.yaml`, update the README,
  etc.), run `make generate`.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run
the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the
required resource locally.

## How to release?

The release is done in two steps:

- Bump the version in [connector.yaml](/connector.yaml). This can be done
  with [bump_version.sh](/scripts/bump_version.sh) script, e.g.
  `scripts/bump_version.sh 2.3.4` (`2.3.4` is the new version and needs to be a
  valid semantic version). This will also automatically create a PR for the
  change.
- Tag the connector, which will kick off a release. This can be done
  with [tag.sh](/scripts/tag.sh).

## Known Issues & Limitations

- Known issue A
- Limitation A

## Planned work

- [ ] Item A
- [ ] Item B

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=)
