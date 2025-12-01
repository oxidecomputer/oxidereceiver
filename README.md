# Oxide Receiver for OpenTelemetry Collector

This repository provides an OpenTelemetry Collector receiver for the Oxide.

## Configuration

The Oxide receiver collects metrics from the Oxide API and converts them to OpenTelemetry metrics.

All configuration parameters are optional. If `host` and `token` are not provided in the configuration, the receiver will attempt to read them from the environment using the defaults [defined in the Oxide SDK](https://github.com/oxidecomputer/oxide.go#authentication).

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | string | (from environment) | The Oxide API host URL. If not specified, the Oxide SDK will read from environment variables |
| `token` | string | (from environment) | Authentication token for the Oxide API. If not specified, the Oxide SDK will read from environment variables |
| `metric_patterns` | []string | `[".*"]` | List of regex patterns to filter which metrics to collect. Metrics matching any pattern will be included |
| `scrape_concurrency` | int | `16` | Number of concurrent requests to make when scraping metrics |
| `query_lookback` | string | `"5m"` | Lookback interval for queries sent to the Oxide API (e.g., "5m", "1h") |
| `add_labels` | bool | `false` | Add human-readable labels to metrics using the Oxide API |
| `collection_interval` | duration | `1m` | Interval at which metrics are collected |
| `initial_delay` | duration | `1s` | Initial delay before starting collection |
| `timeout` | duration | `0s` | Timeout for the scraper (0 means no timeout) |

### Example Configuration

See [collector/config.example.yaml](collector/config.example.yaml) for a complete example configuration.

## Building an Otel Collector binary

This repository includes utilities to build an OpenTelemetry Collector binary that includes the `oxidereceiver`. For convenience, we also include the Otel components used in the [otelcol-contrib distribution](https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib) provided by the OpenTelemetry organization. To customize the set of Otel plugins used, update [collector/manifest.yaml](collector/manifest.yaml).

### Building the Collector

```bash
make build-collector
```

### Running the Collector

Create a `collector/config.yaml` file with your collector configuration, or copy from collector/config.example.yaml, then run:

```bash
./dist/otelcol-oxide --config collector/config.yaml
```

## Development

### Running Tests

```bash
make test
```
