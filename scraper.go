package oxidereceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/oxidecomputer/oxide.go/oxide"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type oxideScraper struct {
	client   *oxide.Client
	settings component.TelemetrySettings
	cfg      *Config
	logger   *zap.Logger

	metricNames []string

	apiRequestDuration metric.Float64Gauge
	scrapeCount        metric.Int64Counter
	scrapeDuration     metric.Float64Gauge
}

func newOxideScraper(
	cfg *Config,
	settings component.TelemetrySettings,
	client *oxide.Client,
) *oxideScraper {
	return &oxideScraper{
		client:   client,
		settings: settings,
		cfg:      cfg,
		logger:   settings.Logger,
	}
}

func (s *oxideScraper) Start(ctx context.Context, _ component.Host) error {
	schemas, err := s.client.SystemTimeseriesSchemaListAllPages(ctx, oxide.SystemTimeseriesSchemaListParams{})
	if err != nil {
		return err
	}

	regexps := []*regexp.Regexp{}
	for _, pattern := range s.cfg.MetricPatterns {
		regexp, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid metric pattern %s: %w", pattern, err)
		}
		regexps = append(regexps, regexp)
	}

	metricNames := []string{}
	for _, schema := range schemas {
		for _, regexp := range regexps {
			if regexp.MatchString(string(schema.TimeseriesName)) {
				metricNames = append(metricNames, string(schema.TimeseriesName))
			}
		}
	}
	s.metricNames = metricNames

	s.logger.Info("collecting metrics", zap.Any("metrics", metricNames))

	meter := s.settings.MeterProvider.Meter("github.com/oxidecomputer/oxidereceiver")

	s.apiRequestDuration, err = meter.Float64Gauge(
		"oxide_receiver.api_request.duration",
		metric.WithDescription("Duration of API requests to the Oxide API"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("failed to create apiRequestDuration gauge: %w", err)
	}

	s.scrapeCount, err = meter.Int64Counter(
		"oxide_receiver.scrape.count",
		metric.WithDescription("Number of scrapes performed by the Oxide receiver"),
		metric.WithUnit("{scrape}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create scrapeCount counter: %w", err)
	}

	s.scrapeDuration, err = meter.Float64Gauge(
		"oxide_receiver.scrape.duration",
		metric.WithDescription("Total duration of the scrape operation"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("failed to create scrapeDuration gauge: %w", err)
	}

	return nil
}

func (s *oxideScraper) Shutdown(context.Context) error {
	return nil
}

func (s *oxideScraper) Scrape(ctx context.Context) (pmetric.Metrics, error) {
	metrics := pmetric.NewMetrics()

	var group errgroup.Group
	group.SetLimit(s.cfg.ScrapeConcurrency)

	startTime := time.Now()
	results := make([]*oxide.OxqlQueryResult, len(s.metricNames))

	// Track latencies for each request by metric name
	latencies := make(map[string]time.Duration)

	for idx, metricName := range s.metricNames {
		query := fmt.Sprintf("get %s | filter timestamp > @now() - %dm | last 1", metricName, 15)
		group.Go(func() error {
			goroStartTime := time.Now()
			result, err := s.client.SystemTimeseriesQuery(ctx, oxide.SystemTimeseriesQueryParams{
				Body: &oxide.TimeseriesQuery{
					Query: query,
				},
			})
			elapsed := time.Since(goroStartTime)
			latencies[metricName] = elapsed
			s.logger.Info("scrape query finished", zap.String("metric", metricName), zap.String("query", query), zap.Float64("latency", elapsed.Seconds()))
			if err != nil {
				return err
			}
			results[idx] = result
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		s.scrapeCount.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "failure")))
		return metrics, err
	}
	elapsed := time.Since(startTime)
	s.logger.Info("scrape finished", zap.Float64("latency", elapsed.Seconds()))

	s.scrapeDuration.Record(ctx, elapsed.Seconds())
	s.scrapeCount.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))

	// Cargo-culted from https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/googlecloudmonitoringreceiver.
	// TODO(jmcarp): Can we skip this map?
	resourceMetricsMap := make(map[string]pmetric.ResourceMetrics)
	for _, result := range results {
		for _, table := range result.Tables {
			for _, series := range table.Timeseries {
				key := table.Name
				labels := []string{}
				for key, value := range series.Fields {
					labels = append(labels, fmt.Sprintf("%s:%s", key, value.Value))
				}
				sort.Strings(labels)
				key = fmt.Sprintf("%s_%s", key, strings.Join(labels, "::"))

				rm, exists := resourceMetricsMap[key]
				if !exists {
					rm = metrics.ResourceMetrics().AppendEmpty()
					resource := rm.Resource()
					for key, value := range series.Fields {
						switch value.Type {
						case oxide.FieldValueTypeString:
							resource.Attributes().PutStr(key, value.Value.(string))
						case oxide.FieldValueTypeI8, oxide.FieldValueTypeI16, oxide.FieldValueTypeI32, oxide.FieldValueTypeI64,
							oxide.FieldValueTypeU8, oxide.FieldValueTypeU16, oxide.FieldValueTypeU32, oxide.FieldValueTypeU64:
							intValue, ok := value.Value.(float64)
							if !ok {
								s.logger.Warn("couldn't cast label to float", zap.String("metric", table.Name), zap.String("field", key), zap.Any("value", value))
							}
							resource.Attributes().PutInt(key, int64(intValue))
						default:
							resource.Attributes().PutStr(key, fmt.Sprintf("%v", value.Value))
						}
					}
					resourceMetricsMap[key] = rm
				}

				// Ensure we have a ScopeMetrics to append the metric to
				var sm pmetric.ScopeMetrics
				if rm.ScopeMetrics().Len() == 0 {
					sm = rm.ScopeMetrics().AppendEmpty()
				} else {
					// For simplicity, let's assume all metrics will share the same ScopeMetrics
					sm = rm.ScopeMetrics().At(0)
				}

				// Create a new Metric
				m := sm.Metrics().AppendEmpty()

				// Set metric name, description, and unit
				m.SetName(table.Name)

				// Hack: get metadata from the 0th point.
				// TODO(jmcarp): Move this to the timeseries level in the api.
				if len(series.Points.Values) == 0 {
					continue
				}
				v0 := series.Points.Values[0]

				switch {
				// Handle histograms.
				case slices.Contains([]oxide.ValueArrayType{oxide.ValueArrayTypeIntegerDistribution, oxide.ValueArrayTypeDoubleDistribution}, v0.Values.Type):
					measure := m.SetEmptyHistogram()

					for idx, point := range series.Points.Values {
						dp := measure.DataPoints().AppendEmpty()
						dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))

						values, ok := point.Values.Values.([]any)
						if !ok {
							s.logger.Warn("couldn't cast values to []any", zap.String("metric", table.Name))
						}
						for _, value := range values {
							// The histogram value is an `any`, so marshal and unmarshal json to fit it into the appropriate distribution type.
							marshalled, err := json.Marshal(value)
							if err != nil {
								s.logger.Warn("couldn't marshal distribution value", zap.String("name", table.Name), zap.Any("value", value))
							}
							switch v0.Values.Type {
							case oxide.ValueArrayTypeIntegerDistribution:
								var distValue oxide.Distributionint64
								if err := json.Unmarshal(marshalled, &distValue); err != nil {
									s.logger.Warn("couldn't unmarshal distribution value", zap.String("name", table.Name), zap.Any("value", value))
								}
								bins := make([]float64, len(distValue.Bins))
								for idx := range distValue.Bins {
									bins[idx] = float64(distValue.Bins[idx])
								}
								dp.ExplicitBounds().FromRaw(bins)

								counts := dp.BucketCounts()
								total := 0
								for _, count := range distValue.Counts {
									counts.Append(uint64(count))
									total += count
								}
								dp.SetCount(uint64(total))
							case oxide.ValueArrayTypeDoubleDistribution:
								var distValue oxide.Distributiondouble
								if err := json.Unmarshal(marshalled, &distValue); err != nil {
									s.logger.Warn("couldn't unmarshal distribution value", zap.String("name", table.Name), zap.Any("value", value))
								}
								bins := make([]float64, len(distValue.Bins))
								for idx := range distValue.Bins {
									bins[idx] = float64(distValue.Bins[idx])
								}
								dp.ExplicitBounds().FromRaw(bins)

								counts := dp.BucketCounts()
								total := 0
								for _, count := range distValue.Counts {
									counts.Append(uint64(count))
									total += count
								}
								dp.SetCount(uint64(total))
							}
						}
					}
				// Handle scalar gauge.
				case v0.MetricType == oxide.MetricTypeGauge:
					measure := m.SetEmptyGauge()
					addPoint(func() pmetric.NumberDataPoint { return measure.DataPoints().AppendEmpty() }, table, series, s.logger)

				// Handle scalar counter.
				default:
					measure := m.SetEmptySum()

					switch v0.MetricType {
					case oxide.MetricTypeDelta:
						measure.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
						measure.SetIsMonotonic(true)
					case oxide.MetricTypeCumulative:
						measure.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
						measure.SetIsMonotonic(true)
					}

					addPoint(func() pmetric.NumberDataPoint { return measure.DataPoints().AppendEmpty() }, table, series, s.logger)
				}

			}
		}
	}

	for metricName, duration := range latencies {
		s.apiRequestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attribute.String("request_name", metricName)))
	}

	return metrics, nil
}

func addPoint(pointFactory func() pmetric.NumberDataPoint, table oxide.OxqlTable, series oxide.Timeseries, logger *zap.Logger) {
	for _, point := range series.Points.Values {
		switch point.Values.Type {
		case oxide.ValueArrayTypeInteger:
			values, ok := point.Values.Values.([]any)
			if !ok {
				logger.Warn("couldn't cast values to []any", zap.Any("values", point.Values.Values), zap.String("metric", table.Name), zap.Reflect("type", reflect.TypeOf(point.Values.Values)))
			}
			for idx, value := range values {
				if value == nil {
					continue
				}
				intValue, ok := value.(float64)
				if !ok {
					logger.Warn("couldn't cast value to float", zap.Any("value", value), zap.String("metric", table.Name))
				}
				dp := pointFactory()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))
				dp.SetIntValue(int64(intValue))
			}
		case oxide.ValueArrayTypeDouble:
			values, ok := point.Values.Values.([]any)
			if !ok {
				logger.Warn("couldn't cast values to []any", zap.Any("values", point.Values.Values), zap.String("metric", table.Name), zap.Reflect("type", reflect.TypeOf(point.Values.Values)))
			}
			for idx, value := range values {
				if value == nil {
					continue
				}
				floatValue, ok := value.(float64)
				if !ok {
					logger.Warn("couldn't cast value to float", zap.Any("values", point.Values.Values), zap.String("metric", table.Name), zap.Reflect("type", reflect.TypeOf(point.Values.Values)))
				}
				dp := pointFactory()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))
				dp.SetDoubleValue(floatValue)
			}
		case oxide.ValueArrayTypeBoolean:
			values, ok := point.Values.Values.([]any)
			if !ok {
				logger.Warn("couldn't cast values to []any", zap.Any("values", point.Values.Values), zap.String("metric", table.Name))
			}
			for idx, value := range values {
				if value == nil {
					continue
				}
				boolValue, ok := value.(bool)
				if !ok {
					logger.Warn("couldn't cast value to float", zap.Any("values", point.Values.Values), zap.String("metric", table.Name), zap.Reflect("type", reflect.TypeOf(point.Values.Values)))
				}
				dp := pointFactory()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))
				intValue := 0
				if boolValue {
					intValue = 1
				}
				dp.SetIntValue(int64(intValue))
			}
		default:
			logger.Info("Unhandled metric value type:", zap.Reflect("Type", point.Values.Type))
		}
	}
}
