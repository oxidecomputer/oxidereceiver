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

				m.SetName(table.Name)

				// Hack: get metadata from the 0th point.
				// TODO(jmcarp): Move this to the timeseries level in the api.
				if len(series.Points.Values) == 0 {
					continue
				}
				v0 := series.Points.Values[0]

				switch {
				// Handle histograms.
				//
				// Note: OxQL histograms include both buckets
				// and counts, as well as a handful of
				// preselected quantiles estimated using the P²
				// algorithm. We extract the buckets and counts
				// as an otel histogram, and the quantiles as a
				// gauge.
				case slices.Contains([]oxide.ValueArrayType{oxide.ValueArrayTypeIntegerDistribution, oxide.ValueArrayTypeDoubleDistribution}, v0.Values.Type):
					measure := m.SetEmptyHistogram()

					switch v0.MetricType {
					case oxide.MetricTypeDelta:
						measure.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					case oxide.MetricTypeCumulative:
						measure.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
					}

					quantiles := sm.Metrics().AppendEmpty()
					quantiles.SetName(fmt.Sprintf("%s:quantiles", table.Name))
					quantileGauge := quantiles.SetEmptyGauge()

					addHistogram(measure.DataPoints(), quantileGauge, table, series)
				// Handle scalar gauge.
				case v0.MetricType == oxide.MetricTypeGauge:
					measure := m.SetEmptyGauge()
					addPoint(measure.DataPoints(), table, series)

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

					addPoint(measure.DataPoints(), table, series)
				}

			}
		}
	}

	for metricName, duration := range latencies {
		s.apiRequestDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attribute.String("request_name", metricName)))
	}

	return metrics, nil
}

func addHistogram(dataPoints pmetric.HistogramDataPointSlice, quantileGauge pmetric.Gauge, table oxide.OxqlTable, series oxide.Timeseries) ([]pmetric.HistogramDataPoint, []pmetric.NumberDataPoint, error) {
	histPoints := []pmetric.HistogramDataPoint{}
	quantilePoints := []pmetric.NumberDataPoint{}

	for idx, point := range series.Points.Values {
		dp := dataPoints.AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))

		values, ok := point.Values.Values.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("couldn't cast values %+v for metric %s to []any; got unexpected type %+v", point.Values.Values, table.Name, reflect.TypeOf(point.Values.Values))
		}
		for _, value := range values {
			// The histogram value is an `any`, so marshal and unmarshal json to fit it into the appropriate distribution type.
			marshalled, err := json.Marshal(value)
			if err != nil {
				return nil, nil, fmt.Errorf("couldn't marshal distribution %+v for metric %s: %w", value, table.Name, err)
			}

			switch point.Values.Type {
			case oxide.ValueArrayTypeIntegerDistribution:
				// Unmarshall the marshalled JSON back to the expected histogram type.
				var distValue oxide.Distributionint64
				if err := json.Unmarshal(marshalled, &distValue); err != nil {
					return nil, nil, fmt.Errorf("couldn't unmarshal distribution %+v for metric %s: %w", value, table.Name, err)
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

				quantilePoints = addQuantiles(quantileGauge, []oxide.Quantile{distValue.P50, distValue.P90, distValue.P99}, dp.Timestamp())
			case oxide.ValueArrayTypeDoubleDistribution:
				// Unmarshall the marshalled JSON back to the expected histogram type.
				var distValue oxide.Distributiondouble
				if err := json.Unmarshal(marshalled, &distValue); err != nil {
					return nil, nil, fmt.Errorf("couldn't unmarshal distribution %+v for metric %s: %w", value, table.Name, err)
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

				quantilePoints = addQuantiles(quantileGauge, []oxide.Quantile{distValue.P50, distValue.P90, distValue.P99}, dp.Timestamp())
			}
		}
		histPoints = append(histPoints, dp)
	}
	return histPoints, quantilePoints, nil
}

func addPoint(dataPoints pmetric.NumberDataPointSlice, table oxide.OxqlTable, series oxide.Timeseries) ([]pmetric.NumberDataPoint, error) {
	points := []pmetric.NumberDataPoint{}
	for _, point := range series.Points.Values {
		switch point.Values.Type {
		case oxide.ValueArrayTypeInteger:
			values, ok := point.Values.Values.([]any)
			if !ok {
				return nil, fmt.Errorf("couldn't cast values %+v for metric %s to []any; got unexpected type %+v", point.Values.Values, table.Name, reflect.TypeOf(point.Values.Values))
			}
			for idx, value := range values {
				if value == nil {
					continue
				}
				intValue, ok := value.(float64)
				if !ok {
					return nil, fmt.Errorf("couldn't cast value %+v for metric %s to float; got type %+v", value, table.Name, reflect.TypeOf(value))
				}
				dp := dataPoints.AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))
				dp.SetIntValue(int64(intValue))
				points = append(points, dp)
			}
		case oxide.ValueArrayTypeDouble:
			values, ok := point.Values.Values.([]any)
			if !ok {
				return nil, fmt.Errorf("couldn't cast values %+v for metric %s to []any; got unexpected type %+v", point.Values.Values, table.Name, reflect.TypeOf(point.Values.Values))
			}
			for idx, value := range values {
				if value == nil {
					continue
				}
				floatValue, ok := value.(float64)
				if !ok {
					return nil, fmt.Errorf("couldn't cast value %+v for metric %s to float; got type %+v", value, table.Name, reflect.TypeOf(value))
				}
				dp := dataPoints.AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))
				dp.SetDoubleValue(floatValue)
				points = append(points, dp)
			}
		case oxide.ValueArrayTypeBoolean:
			values, ok := point.Values.Values.([]any)
			if !ok {
				return nil, fmt.Errorf("couldn't cast values %+v for metric %s to []any; got unexpected type %+v", point.Values.Values, table.Name, reflect.TypeOf(point.Values.Values))
			}
			for idx, value := range values {
				if value == nil {
					continue
				}
				boolValue, ok := value.(bool)
				if !ok {
					return nil, fmt.Errorf("couldn't cast value %+v for metric %s to bool; got type %+v", value, table.Name, reflect.TypeOf(value))
				}
				dp := dataPoints.AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(series.Points.Timestamps[idx]))
				intValue := 0
				if boolValue {
					intValue = 1
				}
				dp.SetIntValue(int64(intValue))
				points = append(points, dp)
			}
		default:
			return nil, fmt.Errorf("got unexpected metric value type %s", point.Values.Type)
		}
	}
	return points, nil
}

// addQuantiles emits metrics for a slice of oxide.Quantile values. In addition
// to histogram buckets and counts, OxQL exposes a set of predefined quantile
// estimates using the P² algorithm, which we extract here.
func addQuantiles(g pmetric.Gauge, quantiles []oxide.Quantile, timestamp pcommon.Timestamp) []pmetric.NumberDataPoint {
	points := []pmetric.NumberDataPoint{}
	for _, quantile := range quantiles {
		p := g.DataPoints().AppendEmpty()
		p.SetTimestamp(timestamp)
		p.SetDoubleValue(quantile.MarkerHeights[2])
		p.Attributes().PutDouble("quantile", quantile.P)
		points = append(points, p)
	}
	return points
}
