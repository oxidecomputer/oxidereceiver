package oxidereceiver

import (
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/oxidecomputer/oxide.go/oxide"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestAddPoint(t *testing.T) {
	now := time.Now()
	table := oxide.OxqlTable{Name: "test_metric"}

	for _, tc := range []struct {
		name        string
		series      oxide.Timeseries
		wantMetrics []pmetric.NumberDataPoint
		wantErr     string
	}{
		// Ints
		{
			name: "int: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeInteger,
								Values: []any{float64(42)},
							},
						},
					},
				},
			},
			wantMetrics: []pmetric.NumberDataPoint{
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetIntValue(42)
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					return dp
				}(),
			},
		},
		{
			name: "int: type assertion error on outer array",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeInteger,
								Values: "not an array",
							},
						},
					},
				},
			},
			wantErr: "couldn't cast values",
		},
		{
			name: "int: type assertion error on value",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeInteger,
								Values: []any{"not a number"},
							},
						},
					},
				},
			},
			wantErr: "couldn't cast value",
		},
		// Doubles
		{
			name: "double: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeDouble,
								Values: []any{float64(42.5)},
							},
						},
					},
				},
			},
			wantMetrics: []pmetric.NumberDataPoint{
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetDoubleValue(42.5)
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					return dp
				}(),
			},
		},
		{
			name: "double: type assertion error on outer array",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeDouble,
								Values: "not an array",
							},
						},
					},
				},
			},
			wantErr: "couldn't cast values",
		},
		{
			name: "double: type assertion error on value",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeDouble,
								Values: []any{"not a number"},
							},
						},
					},
				},
			},
			wantErr: "couldn't cast value",
		},
		// Bools
		{
			name: "bool: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeBoolean,
								Values: []any{true},
							},
						},
					},
				},
			},
			wantMetrics: []pmetric.NumberDataPoint{
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetIntValue(1)
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					return dp
				}(),
			},
		},
		{
			name: "bool: type assertion error on outer array",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeBoolean,
								Values: "not an array",
							},
						},
					},
				},
			},
			wantErr: "couldn't cast values",
		},
		{
			name: "bool: type assertion error on value",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeBoolean,
								Values: []any{"not a boolean"},
							},
						},
					},
				},
			},
			wantErr: "couldn't cast value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataPoints := pmetric.NewNumberDataPointSlice()

			err := addPoint(dataPoints, table, tc.series)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tc.wantMetrics), dataPoints.Len())

			for idx, wantMetric := range tc.wantMetrics {
				err := pmetrictest.CompareNumberDataPoint(wantMetric, dataPoints.At(idx))
				require.NoError(t, err, "mismatch at index %d", idx)
			}
		})
	}
}

func TestAddHistogram(t *testing.T) {
	now := time.Now()
	table := oxide.OxqlTable{Name: "test_metric"}

	for _, tc := range []struct {
		name          string
		series        oxide.Timeseries
		wantMetrics   []pmetric.HistogramDataPoint
		wantQuantiles []pmetric.NumberDataPoint
		wantErr       string
	}{
		{
			name: "int: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type: oxide.ValueArrayTypeIntegerDistribution,
								Values: []any{
									oxide.Distributionint64{
										Bins:   []int{0, 1, 2},
										Counts: []int{1, 2, 3},
										P50: oxide.Quantile{
											MarkerHeights: []float64{0.0, 1.0, 1.5, 1.8, 2.0},
											P:             0.5,
										},
										P90: oxide.Quantile{
											MarkerHeights: []float64{0.0, 1.0, 1.9, 1.95, 2.0},
											P:             0.9,
										},
										P99: oxide.Quantile{
											MarkerHeights: []float64{0.0, 1.0, 1.99, 1.995, 2.0},
											P:             0.99,
										},
									},
								},
							},
						},
					},
				},
			},
			wantMetrics: []pmetric.HistogramDataPoint{
				func() pmetric.HistogramDataPoint {
					dp := pmetric.NewHistogramDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetCount(6) // 1+2+3
					dp.ExplicitBounds().FromRaw([]float64{0, 1, 2})
					dp.BucketCounts().FromRaw([]uint64{1, 2, 3})
					return dp
				}(),
			},
			wantQuantiles: []pmetric.NumberDataPoint{
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetDoubleValue(1.5)
					dp.Attributes().PutDouble("quantile", 0.5)
					return dp
				}(),
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetDoubleValue(1.9)
					dp.Attributes().PutDouble("quantile", 0.9)
					return dp
				}(),
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetDoubleValue(1.99)
					dp.Attributes().PutDouble("quantile", 0.99)
					return dp
				}(),
			},
		},
		{
			name: "double: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type: oxide.ValueArrayTypeDoubleDistribution,
								Values: []any{
									oxide.Distributiondouble{
										Bins:   []float64{0.0, 1.0, 2.0},
										Counts: []int{1, 2, 3},
										P50: oxide.Quantile{
											MarkerHeights: []float64{0.0, 1.0, 1.5, 1.8, 2.0},
											P:             0.5,
										},
										P90: oxide.Quantile{
											MarkerHeights: []float64{0.0, 1.0, 1.9, 1.95, 2.0},
											P:             0.9,
										},
										P99: oxide.Quantile{
											MarkerHeights: []float64{0.0, 1.0, 1.99, 1.995, 2.0},
											P:             0.99,
										},
									},
								},
							},
						},
					},
				},
			},
			wantMetrics: []pmetric.HistogramDataPoint{
				func() pmetric.HistogramDataPoint {
					dp := pmetric.NewHistogramDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetCount(6) // 1+2+3
					dp.ExplicitBounds().FromRaw([]float64{0.0, 1.0, 2.0})
					dp.BucketCounts().FromRaw([]uint64{1, 2, 3})
					return dp
				}(),
			},
			wantQuantiles: []pmetric.NumberDataPoint{
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetDoubleValue(1.5)
					dp.Attributes().PutDouble("quantile", 0.5)
					return dp
				}(),
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetDoubleValue(1.9)
					dp.Attributes().PutDouble("quantile", 0.9)
					return dp
				}(),
				func() pmetric.NumberDataPoint {
					dp := pmetric.NewNumberDataPoint()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
					dp.SetDoubleValue(1.99)
					dp.Attributes().PutDouble("quantile", 0.99)
					return dp
				}(),
			},
		},
		{
			name: "cast error: values not []any",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type:   oxide.ValueArrayTypeIntegerDistribution,
								Values: "not an array",
							},
						},
					},
				},
			},
			wantErr: "couldn't cast values",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			histogramDataPoints := pmetric.NewHistogramDataPointSlice()
			quantileGauge := pmetric.NewGauge()

			err := addHistogram(histogramDataPoints, quantileGauge, table, tc.series)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(tc.wantMetrics), histogramDataPoints.Len())
			require.Equal(t, len(tc.wantQuantiles), quantileGauge.DataPoints().Len())

			for idx, wantMetric := range tc.wantMetrics {
				err := pmetrictest.CompareHistogramDataPoints(wantMetric, histogramDataPoints.At(idx))
				require.NoError(t, err, "mismatch at index %d", idx)
			}

			for idx, wantQuantile := range tc.wantQuantiles {
				err := pmetrictest.CompareNumberDataPoint(wantQuantile, quantileGauge.DataPoints().At(idx))
				require.NoError(t, err, "mismatch at quantile index %d", idx)
			}
		})
	}
}
