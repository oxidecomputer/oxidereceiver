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

func TestAddLabels(t *testing.T) {
	table := oxide.OxqlTable{Name: "test_metric"}

	for _, tc := range []struct {
		name         string
		series       oxide.Timeseries
		wantResource pcommon.Resource
		wantErr      string
	}{
		{
			name: "string: success",
			series: oxide.Timeseries{
				Fields: map[string]oxide.FieldValue{
					"hostname": {
						Type:  oxide.FieldValueTypeString,
						Value: oxide.FieldValueString("server-01"),
					},
				},
			},
			wantResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("hostname", "server-01")
				return r
			}(),
		},
		{
			name: "int: success",
			series: oxide.Timeseries{
				Fields: map[string]oxide.FieldValue{
					"port": {
						Type:  oxide.FieldValueTypeI64,
						Value: oxide.FieldValueI64(8080),
					},
				},
			},
			wantResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutInt("port", 8080)
				return r
			}(),
		},
		{
			name: "uuid: success",
			series: oxide.Timeseries{
				Fields: map[string]oxide.FieldValue{
					"instance_id": {
						Type:  oxide.FieldValueTypeUuid,
						Value: oxide.FieldValueUuid("550e8400-e29b-41d4-a716-446655440000"),
					},
				},
			},
			wantResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("instance_id", "550e8400-e29b-41d4-a716-446655440000")
				return r
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resource := pcommon.NewResource()

			err := addLabels(tc.series, table, resource)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tc.wantResource.Attributes().AsRaw(), resource.Attributes().AsRaw())
		})
	}
}

func TestEnrichLabels(t *testing.T) {
	for _, tc := range []struct {
		name         string
		resource     pcommon.Resource
		silos        map[string]string
		projects     map[string]string
		wantResource pcommon.Resource
	}{
		{
			name: "silo",
			resource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("silo_id", "123e4567-e89b-12d3-a456-426614174000")
				return r
			}(),
			silos: map[string]string{
				"123e4567-e89b-12d3-a456-426614174000": "default",
			},
			projects: map[string]string{},
			wantResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("silo_id", "123e4567-e89b-12d3-a456-426614174000")
				r.Attributes().PutStr("silo_name", "default")
				return r
			}(),
		},
		{
			name: "project",
			resource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("project_id", "987fcdeb-51a2-43f7-b890-123456789abc")
				return r
			}(),
			silos: map[string]string{},
			projects: map[string]string{
				"987fcdeb-51a2-43f7-b890-123456789abc": "my-project",
			},
			wantResource: func() pcommon.Resource {
				r := pcommon.NewResource()
				r.Attributes().PutStr("project_id", "987fcdeb-51a2-43f7-b890-123456789abc")
				r.Attributes().PutStr("project_name", "my-project")
				return r
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enrichLabels(tc.resource, tc.silos, tc.projects)
			require.Equal(t, tc.wantResource.Attributes().AsRaw(), tc.resource.Attributes().AsRaw())
		})
	}
}

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
								Type: oxide.ValueArrayTypeInteger,
								Values: oxide.ValueArrayInteger{
									Type:   oxide.ValueArrayTypeInteger,
									Values: []int{42},
								},
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
		// Doubles
		{
			name: "double: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type: oxide.ValueArrayTypeDouble,
								Values: oxide.ValueArrayDouble{
									Type:   oxide.ValueArrayTypeDouble,
									Values: []float64{42.5},
								},
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
		// Bools
		{
			name: "bool: success",
			series: oxide.Timeseries{
				Points: oxide.Points{
					Timestamps: []time.Time{now},
					Values: []oxide.Values{
						{
							Values: oxide.ValueArray{
								Type: oxide.ValueArrayTypeBoolean,
								Values: oxide.ValueArrayBoolean{
									Type:   oxide.ValueArrayTypeBoolean,
									Values: []bool{true},
								},
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
								Values: oxide.ValueArrayIntegerDistribution{
									Type: oxide.ValueArrayTypeIntegerDistribution,
									Values: []oxide.Distributionint64{
										{
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
								Values: oxide.ValueArrayDoubleDistribution{
									Type: oxide.ValueArrayTypeDoubleDistribution,
									Values: []oxide.Distributiondouble{
										{
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
