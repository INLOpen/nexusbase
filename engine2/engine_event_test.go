package engine2

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// sys.SetDebugMode(false)
}

func TestStorageEngine_PutEvent(t *testing.T) {
	// Use the in-memory shim so we can inspect stored FieldValues directly.
	shim := newStorageEngineShim(t.TempDir())
	event := HelperDataPoint(
		t,
		"user.login.attempt",
		map[string]string{"ip": "192.168.1.100", "region": "us-east"},
		1678886400000000000,
		map[string]interface{}{
			"success":       true,
			"username":      "testuser",
			"latency_ms":    float64(55.8),
			"attempt_count": int64(3),
		},
	)

	// Action via shim (stores FieldValues directly)
	err := shim.Put(context.Background(), event)
	require.NoError(t, err)

	// Verification: check shim's in-memory store
	key := string(core.EncodeSeriesKeyWithString(event.Metric, event.Tags))
	shim.mu.RLock()
	series, ok := shim.data[key]
	shim.mu.RUnlock()
	require.True(t, ok, "series key should exist in shim store")
	fv, ok2 := series[event.Timestamp]
	require.True(t, ok2, "timestamp should exist in shim series map")
	assert.Equal(t, event.Fields, fv, "stored FieldValues should match original event fields")
}

func TestStorageEngine_PutEvent_EdgeCasesAndErrors(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name             string
		metric           string
		tags             map[string]string
		ts               int64
		fields           map[string]interface{}
		setupOpts        func(opts *StorageEngineOptions)
		wantErr          bool
		wantErrType      interface{}
		verify           func(t *testing.T, eng StorageEngineExternal)
		wantErrField     bool
		wantErrFieldType interface{}
	}{
		{
			name:        "error_invalid_metric_name",
			metric:      "",
			tags:        map[string]string{"id": "A"},
			ts:          1,
			fields:      map[string]interface{}{"value": 1.0},
			wantErr:     true,
			wantErrType: (*core.ValidationError)(nil),
		},
		{
			name:        "error_invalid_tag_name",
			metric:      "valid.metric",
			tags:        map[string]string{"__reserved": "fail"},
			ts:          1,
			fields:      map[string]interface{}{"value": 1.0},
			wantErr:     true,
			wantErrType: (*core.ValidationError)(nil),
		},
		{
			name:             "error_unsupported_field_type",
			metric:           "valid.metric",
			tags:             map[string]string{"id": "A"},
			ts:               1,
			fields:           map[string]interface{}{"bad_field": make(chan int)},
			wantErrField:     true,
			wantErrFieldType: (*core.UnsupportedTypeError)(nil),
		},
		{
			name:    "success_no_tags",
			metric:  "metric.no_tags",
			tags:    nil,
			ts:      1,
			fields:  map[string]interface{}{"value": 1.0},
			wantErr: false,
			verify: func(t *testing.T, eng StorageEngineExternal) {
				// Verify via public API: Get should find the point
				_, err := eng.Get(context.Background(), "metric.no_tags", nil, 1)
				require.NoError(t, err)
			},
		},
		{
			name:    "success_no_fields",
			metric:  "metric.no_fields",
			tags:    map[string]string{"id": "A"},
			ts:      1,
			fields:  nil,
			wantErr: false,
			verify: func(t *testing.T, eng StorageEngineExternal) {
				// Verify via public API: Get should return a FieldValues map (possibly empty)
				fv, err := eng.Get(context.Background(), "metric.no_fields", map[string]string{"id": "A"}, 1)
				require.NoError(t, err)
				assert.Empty(t, fv, "decoded fields map should be empty")
			},
		},
		{
			name:   "success_triggers_flush",
			metric: "metric.flush.trigger",
			tags:   map[string]string{"id": "A"},
			ts:     1,
			fields: map[string]interface{}{"data": "some data that makes the entry large enough"},
			setupOpts: func(opts *StorageEngineOptions) {
				opts.MemtableThreshold = 10
			},
			wantErr: false,
			verify: func(t *testing.T, eng StorageEngineExternal) {
				// Instead of inspecting internal L0 tables, verify via public API that data is queryable
				require.Eventually(t, func() bool {
					_, err := eng.Get(context.Background(), "metric.flush.trigger", map[string]string{"id": "A"}, 1)
					return err == nil
				}, 2*time.Second, 10*time.Millisecond, "timed out waiting for data to be queryable after flush")
			},
		},
	}

	for _, tc1 := range testCases {
		tc := tc1
		t.Run(tc.name, func(t *testing.T) {
			opts := GetBaseOptsForTest(t, "test")
			if tc.setupOpts != nil {
				tc.setupOpts(&opts)
			}

			engine, err := NewStorageEngine(opts)
			require.NoError(t, err)

			// Validate metric and tag names using a Validator (matches legacy engine behavior)
			// Perform validation before starting the engine to avoid interference
			// from background processes that run during Start().
			validator := core.NewValidator()
			vErr := core.ValidateMetricAndTags(validator, tc.metric, tc.tags)

			err = engine.Start()
			require.NoError(t, err)
			defer engine.Close()
			if tc.wantErr {
				require.Error(t, vErr)
				if tc.wantErrType != nil {
					assert.ErrorAs(t, vErr, &tc.wantErrType)
				}
			} else {
				require.NoError(t, vErr)
			}

			// Create the datapoint and validate field types
			dpPtr, dpErr := core.NewSimpleDataPoint(tc.metric, tc.tags, tc.ts, tc.fields)
			if tc.wantErrField {
				require.Error(t, dpErr)
				if tc.wantErrFieldType != nil {
					assert.ErrorAs(t, dpErr, &tc.wantErrFieldType)
				}
			} else {
				require.NoError(t, dpErr)
			}

			var event core.DataPoint
			if dpErr == nil {
				event = *dpPtr
				err = engine.Put(ctx, event)
			} else {
				err = dpErr
			}

			if tc.wantErr {
				require.Error(t, err)
			}

			if tc.wantErrField {
				require.Error(t, err)
			}

			if !tc.wantErr && !tc.wantErrField {
				require.NoError(t, err)
			}

			if tc.verify != nil {
				tc.verify(t, engine)
			}
		})
	}
}
