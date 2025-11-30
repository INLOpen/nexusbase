package nbql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	corenbql "github.com/INLOpen/nexuscore/nbql"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// Executor processes parsed AST commands and interacts with the storage engine.
type Executor struct {
	engine engine2.StorageEngineExternal
	clock  clock.Clock
}

// NewExecutor creates a new command executor.
func NewExecutor(eng engine2.StorageEngineExternal, clock clock.Clock) *Executor {
	return &Executor{engine: eng, clock: clock}
}

// Execute takes a command from the AST and runs it against the engine.
// It returns a formatted string response.
func (e *Executor) Execute(ctx context.Context, cmd corenbql.Command) (interface{}, error) {
	switch c := cmd.(type) {
	case *corenbql.PushStatement:
		return e.executePush(ctx, c)
	case *corenbql.PushsStatement:
		return e.executePushs(ctx, c)
	case *corenbql.QueryStatement:
		return e.executeQuery(ctx, c)
	case *corenbql.RemoveStatement:
		return e.executeRemove(ctx, c)
	case *corenbql.ShowStatement:
		return e.executeShow(ctx, c)
	case *corenbql.FlushStatement:
		return e.executeFlush(ctx, c)
	case *corenbql.SnapshotStatement:
		return e.executeSnapshot(ctx, c)
	case *corenbql.RestoreStatement:
		return e.executeRestore(ctx, c)
	default:
		return "", fmt.Errorf("unknown or unsupported command type: %T", c)
	}
}

// executeSnapshot handles the SNAPSHOT command.
func (e *Executor) executeSnapshot(ctx context.Context, cmd *corenbql.SnapshotStatement) (interface{}, error) {
	// Note: The StorageEngineInterface needs to be updated with a CreateSnapshot method.
	// e.g., CreateSnapshot(ctx context.Context) (snapshotPath string, err error)
	snapshotPath, err := e.engine.CreateSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Return a structured response with the path to the created snapshot.
	return map[string]interface{}{
		"status":  "OK",
		"message": fmt.Sprintf("Snapshot created successfully at: %s", snapshotPath),
		"path":    snapshotPath,
	}, nil
}

// executeRestore handles the RESTORE command.
func (e *Executor) executeRestore(ctx context.Context, cmd *corenbql.RestoreStatement) (interface{}, error) {
	// Note: The StorageEngineInterface needs to be updated with a RestoreFromSnapshot method.
	// e.g., RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error
	err := e.engine.RestoreFromSnapshot(ctx, cmd.Path, cmd.Overwrite)
	if err != nil {
		// For restore, we return a standard ManipulateResponse even on error,
		// but the error field will be populated.
		return nil, fmt.Errorf("failed to restore from snapshot '%s': %w", cmd.Path, err)
	}

	return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil
}

// executePush handles the PUSH command.
func (e *Executor) executePush(ctx context.Context, cmd *corenbql.PushStatement) (interface{}, error) {
	ts := cmd.Timestamp
	if ts == 0 {
		ts = e.clock.Now().UnixNano()
	}

	fieldValues, err := core.NewFieldValuesFromMap(cmd.Fields)
	if err != nil {
		return ManipulateResponse{}, fmt.Errorf("invalid field values for metric '%s': %w", cmd.Metric, err)
	}

	dp := core.DataPoint{Metric: cmd.Metric, Tags: cmd.Tags, Timestamp: ts, Fields: fieldValues}

	if err := e.engine.Put(ctx, dp); err != nil {
		return ManipulateResponse{}, err
	}

	return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil
}

// executePush handles the PUSH command batch.
func (e *Executor) executePushs(ctx context.Context, cmd *corenbql.PushsStatement) (interface{}, error) {
	now := e.clock.Now().UnixNano()
	dps := make([]core.DataPoint, len(cmd.Items))
	for i, item := range cmd.Items {
		fieldValues, err := core.NewFieldValuesFromMap(item.Fields)
		if err != nil {
			return ManipulateResponse{}, fmt.Errorf("invalid field values for item %d: %w", i, err)
		}
		ts := now
		if item.Timestamp != 0 {
			ts = item.Timestamp
		}

		dps[i] = core.DataPoint{
			Metric:    item.Metric,
			Tags:      item.Tags,
			Fields:    fieldValues,
			Timestamp: ts,
		}
	}
	if err := e.engine.PutBatch(ctx, dps); err != nil {
		return ManipulateResponse{}, err
	}
	return ManipulateResponse{Status: ResponseOK, RowsAffected: uint64(len(cmd.Items))}, nil
}

func (e *Executor) QueryStream(ctx context.Context, cmd *corenbql.QueryStatement) (core.QueryParams, core.QueryResultIteratorInterface, error) {
	// Translate AST-level AggregationSpecs to core-level AggregationSpecs
	coreAggSpecs := make([]core.AggregationSpec, len(cmd.AggregationSpecs))
	for i, spec := range cmd.AggregationSpecs {
		coreAggSpecs[i] = core.AggregationSpec{
			// Note: We might need validation here to ensure the function string is valid
			// before casting, but for now, we assume the parser and engine will handle it.
			Function: core.AggregationFunc(spec.Function),
			Field:    spec.Field,
			Alias:    spec.Alias,
		}
	}

	params := core.QueryParams{
		Metric:             cmd.Metric,
		StartTime:          cmd.StartTime,
		EndTime:            cmd.EndTime,
		Tags:               cmd.Tags,
		AggregationSpecs:   coreAggSpecs,
		IsRelative:         cmd.IsRelative,
		RelativeDuration:   cmd.RelativeDuration,
		DownsampleInterval: cmd.DownsampleInterval,
		Limit:              cmd.Limit,
		EmitEmptyWindows:   cmd.EmitEmptyWindows,
		Order:              cmd.SortOrder,
	}

	// If a cursor is provided in the query, decode it and set it in the params.
	if cmd.AfterCursor != "" {
		decodedKey, err := base64.StdEncoding.DecodeString(cmd.AfterCursor)
		if err != nil {
			return params, nil, fmt.Errorf("invalid 'after' cursor: not valid base64: %w", err)
		}
		params.AfterKey = decodedKey
	}

	iter, err := e.engine.Query(ctx, params)
	if err != nil {
		return params, nil, err
	}
	return params, iter, nil
}

// executeRemove handles the REMOVE command.
func (e *Executor) executeRemove(ctx context.Context, cmd *corenbql.RemoveStatement) (interface{}, error) {
	switch cmd.Type {
	case corenbql.RemoveTypeSeries:
		// This handles REMOVE SERIES "metric" TAGGED (...)
		if err := e.engine.DeleteSeries(ctx, cmd.Metric, cmd.Tags); err != nil {
			return ManipulateResponse{}, err
		}
		// DeleteSeries affects 1 series definition.
		return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil

	case corenbql.RemoveTypePoint:
		// This handles REMOVE FROM "metric" TAGGED (...) AT <timestamp>
		// We can use DeletesByTimeRange with the same start and end time to delete a single point.
		if err := e.engine.DeletesByTimeRange(ctx, cmd.Metric, cmd.Tags, cmd.StartTime, cmd.StartTime); err != nil {
			return ManipulateResponse{}, fmt.Errorf("failed to delete point for series '%s' with tags %v: %w", cmd.Metric, cmd.Tags, err)
		}
		return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil

	case corenbql.RemoveTypeRange:
		// This handles REMOVE FROM "metric" TAGGED (...) FROM <start> TO <end>
		if err := e.engine.DeletesByTimeRange(ctx, cmd.Metric, cmd.Tags, cmd.StartTime, cmd.EndTime); err != nil {
			return ManipulateResponse{}, fmt.Errorf("failed to delete range for series '%s' with tags %v: %w", cmd.Metric, cmd.Tags, err)
		}
		// The operation affects 1 series, but the number of deleted points is unknown.
		// We return 1 to indicate the series operation was attempted.
		return ManipulateResponse{Status: ResponseOK, RowsAffected: 1}, nil

	default:
		return ManipulateResponse{}, fmt.Errorf("unsupported REMOVE command type: %s", cmd.Type)
	}
}

// executeShow handles the SHOW command.
func (e *Executor) executeShow(ctx context.Context, cmd *corenbql.ShowStatement) (interface{}, error) {
	var results []string
	var err error

	switch cmd.Type {
	case corenbql.ShowMetrics:
		results, err = e.engine.GetMetrics()
	case corenbql.ShowTagKeys:
		// Corresponds to SHOW TAG KEYS FROM <metric>
		results, err = e.engine.GetTagsForMetric(cmd.Metric)
	case corenbql.ShowTagValues:
		// Corresponds to SHOW TAG VALUES [FROM <metric>] WITH KEY = <key>
		results, err = e.engine.GetTagValues(cmd.Metric, cmd.TagKey)
	default:
		return "", fmt.Errorf("unsupported SHOW command type: %s", cmd.Type)
	}

	if err != nil {
		return "", err // Propagate engine errors
	}

	if len(results) == 0 {
		return "(empty set)", nil
	}

	return strings.Join(results, "\n"), nil
}

// executeFlush handles the FLUSH command with its variations.
func (e *Executor) executeFlush(ctx context.Context, cmd *corenbql.FlushStatement) (interface{}, error) {
	switch cmd.Type {
	case corenbql.FlushMemtable:
		// Asynchronously trigger memtable rotation.
		if err := e.engine.ForceFlush(ctx, false); err != nil {
			return nil, fmt.Errorf("force flush memtable failed: %w", err)
		}
		return ManipulateResponse{Status: ResponseOK /*, Message: "Memtable flush to immutable list triggered."*/}, nil
	case corenbql.FlushDisk:
		// Asynchronously trigger a compaction cycle check.
		e.engine.TriggerCompaction()
		return ManipulateResponse{Status: ResponseOK /*, Message: "Compaction check triggered."*/}, nil
	case corenbql.FlushAll:
		// Synchronously flush memtable all the way to disk.
		if err := e.engine.ForceFlush(ctx, true); err != nil {
			return nil, fmt.Errorf("force flush all failed: %w", err)
		}
		return ManipulateResponse{Status: ResponseOK /*, Message: "Memtable flushed to disk successfully."*/}, nil
	default:
		return nil, fmt.Errorf("unknown flush type: %s", cmd.Type)
	}
}

// processQueryIterator drains the query iterator, collects results, and finds the last key for pagination.
func processQueryIterator(iter core.QueryResultIteratorInterface) ([]QueryResultLine, []byte, error) {
	var results []QueryResultLine
	var lastKey []byte

	for iter.Next() {
		// Get the raw key for the cursor *before* decoding the item.
		// This is crucial for pagination as the raw key is the source of truth for the cursor.
		// rawKey, _, _, _ := iter.UnderlyingAt()
		cur, err := iter.UnderlyingAt()
		if err != nil {
			return nil, nil, err
		}
		rawKey := cur.Key
		if rawKey != nil {
			// This copy is important because the underlying buffer might be reused by the iterator.
			lastKey = make([]byte, len(rawKey))
			copy(lastKey, rawKey)
		}

		item, err := iter.At()
		if err != nil {
			return nil, nil, fmt.Errorf("error retrieving query result item: %w", err)
		}

		// Convert core.QueryResultItem to nbql.QueryResultLine
		resultLine := QueryResultLine{
			Metric:          item.Metric,
			Timestamp:       item.Timestamp,
			IsAggregated:    item.IsAggregated,
			WindowStartTime: item.WindowStartTime,
		}

		// Copy maps so the returned results do not reference pooled memory
		if item.Tags != nil {
			tagsCopy := make(map[string]string, len(item.Tags))
			for k, v := range item.Tags {
				tagsCopy[k] = v
			}
			resultLine.Tags = tagsCopy
		}
		if item.AggregatedValues != nil {
			aggCopy := make(map[string]float64, len(item.AggregatedValues))
			for k, v := range item.AggregatedValues {
				aggCopy[k] = v
			}
			resultLine.AggregatedValues = aggCopy
		}
		if item.Fields != nil {
			fv := make(core.FieldValues, len(item.Fields))
			for k, v := range item.Fields {
				fv[k] = v
			}
			resultLine.Fields = fv
		}

		results = append(results, resultLine)
		iter.Put(item) // Return the item to the pool for reuse
	}

	if err := iter.Error(); err != nil {
		return nil, nil, fmt.Errorf("query iterator failed: %w", err)
	}

	return results, lastKey, nil
}

// executeQuery handles the QUERY command.
func (e *Executor) executeQuery(ctx context.Context, cmd *corenbql.QueryStatement) (interface{}, error) {
	// 1. Get the iterator stream from the engine.
	_, iter, err := e.QueryStream(ctx, cmd)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// 2. Process the iterator to get results and the last key for pagination.
	results, lastKey, err := processQueryIterator(iter)
	if err != nil {
		return nil, err
	}

	// 3. Construct the final response.
	response := QueryResponse{
		Results: results,
	}

	// If we hit the query limit, it's likely there's more data.
	// Provide the last key seen as the cursor for the next page.
	if cmd.Limit > 0 && int64(len(results)) == cmd.Limit && lastKey != nil {
		response.NextCursor = base64.StdEncoding.EncodeToString(lastKey)
	}

	// 4. Marshal the structured response to JSON.
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		// This would be an internal server error.
		return nil, fmt.Errorf("failed to marshal query response to JSON: %w", err)
	}
	return string(jsonResponse), nil
}
