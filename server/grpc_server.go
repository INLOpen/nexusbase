package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/auth"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/sstable"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// GRPCServer wraps the grpc.Server and implements the TSDBService.
type GRPCServer struct {
	tsdb.UnimplementedTSDBServiceServer
	engine        engine2.StorageEngineExternal
	server        *grpc.Server
	healthSrv     *health.Server
	logger        *slog.Logger
	authenticator core.IAuthenticator
	putWorker     *WorkerPool
	batchWorker   *WorkerPool
}

// NewGRPCServer creates and configures a new gRPC server instance.
// It handles TLS, authentication, and service registration.
func NewGRPCServer(eng engine2.StorageEngineExternal, putPool *WorkerPool, batchPool *WorkerPool, cfg *config.ServerConfig, authenticator core.IAuthenticator, logger *slog.Logger) (*GRPCServer, error) {
	s := &GRPCServer{
		engine:        eng,
		logger:        logger.With("component", "GRPCServer"),
		authenticator: authenticator,
		healthSrv:     health.NewServer(),
		putWorker:     putPool,
		batchWorker:   batchPool,
	}

	var opts []grpc.ServerOption
	if cfg.TLS.Enabled {
		creds, err := s.loadTLSCredentials(&cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("could not load TLS credentials: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
		s.logger.Info("gRPC server initialized with TLS.")
	} else {
		s.logger.Info("gRPC server initialized without TLS (insecure).")
	}

	// Add interceptors if security is enabled

	opts = append(opts, grpc.ChainUnaryInterceptor(s.authenticator.UnaryInterceptor))
	opts = append(opts, grpc.ChainStreamInterceptor(s.authenticator.StreamInterceptor))
	s.logger.Info("Authentication interceptors enabled for gRPC server.")

	s.server = grpc.NewServer(opts...)
	tsdb.RegisterTSDBServiceServer(s.server, s)
	grpc_health_v1.RegisterHealthServer(s.server, s.healthSrv)
	reflection.Register(s.server)

	return s, nil
}

// Start begins listening for gRPC requests.
func (s *GRPCServer) Start(lis net.Listener) error {
	s.logger.Info("gRPC server listening", "address", lis.Addr().String())
	return s.server.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *GRPCServer) Stop() {
	s.logger.Info("Stopping gRPC server...")
	if s.healthSrv != nil {
		s.healthSrv.Shutdown()
	}
	if s.server != nil {
		s.server.GracefulStop()
	}
	s.logger.Info("gRPC server stopped.")
}

func (s *GRPCServer) loadTLSCredentials(cfg *config.TLSConfig) (credentials.TransportCredentials, error) {
	serverCert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.NoClientCert,
	}

	// Optional: Configure mTLS if a client CA is provided
	/*if cfg.ClientCAFile != "" {
		caCert, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append client CA certs")
		}
		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		s.logger.Info("mTLS enabled for gRPC server.")
	}*/

	return credentials.NewTLS(tlsConfig), nil
}

// Put handles the gRPC request to store a single data point.
func (s *GRPCServer) Put(ctx context.Context, req *tsdb.PutRequest) (*tsdb.PutResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}

	// Convert protobuf struct to core.FieldValues directly to avoid
	// the allocation-heavy intermediate map[string]interface{} from AsMap().
	fieldValues, err := convertProtoStructToFieldValues(req.GetFields())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid field values: %v", err)
	}
	if len(fieldValues) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "fields cannot be empty")
	}

	point := core.DataPoint{
		Metric:    req.GetMetric(),
		Tags:      req.GetTags(),
		Timestamp: req.GetTimestamp(),
		Fields:    fieldValues,
	}

	job := Job{
		Ctx:      ctx,
		Data:     []core.DataPoint{point},
		ResultCh: make(chan error, 1),
	}
	s.putWorker.jobQueue <- job

	err = <-job.ResultCh
	if err != nil {
		var validationErr *core.ValidationError
		if errors.As(err, &validationErr) {
			s.logger.Warn("Invalid data point received in Put request", "error", err)
			return nil, status.Errorf(codes.InvalidArgument, "invalid data point: %v", err)
		}
		s.logger.Error("Internal error during Put operation", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to put data point")
	}

	return &tsdb.PutResponse{}, nil
}

// PutBatch handles the gRPC request to store a batch of data points.
func (s *GRPCServer) PutBatch(ctx context.Context, req *tsdb.PutBatchRequest) (*tsdb.PutBatchResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}

	points := make([]core.DataPoint, len(req.GetPoints()))
	for i, p := range req.GetPoints() {
		// OPTIMIZATION: The original code used `p.GetFields().AsMap()`, which is
		// known to be allocation-heavy. A direct conversion is more efficient.
		fieldValues, err := convertProtoStructToFieldValues(p.GetFields())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid field values: %v", err)
		}
		if len(fieldValues) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "fields cannot be empty")
		}

		points[i] = core.DataPoint{
			Metric:    p.GetMetric(), // Metric is defined at the batch level
			Tags:      p.GetTags(),
			Timestamp: p.GetTimestamp(), // Timestamp is also at the batch level
			Fields:    fieldValues,
		}
	}

	job := Job{
		Ctx:      ctx,
		Data:     points,
		ResultCh: make(chan error, 1),
	}

	s.batchWorker.jobQueue <- job

	err := <-job.ResultCh
	if err != nil {
		var validationErr *core.ValidationError
		if errors.As(err, &validationErr) {
			s.logger.Warn("Invalid data point received in PutBatch request", "error", err)
			return nil, status.Errorf(codes.InvalidArgument, "invalid data point in batch: %v", err)
		}
		s.logger.Error("Internal error during PutBatch operation", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to process batch")
	}
	return &tsdb.PutBatchResponse{}, nil
}

// convertProtoStructToFieldValues converts a *structpb.Struct to core.FieldValues
// without creating an intermediate map[string]interface{}, which is more memory-efficient
// than using `s.AsMap()`.
func convertProtoStructToFieldValues(s *structpb.Struct) (core.FieldValues, error) {
	if s == nil || s.Fields == nil {
		return nil, nil
	}

	fieldValues := make(core.FieldValues, len(s.Fields))
	for key, protoVal := range s.Fields {
		if protoVal == nil {
			continue
		}

		// AsInterface() converts the protobuf Value to its natural Go representation.
		// This is more efficient than creating a full map with AsMap().
		nativeVal := protoVal.AsInterface()

		// We can't accept nil values in our fields.
		if nativeVal == nil {
			continue
		}

		// Create the internal PointValue from the native value.
		// This reuses the existing logic for handling different data types.
		pointVal, err := core.NewPointValue(nativeVal)
		if err != nil {
			return nil, fmt.Errorf("could not convert field '%s': %w", key, err)
		}
		fieldValues[key] = pointVal
	}

	return fieldValues, nil
}

// Get handles the gRPC request to retrieve a single data point.
func (s *GRPCServer) Get(ctx context.Context, req *tsdb.GetRequest) (*tsdb.GetResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleReader); err != nil {
		return nil, err
	}
	val, err := s.engine.Get(ctx, req.GetMetric(), req.GetTags(), req.GetTimestamp())
	if err != nil {
		if err == sstable.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "data point not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get data point: %v", err)
	}

	// Convert core.FieldValues back to google.protobuf.Struct for the response
	fieldsStruct, err := structpb.NewStruct(val.ToMap())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert response fields to protobuf struct: %v", err)
	}
	return &tsdb.GetResponse{Fields: fieldsStruct}, nil
}

// Delete handles the gRPC request to delete a single data point.
func (s *GRPCServer) Delete(ctx context.Context, req *tsdb.DeleteRequest) (*tsdb.DeleteResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}
	if err := s.engine.Delete(ctx, req.GetMetric(), req.GetTags(), req.GetTimestamp()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete data point: %v", err)
	}
	return &tsdb.DeleteResponse{}, nil
}

// DeleteSeries handles the gRPC request to delete an entire series.
func (s *GRPCServer) DeleteSeries(ctx context.Context, req *tsdb.DeleteSeriesRequest) (*tsdb.DeleteSeriesResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}
	if err := s.engine.DeleteSeries(ctx, req.GetMetric(), req.GetTags()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete series: %v", err)
	}
	return &tsdb.DeleteSeriesResponse{}, nil
}

// DeletesByTimeRange handles the gRPC request to delete a range of data points.
func (s *GRPCServer) DeletesByTimeRange(ctx context.Context, req *tsdb.DeletesByTimeRangeRequest) (*tsdb.DeletesByTimeRangeResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}
	if err := s.engine.DeletesByTimeRange(ctx, req.GetMetric(), req.GetTags(), req.GetStartTime(), req.GetEndTime()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete by time range: %v", err)
	}
	return &tsdb.DeletesByTimeRangeResponse{}, nil
}

var mapAggregationSpec2Core = map[tsdb.AggregationSpec_AggregationFunc]core.AggregationFunc{
	tsdb.AggregationSpec_SUM:     core.AggSum,
	tsdb.AggregationSpec_COUNT:   core.AggCount,
	tsdb.AggregationSpec_AVERAGE: core.AggAvg,
	tsdb.AggregationSpec_MIN:     core.AggMin,
	tsdb.AggregationSpec_MAX:     core.AggMax,
	tsdb.AggregationSpec_FIRST:   core.AggFirst,
	tsdb.AggregationSpec_LAST:    core.AggLast,
}

// Query handles the gRPC request to query data.
func (s *GRPCServer) Query(req *tsdb.QueryRequest, stream tsdb.TSDBService_QueryServer) error {
	if err := s.authenticator.Authorize(stream.Context(), auth.RoleReader); err != nil {
		return err
	}
	ctx := stream.Context()

	// Convert protobuf AggregationSpec to core AggregationSpec
	coreAggSpecs := make([]core.AggregationSpec, 0, len(req.GetAggregationSpecs()))
	for _, spec := range req.GetAggregationSpecs() {
		if spec != nil {
			funcAgg, ok := mapAggregationSpec2Core[spec.GetFunction()]
			if !ok {
				return status.Errorf(codes.InvalidArgument, "invalid aggregation function: %v", spec.GetFunction())
			}
			coreAggSpecs = append(coreAggSpecs, core.AggregationSpec{
				Function: funcAgg,
				Field:    spec.GetField(),
			})
		}
	}

	queryParams := core.QueryParams{
		Metric:             req.GetMetric(),
		StartTime:          req.GetStartTime(),
		EndTime:            req.GetEndTime(),
		Tags:               req.GetTags(),
		AggregationSpecs:   coreAggSpecs,
		DownsampleInterval: req.GetDownsampleInterval(),
		EmitEmptyWindows:   req.GetEmitEmptyWindows(),
		Limit:              req.GetLimit(),
	}

	iter, err := s.engine.Query(ctx, queryParams)
	if err != nil {
		s.logger.Error("Storage engine Query failed", "error", err)
		return status.Errorf(codes.Internal, "failed to execute query: %v", err)
	}
	defer iter.Close()

	for iter.Next() {
		item, err := iter.At()
		if err != nil {
			s.logger.Error("Error getting item from query iterator", "error", err)
			return status.Errorf(codes.Internal, "error processing query result: %v", err)
		}

		var fields *structpb.Struct
		if item.Fields != nil {
			fields, err = structpb.NewStruct(item.Fields.ToMap())
			if err != nil {
				s.logger.Warn("Failed to convert fields map to protobuf struct", "error", err)
			}
		}

		// Copy tags and aggregated values so we don't reference pooled memory
		var tagsCopy map[string]string
		if item.Tags != nil {
			tagsCopy = make(map[string]string, len(item.Tags))
			for k, v := range item.Tags {
				tagsCopy[k] = v
			}
		}
		var aggCopy map[string]float64
		if item.AggregatedValues != nil {
			aggCopy = make(map[string]float64, len(item.AggregatedValues))
			for k, v := range item.AggregatedValues {
				aggCopy[k] = v
			}
		}

		result := &tsdb.QueryResult{
			Metric:           item.Metric,
			Tags:             tagsCopy,
			IsAggregated:     item.IsAggregated,
			Timestamp:        item.Timestamp,
			Fields:           fields,
			WindowStartTime:  item.WindowStartTime,
			WindowEndTime:    item.WindowEndTime,
			AggregatedValues: aggCopy,
		}
		iter.Put(item) // Return item to pool

		if err := stream.Send(result); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		s.logger.Error("Query iterator finished with an error", "error", err)
		return status.Errorf(codes.Internal, "query iteration failed: %v", err)
	}

	return nil
}

// ForceFlush handles the gRPC request to manually trigger a memtable flush.
func (s *GRPCServer) ForceFlush(ctx context.Context, req *tsdb.ForceFlushRequest) (*tsdb.ForceFlushResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}

	if err := s.engine.ForceFlush(ctx, true); err != nil {
		s.logger.Error("Storage engine ForceFlush failed", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to force flush: %v", err)
	}

	s.logger.Info("Memtable flush successfully triggered via gRPC.")
	return &tsdb.ForceFlushResponse{}, nil
}

// CreateSnapshot handles the gRPC request to create a snapshot.
func (s *GRPCServer) CreateSnapshot(ctx context.Context, req *tsdb.CreateSnapshotRequest) (*tsdb.CreateSnapshotResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleWriter); err != nil {
		return nil, err
	}
	// The snapshot directory is now generated by the engine. The request parameter is ignored.
	// A future improvement could be to allow an optional prefix or name from the request.
	_, err := s.engine.CreateSnapshot(ctx)
	if err != nil {
		s.logger.Error("Storage engine CreateSnapshot failed", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}
	// The response could be updated to return the path of the created snapshot.
	// For now, we assume the response is empty but could be extended to return snapshotPath.
	return &tsdb.CreateSnapshotResponse{}, nil
}

// GetSeriesByTags handles the gRPC request to find series by tags.
func (s *GRPCServer) GetSeriesByTags(ctx context.Context, req *tsdb.GetSeriesByTagsRequest) (*tsdb.GetSeriesByTagsResponse, error) {
	if err := s.authenticator.Authorize(ctx, auth.RoleReader); err != nil {
		return nil, err
	}
	seriesKeys, err := s.engine.GetSeriesByTags(req.GetMetric(), req.GetTags())
	if err != nil {
		s.logger.Error("Storage engine GetSeriesByTags failed", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to get series by tags: %v", err)
	}
	return &tsdb.GetSeriesByTagsResponse{SeriesKeys: seriesKeys}, nil
}

// Subscribe handles the gRPC request to subscribe to real-time updates.
func (s *GRPCServer) Subscribe(req *tsdb.SubscribeRequest, stream tsdb.TSDBService_SubscribeServer) error {
	if err := s.authenticator.Authorize(stream.Context(), auth.RoleReader); err != nil {
		return err
	}
	ctx := stream.Context()

	filter := engine2.SubscriptionFilter{
		Metric: req.GetMetric(),
		Tags:   req.GetTags(),
	}

	pubsub, err := s.engine.GetPubSub()
	if err != nil {
		s.logger.Error("Failed to get PubSub instance", "error", err)
		return status.Errorf(codes.Internal, "failed to get PubSub instance: %v", err)
	}
	sub := pubsub.Subscribe(filter)
	defer sub.Close()

	for {
		select {
		case update, ok := <-sub.Updates:
			if !ok {
				s.logger.Info("Subscription channel closed.", "sub_id", sub.ID)
				return nil
			}
			if err := stream.Send(update); err != nil {
				s.logger.Warn("Failed to send update to subscriber.", "sub_id", sub.ID, "error", err)
				return err
			}
		case <-ctx.Done():
			s.logger.Info("Subscription context done, closing stream.", "sub_id", sub.ID, "error", ctx.Err())
			return ctx.Err()
		}
	}
}
