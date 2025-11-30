package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/core"

	corenbql "github.com/INLOpen/nexuscore/nbql"
)

// TCPConnectionHandler encapsulates the logic for handling a single TCP connection's
// protocol, including authentication and command processing. It is designed to be
// reused by different TCP server implementations.
type TCPConnectionHandler struct {
	executor      *nbql.Executor
	authenticator core.IAuthenticator
	logger        *slog.Logger
}

// NewTCPConnectionHandler creates a new handler for TCP connections.
func NewTCPConnectionHandler(executor *nbql.Executor, authenticator core.IAuthenticator, logger *slog.Logger) *TCPConnectionHandler {
	return &TCPConnectionHandler{
		executor:      executor,
		authenticator: authenticator,
		logger:        logger,
	}
}

// HandleAuthentication performs the initial handshake and authentication for a new TCP connection.
// It returns true if authentication is successful, otherwise false.
func (h *TCPConnectionHandler) HandleAuthentication(ctx context.Context, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) bool {
	// Helper to send a response and log errors.
	sendResponse := func(status ResponseOp, message string) {
		responsePacket := &AuthenticationPacket{
			Version: 1, // Use a consistent version for responses
			Op:      ConnectResponseAuthenticationOp,
			Payload: &ResponseAuthenticationPacket{
				Status:  status,
				Message: message,
			},
		}
		respData, err := responsePacket.MarshalBinary()
		if err != nil {
			h.logger.Error("Failed to marshal auth response", "error", err)
			return
		}
		if _, err := writer.Write(respData); err != nil {
			h.logger.Warn("Failed to write auth response to client", "error", err)
		}
		if err := writer.Flush(); err != nil {
			h.logger.Warn("Failed to flush auth response to client", "error", err)
		}
	}

	// 1. Read the fixed-size header to determine payload length.
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		if !errors.Is(err, io.EOF) {
			h.logger.Warn("Failed to read auth packet header", "remote_addr", conn.RemoteAddr(), "error", err)
		}
		return false // Don't send response, client likely disconnected.
	}

	// 2. Unpack header fields.
	op := ConnectOp(header[1])
	payloadLen := binary.BigEndian.Uint16(header[2:4])

	if op != ConnectRequestAuthenticationOp {
		h.logger.Warn("Received unexpected operation code during auth", "op", op)
		sendResponse(ResponseError, "Invalid operation during authentication")
		return false
	}

	// 3. Read the payload based on the length from the header.
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(reader, payload); err != nil {
			if !errors.Is(err, io.EOF) {
				h.logger.Warn("Failed to read auth packet payload", "error", err)
			}
			return false
		}
	}

	// 4. Unmarshal the payload into a RequestAuthenticationPacket.
	reqPayload := &RequestAuthenticationPacket{}
	if err := reqPayload.UnmarshalBinary(payload); err != nil {
		h.logger.Error("Failed to unmarshal auth request payload", "error", err)
		sendResponse(ResponseError, "Invalid authentication payload format")
		return false
	}

	// 5. Perform authentication.
	if err := h.authenticator.AuthenticateUserPass(reqPayload.Username, reqPayload.Password); err != nil {
		h.logger.Warn("TCP authentication failed", "username", reqPayload.Username, "error", err)
		sendResponse(ResponseError, "Invalid username or password")
		return false
	}

	// 6. Authentication successful.
	h.logger.Info("TCP authentication successful", "username", reqPayload.Username)
	sendResponse(ResponseOK, "Authentication successful")
	return true
}

// HandleCommand parses and executes a command received from the client.
func (h *TCPConnectionHandler) HandleCommand(ctx context.Context, w io.Writer, cmdType nbql.CommandType, payload []byte) {
	var stmt corenbql.Command
	var err error

	switch cmdType {
	// Fast-path for high-throughput writes. This bypasses the string parser.
	case nbql.CommandPush:
		req, decErr := nbql.DecodePushRequest(bytes.NewReader(payload))
		if decErr != nil {
			err = fmt.Errorf("failed to decode PUSH request: %w", decErr)
			break
		}
		stmt = &corenbql.PushStatement{
			Metric:    req.Metric,
			Tags:      req.Tags,
			Timestamp: req.Timestamp,
			Fields:    req.Fields.ToMap(),
		}
	case nbql.CommandPushs:
		req, decErr := nbql.DecodePushsRequest(bytes.NewReader(payload))
		if decErr != nil {
			err = fmt.Errorf("failed to decode PUSHS request: %w", decErr)
			break
		}
		points := make([]corenbql.PushStatement, len(req.Items))
		for i, item := range req.Items {
			points[i] = corenbql.PushStatement{
				Metric:    item.Metric,
				Tags:      item.Tags,
				Timestamp: item.Timestamp,
				Fields:    item.Fields.ToMap(),
			}
		}
		stmt = &corenbql.PushsStatement{
			Items: points,
		}
	case nbql.CommandQuery:
		// Flexible path for complex queries.
		req, decErr := nbql.DecodeQueryRequest(bytes.NewReader(payload))
		if decErr != nil {
			err = fmt.Errorf("failed to decode QUERY request: %w", decErr)
			break
		}
		stmt, err = corenbql.Parse(req.QueryString)

	default:
		err = fmt.Errorf("received unknown command type: %v", cmdType)
	}

	if err != nil {
		h.logger.Error("Command decoding/parsing error", "error", err)
		h.writeErrorFrame(w, "command handling error", err)
		return
	}

	// --- Execute and handle response ---
	if queryStmt, isQuery := stmt.(*corenbql.QueryStatement); isQuery {
		// Handle streaming query results
		h.handleStreamedQuery(ctx, w, queryStmt)
	} else {
		buf := core.BufferPool.Get()
		defer core.BufferPool.Put(buf)
		// Handle single-response manipulation commands
		res, execErr := h.executor.Execute(ctx, stmt)
		if execErr != nil {
			h.logger.Error("Executor failed", "error", execErr)
			h.writeErrorFrame(w, "executor failed", execErr) // writeErrorFrame also uses the pool now
			return
		}

		if manipResp, ok := res.(nbql.ManipulateResponse); ok {
			nbql.EncodeManipulateResponse(buf, manipResp)
			nbql.WriteFrame(w, nbql.CommandManipulate, buf.Bytes())
		} else {
			h.writeErrorFrame(w, "response handling error", fmt.Errorf("unexpected response type %T for manipulation command", res))
		}
	}
	h.logger.Debug("Command handled successfully")
}

func (h *TCPConnectionHandler) handleStreamedQuery(ctx context.Context, w io.Writer, stmt *corenbql.QueryStatement) {
	queryParams, iter, err := h.executor.QueryStream(ctx, stmt)
	if err != nil {
		h.logger.Error("Failed to create query iterator", "error", err)
		h.writeErrorFrame(w, "query initialization failed", err)
		return
	}
	defer iter.Close()

	// Construct the appropriate response based on whether the data is aggregated.
	var flags nbql.PointItemFlag
	if len(queryParams.AggregationSpecs) > 0 || queryParams.DownsampleInterval != "" {
		flags |= nbql.PointItemFlagIsAggregated
	}

	var rowCount uint64
	for iter.Next() {
		item, err := iter.At()
		if err != nil {
			h.logger.Error("Failed to get item from iterator", "error", err)
			h.writeErrorFrame(w, "query iteration failed", err)
			return
		}

		// Copy tags/fields/aggregates out of the pooled item before encoding
		var tagsCopy map[string]string
		if item.Tags != nil {
			tagsCopy = make(map[string]string, len(item.Tags))
			for k, v := range item.Tags {
				tagsCopy[k] = v
			}
		}

		var fieldsCopy core.FieldValues
		if item.Fields != nil {
			fieldsCopy = make(core.FieldValues, len(item.Fields))
			for k, v := range item.Fields {
				fieldsCopy[k] = v
			}
		}

		var aggCopy map[string]float64
		if item.AggregatedValues != nil {
			aggCopy = make(map[string]float64, len(item.AggregatedValues))
			for k, v := range item.AggregatedValues {
				aggCopy[k] = v
			}
		}

		// Send one row at a time using the copies so we don't encode pooled maps
		buf := core.BufferPool.Get()
		nbql.EncodeQueryResponse(buf, nbql.QueryResponse{
			Status: nbql.ResponseDataRow,
			Flags:  flags,
			Results: []nbql.QueryResultLine{
				{
					// TODO: Implement SequenceID when available from the engine.
					// SequenceID: item.SequenceID,
					Tags:             tagsCopy,
					Timestamp:        item.Timestamp,
					Fields:           fieldsCopy,
					AggregatedValues: aggCopy,
				},
			},
		})
		nbql.WriteFrame(w, nbql.CommandQueryResultPart, buf.Bytes())
		core.BufferPool.Put(buf) // Return buffer to pool immediately after use in loop
		rowCount++
		// Return item to the pool after it has been encoded and sent.
		iter.Put(item)
	}

	// Check for iterator errors after the loop
	if err := iter.Error(); err != nil {
		h.logger.Error("Query iterator finished with an error", "error", err)
		h.writeErrorFrame(w, "query iteration failed", err)
		return
	}

	// Send the end-of-stream marker
	endBuf := core.BufferPool.Get()
	defer core.BufferPool.Put(endBuf)
	nbql.EncodeQueryEndResponse(endBuf, nbql.QueryEndResponse{TotalRows: rowCount, Status: nbql.ResponseDataEnd})
	nbql.WriteFrame(w, nbql.CommandQueryEnd, endBuf.Bytes())
}

func (h *TCPConnectionHandler) writeErrorFrame(w io.Writer, context string, err error) {
	buf := core.BufferPool.Get()
	defer core.BufferPool.Put(buf)
	errMsg := fmt.Sprintf("%s: %v", context, err)
	nbql.EncodeErrorMessage(buf, &nbql.ErrorMessage{Message: errMsg})
	// We ignore the error from WriteFrame here as the connection might already be broken.
	// The primary goal is to try to inform the client.
	_ = nbql.WriteFrame(w, nbql.CommandError, buf.Bytes())
}
