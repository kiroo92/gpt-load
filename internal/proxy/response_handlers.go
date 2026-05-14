package proxy

import (
	"bufio"
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// handleStreamingResponse forwards the streaming response to the client.
// While forwarding, it scans SSE events to extract usage info and capture key events
// for logging. Returns the captured/distilled body for logging purposes.
func (ps *ProxyServer) handleStreamingResponse(c *gin.Context, resp *http.Response) []byte {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		logrus.Error("Streaming unsupported by the writer, falling back to normal response")
		ps.handleNormalResponse(c, resp)
		return nil
	}

	const maxCaptureSize = 32768 // Cap captured non-delta events to 32KB

	// We capture only meaningful events (skip delta events that just contain partial text)
	// and always keep the last event (which usually contains usage info)
	captured := make([]byte, 0, 4096)
	var lastEvent []byte

	// Use a line-buffered reader to scan SSE events
	reader := bufio.NewReaderSize(resp.Body, 16*1024)

	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			// Forward to client immediately
			if _, writeErr := c.Writer.Write(line); writeErr != nil {
				logUpstreamError("writing stream to client", writeErr)
				return appendCaptured(captured, lastEvent)
			}
			flusher.Flush()

			// Capture for logging - keep meaningful events
			if shouldCaptureSSELine(line) {
				if len(captured) < maxCaptureSize {
					captured = append(captured, line...)
				}
				// Always remember the most recent meaningful event for usage extraction
				lastEvent = make([]byte, len(line))
				copy(lastEvent, line)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			logUpstreamError("reading from upstream", err)
			return appendCaptured(captured, lastEvent)
		}
	}

	return appendCaptured(captured, lastEvent)
}

// shouldCaptureSSELine returns true if the SSE line is worth keeping for logging.
// Skip output_text.delta events (they're just partial text fragments).
func shouldCaptureSSELine(line []byte) bool {
	// Always keep event: lines and empty lines (separators)
	if len(line) <= 1 {
		return true
	}
	// Filter out delta events which are just text fragments
	if bytes.Contains(line, []byte(`"type":"response.output_text.delta"`)) {
		return false
	}
	if bytes.Contains(line, []byte(`"object":"chat.completion.chunk"`)) {
		// For chat completion streams, only keep chunks that have usage or finish_reason
		if !bytes.Contains(line, []byte(`"usage"`)) && !bytes.Contains(line, []byte(`"finish_reason"`)) {
			return false
		}
	}
	return true
}

// appendCaptured appends the last event to the captured buffer if not already there.
func appendCaptured(captured, lastEvent []byte) []byte {
	if len(lastEvent) == 0 {
		return captured
	}
	// If last event is already at the end, don't duplicate
	if len(captured) >= len(lastEvent) && bytes.Equal(captured[len(captured)-len(lastEvent):], lastEvent) {
		return captured
	}
	// Append separator and last event
	captured = append(captured, '\n')
	captured = append(captured, lastEvent...)
	return captured
}

func (ps *ProxyServer) handleNormalResponse(c *gin.Context, resp *http.Response) {
	if _, err := io.Copy(c.Writer, resp.Body); err != nil {
		logUpstreamError("copying response body", err)
	}
}

// writeStreamError writes an error in SSE format for streaming requests.
// This ensures streaming clients (ChatGPT frontends, Cursor, etc.) can properly
// display the error instead of showing an empty response.
func (ps *ProxyServer) writeStreamError(c *gin.Context, statusCode int, errorMessage string) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(statusCode)

	// Write error in OpenAI-compatible SSE format
	errorData := `{"error":{"message":"` + escapeJSON(errorMessage) + `","type":"server_error","code":"rate_limit_exceeded"}}`
	c.Writer.WriteString("data: " + errorData + "\n\n")
	c.Writer.WriteString("data: [DONE]\n\n")

	if flusher, ok := c.Writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

// escapeJSON escapes special characters for JSON string embedding.
func escapeJSON(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			result = append(result, '\\', '"')
		case '\\':
			result = append(result, '\\', '\\')
		case '\n':
			result = append(result, '\\', 'n')
		case '\r':
			result = append(result, '\\', 'r')
		case '\t':
			result = append(result, '\\', 't')
		default:
			result = append(result, s[i])
		}
	}
	return string(result)
}
