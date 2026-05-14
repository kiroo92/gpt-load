package proxy

import (
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// handleStreamingResponse forwards the streaming response to the client.
// Returns the captured stream content for logging purposes (capped to avoid memory issues).
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

	const maxCaptureSize = 65536 // Cap captured body to 64KB
	captured := make([]byte, 0, 8192)

	buf := make([]byte, 4*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := c.Writer.Write(buf[:n]); writeErr != nil {
				logUpstreamError("writing stream to client", writeErr)
				return captured
			}
			flusher.Flush()
			// Capture content for logging (with size cap)
			if len(captured) < maxCaptureSize {
				remaining := maxCaptureSize - len(captured)
				toCopy := n
				if toCopy > remaining {
					toCopy = remaining
				}
				captured = append(captured, buf[:toCopy]...)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			logUpstreamError("reading from upstream", err)
			return captured
		}
	}
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
