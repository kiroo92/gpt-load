package proxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	app_errors "gpt-load/internal/errors"
	"gpt-load/internal/models"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
)

func (ps *ProxyServer) applyParamOverrides(bodyBytes []byte, group *models.Group) ([]byte, error) {
	if len(group.ParamOverrides) == 0 || len(bodyBytes) == 0 {
		return bodyBytes, nil
	}

	var requestData map[string]any
	if err := json.Unmarshal(bodyBytes, &requestData); err != nil {
		logrus.Warnf("failed to unmarshal request body for param override, passing through: %v", err)
		return bodyBytes, nil
	}

	for key, value := range group.ParamOverrides {
		requestData[key] = value
	}

	return json.Marshal(requestData)
}

// logUpstreamError provides a centralized way to log errors from upstream interactions.
func logUpstreamError(context string, err error) {
	if err == nil {
		return
	}
	if app_errors.IsIgnorableError(err) {
		logrus.Debugf("Ignorable upstream error in %s: %v", context, err)
	} else {
		logrus.Errorf("Upstream error in %s: %v", context, err)
	}
}

// handleGzipCompression checks for gzip encoding and decompresses the body if necessary.
func handleGzipCompression(resp *http.Response, bodyBytes []byte) []byte {
	if resp.Header.Get("Content-Encoding") == "gzip" {
		reader, gzipErr := gzip.NewReader(bytes.NewReader(bodyBytes))
		if gzipErr != nil {
			logrus.Warnf("Failed to create gzip reader for error body: %v", gzipErr)
			return bodyBytes
		}
		defer reader.Close()

		decompressedBody, readAllErr := io.ReadAll(reader)
		if readAllErr != nil {
			logrus.Warnf("Failed to decompress gzip error body: %v", readAllErr)
			return bodyBytes
		}
		return decompressedBody
	}
	return bodyBytes
}

// isEmptyContentResponse checks if a chat completion response has empty content.
// This happens when a key's TPM is exhausted mid-request - OpenAI returns 200 but
// with no content in the message. We treat this as a soft failure and retry.
func isEmptyContentResponse(body []byte) bool {
	if len(body) == 0 {
		return true
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		// Not a chat completion response format, don't treat as empty
		return false
	}

	// If no choices at all, it's empty
	if len(result.Choices) == 0 {
		return false // Could be a non-chat endpoint, don't interfere
	}

	// If content is nil or empty string, it's an empty response
	if result.Choices[0].Message.Content == nil || *result.Choices[0].Message.Content == "" {
		return true
	}

	return false
}

// extractTokenUsage parses the usage field from an OpenAI-compatible response body.
// Supports both regular JSON responses and SSE streaming responses.
// Returns (promptTokens, completionTokens). Returns (0, 0) if parsing fails.
func extractTokenUsage(body []byte) (int, int) {
	if len(body) == 0 {
		return 0, 0
	}

	// Try parsing as regular JSON first
	if p, c := parseUsageFromJSON(body); p > 0 || c > 0 {
		return p, c
	}

	// Try parsing as SSE stream - scan all data: lines for usage info
	for i := 0; i < len(body); {
		dataStart := bytes.Index(body[i:], []byte("data:"))
		if dataStart < 0 {
			break
		}
		dataStart += i + 5 // skip "data:"

		// Skip whitespace
		for dataStart < len(body) && (body[dataStart] == ' ' || body[dataStart] == '\t') {
			dataStart++
		}

		lineEnd := bytes.Index(body[dataStart:], []byte("\n"))
		if lineEnd < 0 {
			lineEnd = len(body) - dataStart
		}

		line := body[dataStart : dataStart+lineEnd]
		i = dataStart + lineEnd + 1

		if p, c := parseUsageFromJSON(line); p > 0 || c > 0 {
			return p, c
		}
	}

	return 0, 0
}

// parseUsageFromJSON tries to extract token usage from a single JSON object.
func parseUsageFromJSON(data []byte) (int, int) {
	var result struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
		} `json:"usage"`
		Response *struct {
			Usage struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
				PromptTokens int `json:"prompt_tokens"`
				OutputTotal  int `json:"completion_tokens"`
			} `json:"usage"`
		} `json:"response"`
	}

	if err := json.Unmarshal(data, &result); err != nil {
		return 0, 0
	}

	if result.Usage != nil {
		if result.Usage.PromptTokens > 0 || result.Usage.CompletionTokens > 0 {
			return result.Usage.PromptTokens, result.Usage.CompletionTokens
		}
		if result.Usage.InputTokens > 0 || result.Usage.OutputTokens > 0 {
			return result.Usage.InputTokens, result.Usage.OutputTokens
		}
	}

	if result.Response != nil {
		u := result.Response.Usage
		if u.PromptTokens > 0 || u.OutputTotal > 0 {
			return u.PromptTokens, u.OutputTotal
		}
		if u.InputTokens > 0 || u.OutputTokens > 0 {
			return u.InputTokens, u.OutputTokens
		}
	}

	return 0, 0
}

// peekStreamForError reads up to maxBytes from the response body to inspect for inline errors.
// Returns the read bytes; the caller must prepend them when forwarding to the client.
func peekStreamForError(body io.Reader, maxBytes int) ([]byte, error) {
	buf := make([]byte, maxBytes)
	n, err := io.ReadFull(body, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// Stream ended before maxBytes - that's fine, return what we got
		return buf[:n], nil
	}
	if err != nil {
		return buf[:n], err
	}
	return buf[:n], nil
}

// containsStreamError checks if the SSE stream chunk contains an error event.
func containsStreamError(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	// OpenAI Responses API: "event: error" or "type":"error"
	if bytes.Contains(data, []byte("event: error")) ||
		bytes.Contains(data, []byte("event:error")) {
		return true
	}
	// Look for inline error payloads
	if bytes.Contains(data, []byte(`"type":"error"`)) {
		return true
	}
	if bytes.Contains(data, []byte(`"status":"failed"`)) {
		return true
	}
	if bytes.Contains(data, []byte(`"code":"rate_limit_exceeded"`)) {
		return true
	}
	return false
}

// extractStreamErrorMessage tries to extract an error message from an SSE error event.
func extractStreamErrorMessage(data []byte) string {
	// Scan SSE data: lines and look for error.message
	for i := 0; i < len(data); {
		dataStart := bytes.Index(data[i:], []byte("data:"))
		if dataStart < 0 {
			break
		}
		dataStart += i + 5

		for dataStart < len(data) && (data[dataStart] == ' ' || data[dataStart] == '\t') {
			dataStart++
		}

		lineEnd := bytes.Index(data[dataStart:], []byte("\n"))
		if lineEnd < 0 {
			lineEnd = len(data) - dataStart
		}

		line := data[dataStart : dataStart+lineEnd]
		i = dataStart + lineEnd + 1

		var parsed struct {
			Type  string `json:"type"`
			Error struct {
				Message string `json:"message"`
				Code    string `json:"code"`
			} `json:"error"`
			Response struct {
				Error struct {
					Message string `json:"message"`
					Code    string `json:"code"`
				} `json:"error"`
			} `json:"response"`
		}
		if err := json.Unmarshal(line, &parsed); err != nil {
			continue
		}

		if parsed.Error.Message != "" {
			return parsed.Error.Message
		}
		if parsed.Response.Error.Message != "" {
			return parsed.Response.Error.Message
		}
	}
	return "stream returned error event"
}
