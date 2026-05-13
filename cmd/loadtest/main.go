package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	baseURL     = "http://162.243.117.81:8080/v1/chat/completions"
	apiKey      = "sk-ef7d32113a574f5c6abdd2209dde0f167c2700cfb0a285529596a42954f7eb5a"
	concurrency = 10
	duration    = 20 * time.Second
)

func main() {
	body := map[string]any{
		"model": "gpt-5.4-mini",
		"messages": []map[string]string{
			{"role": "user", "content": "Explain the difference between TCP and UDP in detail with examples. Write at least 3 paragraphs."},
		},
		"max_tokens": 500,
		"stream":     false,
	}
	bodyBytes, _ := json.Marshal(body)

	var success, fail429, failOther, emptyContent, totalReqs, totalDuration int64

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	start := time.Now()

	// Print progress every 30 seconds
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(start).Seconds()
				total := atomic.LoadInt64(&totalReqs)
				s := atomic.LoadInt64(&success)
				r := atomic.LoadInt64(&fail429)
				o := atomic.LoadInt64(&failOther)
				e := atomic.LoadInt64(&emptyContent)
				fmt.Printf("[%.0fs] reqs=%d success=%d(empty=%d) 429=%d other=%d rate=%.1f/s\n", elapsed, total, s, e, r, o, float64(total)/elapsed)
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		sem <- struct{}{}
		wg.Add(1)
		idx := atomic.AddInt64(&totalReqs, 1)
		go func(idx int64) {
			defer wg.Done()
			defer func() { <-sem }()

			reqStart := time.Now()
			req, _ := http.NewRequestWithContext(ctx, "POST", baseURL, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+apiKey)

			client := &http.Client{Timeout: 120 * time.Second}
			resp, err := client.Do(req)
			dur := time.Since(reqStart).Milliseconds()
			atomic.AddInt64(&totalDuration, dur)

			if err != nil {
				atomic.AddInt64(&failOther, 1)
				return
			}
			defer resp.Body.Close()
			respBody, _ := io.ReadAll(resp.Body)

			switch {
			case resp.StatusCode == 200:
				atomic.AddInt64(&success, 1)
				content := extractContent(respBody)
				usage := extractUsage(respBody)
				if content == "" {
					atomic.AddInt64(&emptyContent, 1)
					fmt.Printf("[%d] 200 EMPTY (%dms) usage=%s raw: %.400s\n", idx, dur, usage, string(respBody))
				} else {
					fmt.Printf("[%d] 200 OK (%dms) len=%d usage=%s\n", idx, dur, len(content), usage)
				}
			case resp.StatusCode == 429:
				atomic.AddInt64(&fail429, 1)
			default:
				atomic.AddInt64(&failOther, 1)
				fmt.Printf("[%d] %d (%dms): %.100s\n", idx, resp.StatusCode, dur, string(respBody))
			}
		}(idx)
	}

done:
	wg.Wait()
	elapsed := time.Since(start)
	total := atomic.LoadInt64(&totalReqs)

	fmt.Printf("\n========== RESULTS (%.0fs run) ==========\n", elapsed.Seconds())
	fmt.Printf("Total requests:  %d\n", total)
	fmt.Printf("Concurrency:     %d\n", concurrency)
	fmt.Printf("Success (200):   %d\n", success)
	fmt.Printf("Empty content:   %d\n", emptyContent)
	fmt.Printf("Rate limited:    %d\n", fail429)
	fmt.Printf("Other errors:    %d\n", failOther)
	if total > 0 {
		fmt.Printf("Avg latency:     %dms\n", totalDuration/total)
		fmt.Printf("Success rate:    %.1f%%\n", float64(success)/float64(total)*100)
		fmt.Printf("RPS:             %.1f\n", float64(total)/elapsed.Seconds())
	}
}

func extractContent(body []byte) string {
	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return ""
	}
	if len(result.Choices) == 0 {
		return ""
	}
	return result.Choices[0].Message.Content
}

func extractUsage(body []byte) string {
	var result struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "parse_err"
	}
	return fmt.Sprintf("in=%d out=%d total=%d", result.Usage.PromptTokens, result.Usage.CompletionTokens, result.Usage.TotalTokens)
}
