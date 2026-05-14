package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

func main() {
	url := "http://162.243.117.81:3001/proxy/openai/v1/responses"
	apiKey := "sk-zBYUC6Hq17f3D211c93eT3BlBKFJEF22bec38EdF4dFFbB22"

	body := map[string]any{
		"model": "gpt-5.4-mini",
		"input": []map[string]string{
			{"role": "user", "content": "Say hi"},
		},
		"max_output_tokens": 50,
		"stream":            true,
	}
	bodyBytes, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println("Status:", resp.StatusCode)
	fmt.Println("Content-Type:", resp.Header.Get("Content-Type"))
	fmt.Println("---")

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		lineCount++
		fmt.Printf("[%d] %s\n", lineCount, line)
		if lineCount > 100 {
			fmt.Println("... truncated")
			break
		}
	}
}
