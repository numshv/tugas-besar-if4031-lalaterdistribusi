package httpx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/correlation"
)

type Client struct {
	http     *http.Client
	log      *slog.Logger
	upstream string
}

func NewClient(upstream string, timeout time.Duration, log *slog.Logger) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.MaxIdleConns = 200
	transport.MaxIdleConnsPerHost = 100

	return &Client{
		http:     &http.Client{Timeout: timeout, Transport: transport},
		log:      log,
		upstream: upstream,
	}
}

func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	req = req.WithContext(ctx)
	if id := correlation.FromContext(ctx); id != "" {
		req.Header.Set(correlation.Header, id)
	}

	start := time.Now()
	resp, err := c.http.Do(req)
	attrs := []any{
		"upstream", c.upstream,
		"method", req.Method,
		"path", req.URL.Path,
		"duration_ms", msSince(start),
	}
	if err != nil {
		c.log.WarnContext(ctx, "outbound call failed", append(attrs, "error", err)...)
		return nil, fmt.Errorf("%s %s: %w", req.Method, req.URL.Redacted(), err)
	}

	c.log.InfoContext(ctx, "outbound call", append(attrs, "status", resp.StatusCode)...)
	return resp, nil
}

func (c *Client) GetJSON(ctx context.Context, url string, header http.Header, out any) error {
	_, err := c.GetJSONWithResponseHeader(ctx, url, header, out, "")
	return err
}

func (c *Client) GetJSONWithResponseHeader(ctx context.Context, url string, header http.Header, out any, responseHeader string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	for k, values := range header {
		for _, v := range values {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.Do(ctx, req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return "", &StatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(body))}
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	return resp.Header.Get(responseHeader), nil
}

type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected status %d: %s", e.Status, e.Body)
}
