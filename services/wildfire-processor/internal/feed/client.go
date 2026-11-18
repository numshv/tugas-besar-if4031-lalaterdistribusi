// Package feed talks to the Feed Satelit: the session endpoint and the SSE stream.
package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
)

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// IdleTimeout ends a connection that has delivered nothing, not even a
	// heartbeat, for this long.
	IdleTimeout time.Duration
}

type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("feed answered HTTP %d: %s", e.Code, e.Body)
}

var ErrIdle = errors.New("feed connection idle")

func (c *Client) Session(ctx context.Context) (satfeed.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/v1/session", nil)
	if err != nil {
		return satfeed.Session{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return satfeed.Session{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return satfeed.Session{}, statusError(resp)
	}
	var s satfeed.Session
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&s); err != nil {
		return satfeed.Session{}, fmt.Errorf("decode session: %w", err)
	}
	return s, nil
}

// Stream opens one SSE connection starting at offset from and calls fn for
// every event until the server closes it, the connection goes idle, fn fails
// or ctx ends. The server closes connections on its own every ~40 s, so a nil
// return is normal and the caller reconnects from the next offset.
func (c *Client) Stream(ctx context.Context, from int64, fn func(satfeed.Event) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	url := fmt.Sprintf("%s/v1/stream?from=%d", strings.TrimRight(c.BaseURL, "/"), from)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "text/event-stream")
	if from > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(from-1, 10))
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}

	idle := time.AfterFunc(c.IdleTimeout, func() { cancel(ErrIdle) })
	defer idle.Stop()
	err = satfeed.ReadEvents(resp.Body, func(ev satfeed.Event) error {
		idle.Reset(c.IdleTimeout)
		return fn(ev)
	})
	if cause := context.Cause(ctx); errors.Is(cause, ErrIdle) {
		return ErrIdle
	}
	if err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

func statusError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(body))}
}
