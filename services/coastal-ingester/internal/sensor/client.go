// Package sensor talks to Sensor Pantai through timeout, retry, circuit breaker and
// bulkhead policies, so a sick sensor is not hammered and callers fail fast.
package sensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/bulkhead"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/failsafe-go/failsafe-go/timeout"
)

type Station struct {
	ID        string  `json:"station_id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Page struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Items       []json.RawMessage `json:"items"`
	NextCursor  string            `json:"next_cursor"`
	ReceivedAt  time.Time         `json:"-"`
}

type Config struct {
	BaseURL, Token   string
	RequestTimeout   time.Duration
	MaxRetries       int
	BreakerFailures  uint          // consecutive failures that open the breaker
	BreakerDelay     time.Duration // how long it stays open before a trial request
	MaxConcurrency   uint
	StuckAfter       time.Duration // generated_at not moving for this long counts as a failure
	StaleAfter       time.Duration // caught up, yet the newest observation is older than this
	OnBreakerChanged func(state string)
}

// ErrStale marks a 200 response with expired data: the sensor says it is healthy, but its
// snapshot stopped moving, or it has nothing newer than StaleAfter to give.
var ErrStale = errors.New("sensor data is stale")

type statusError struct{ code int }

func (e statusError) Error() string { return fmt.Sprintf("sensor answered %d", e.code) }

type Client struct {
	cfg     Config
	http    *http.Client
	breaker circuitbreaker.CircuitBreaker[*Page]
	pages   failsafe.Executor[*Page]
	plain   failsafe.Executor[[]Station]

	mu          sync.Mutex
	lastGen     time.Time
	lastAdvance time.Time
}

func New(cfg Config, httpClient *http.Client) *Client {
	retryable := func(_ *Page, err error) bool {
		var se statusError
		return err != nil && !errors.Is(err, circuitbreaker.ErrOpen) && !errors.Is(err, bulkhead.ErrFull) &&
			(!errors.As(err, &se) || se.code >= 500 || se.code == http.StatusTooManyRequests)
	}
	breaker := circuitbreaker.NewBuilder[*Page]().
		WithFailureThreshold(cfg.BreakerFailures).
		WithDelay(cfg.BreakerDelay).
		WithSuccessThreshold(1).
		OnStateChanged(func(e circuitbreaker.StateChangedEvent) {
			if cfg.OnBreakerChanged != nil {
				cfg.OnBreakerChanged(e.NewState.String())
			}
		}).
		Build()
	retry := retrypolicy.NewBuilder[*Page]().
		HandleIf(retryable).
		WithMaxRetries(cfg.MaxRetries).
		WithBackoff(200*time.Millisecond, 2*time.Second).
		WithJitterFactor(0.3).
		ReturnLastFailure().
		Build()
	return &Client{
		cfg:     cfg,
		http:    httpClient,
		breaker: breaker,
		// Outermost first: one bulkhead slot per call, retries inside it, and every attempt
		// passes through the breaker (so it counts each failure) and has its own timeout.
		pages: failsafe.With[*Page](bulkhead.New[*Page](cfg.MaxConcurrency), retry, breaker, timeout.New[*Page](cfg.RequestTimeout)),
		plain: failsafe.With[[]Station](timeout.New[[]Station](cfg.RequestTimeout)),
	}
}

func (c *Client) BreakerState() string { return c.breaker.State().String() }

func (c *Client) Observations(ctx context.Context, cursor string, limit int) (*Page, error) {
	q := url.Values{"limit": {fmt.Sprint(limit)}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	return c.pages.WithContext(ctx).Get(func() (*Page, error) {
		var p Page
		if err := c.get(ctx, "/v1/observations?"+q.Encode(), &p); err != nil {
			return nil, err
		}
		p.ReceivedAt = time.Now().UTC()
		if c.stuck(p.GeneratedAt) || c.expired(&p, limit) {
			return nil, ErrStale
		}
		return &p, nil
	})
}

func (c *Client) Stations(ctx context.Context) ([]Station, error) {
	return c.plain.WithContext(ctx).Get(func() ([]Station, error) {
		var body struct {
			Items []Station `json:"items"`
		}
		err := c.get(ctx, "/v1/stations", &body)
		return body.Items, err
	})
}

// stuck reports whether generated_at has not advanced for longer than StuckAfter.
// The sensor runs on its own (virtual) clock, so only its own timestamps are compared.
func (c *Client) stuck(generated time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if !generated.Equal(c.lastGen) || c.lastAdvance.IsZero() {
		// Moving forward is progress; moving back means the sensor restarted its clock.
		c.lastGen, c.lastAdvance = generated, now
		return false
	}
	return now.Sub(c.lastAdvance) > c.cfg.StuckAfter
}

// expired reports a page that is caught up (not full) whose newest observation lags the
// sensor's own clock by more than StaleAfter. A full page is backlog, not stale data.
func (c *Client) expired(p *Page, limit int) bool {
	if c.cfg.StaleAfter <= 0 || len(p.Items) == 0 || len(p.Items) >= limit {
		return false
	}
	var newest time.Time
	for _, raw := range p.Items {
		var o struct {
			ObservedAt time.Time `json:"observed_at"`
		}
		if json.Unmarshal(raw, &o) == nil && o.ObservedAt.After(newest) {
			newest = o.ObservedAt
		}
	}
	return !newest.IsZero() && p.GeneratedAt.Sub(newest) > c.cfg.StaleAfter
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.BaseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return statusError{code: resp.StatusCode}
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(out)
}
