package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
)

const (
	CorruptTruncated   = "truncated"    // valid JSON cut in half
	CorruptDeep        = "deep"         // arrays nested corruptDepth levels deep
	CorruptInvalidUTF8 = "invalid_utf8" // a string value contains bytes that are not UTF-8
	CorruptOversize    = "oversize"     // valid JSON padded past corruptOversizeBytes
)

const (
	corruptDepth         = 100_000
	corruptOversizeBytes = 8 << 20
	corruptMaxCount      = 1000
)

// corruption holds the broken bodies still to be served; each response consumes one.
type corruption struct {
	mu        sync.Mutex
	kind      string
	remaining int
}

func (c *corruption) set(kind string, count int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.kind, c.remaining = kind, count
}

func (c *corruption) take() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining <= 0 {
		return ""
	}
	c.remaining--
	return c.kind
}

func (c *corruption) status() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining <= 0 {
		return nil
	}
	return map[string]any{"kind": c.kind, "remaining": c.remaining}
}

func corruptBody(kind string, payload []map[string]any) []byte {
	switch kind {
	case CorruptTruncated:
		b, _ := json.Marshal(payload)
		return b[:len(b)/2]
	case CorruptDeep:
		return []byte(strings.Repeat("[", corruptDepth) + strings.Repeat("]", corruptDepth))
	case CorruptInvalidUTF8:
		first := map[string]any{"report_id": "corrupt"}
		if len(payload) > 0 {
			first = payload[0]
		}
		b, _ := json.Marshal([]map[string]any{first})
		// Insert raw 0xff 0xfe bytes at the start of the report_id value.
		return bytes.Replace(b, []byte(`"report_id":"`), []byte("\"report_id\":\"\xff\xfe"), 1)
	case CorruptOversize:
		padded := append([]map[string]any{{"padding": strings.Repeat("x", corruptOversizeBytes)}}, payload...)
		b, _ := json.Marshal(padded)
		return b
	}
	b, _ := json.Marshal(payload)
	return b
}

func validCorruptKind(kind string) bool {
	switch kind {
	case CorruptTruncated, CorruptDeep, CorruptInvalidUTF8, CorruptOversize:
		return true
	}
	return false
}

// setCorrupt schedules the next count responses of GET /volcanic-reports to be broken.
// count 0 cancels whatever is still pending.
func (h *handler) setCorrupt(w http.ResponseWriter, r *http.Request) {
	if !h.adminAuthorized(w, r) {
		return
	}
	var body struct {
		Kind  string `json:"kind"`
		Count *int   `json:"count"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	count := 1
	if body.Count != nil {
		count = *body.Count
	}
	if count < 0 || count > corruptMaxCount {
		httpx.Error(w, http.StatusBadRequest, "invalid_count", "count must be between 0 and 1000")
		return
	}
	if count > 0 && !validCorruptKind(body.Kind) {
		httpx.Error(w, http.StatusBadRequest, "invalid_kind",
			"kind must be truncated, deep, invalid_utf8 or oversize")
		return
	}

	h.corrupt.set(body.Kind, count)
	h.log.InfoContext(r.Context(), "corruption scheduled", "kind", body.Kind, "count", count)
	httpx.JSON(w, http.StatusOK, map[string]any{"kind": body.Kind, "remaining": count})
}
