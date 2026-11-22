package api

import (
	"net/http"
	"strconv"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/httpx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/processor"
)

type Source interface {
	Status() processor.Status
	Fires() ([]hazard.UpsertMessage, string)
	Late() []fire.LateDetection
}

const maxLimit = 1000

func Routes(src Source, auth func(http.Handler) http.Handler) http.Handler {
	internal := http.NewServeMux()
	internal.HandleFunc("GET /internal/wildfire/stats", func(w http.ResponseWriter, r *http.Request) {
		httpx.JSON(w, http.StatusOK, src.Status())
	})
	internal.HandleFunc("GET /internal/wildfire/fires", func(w http.ResponseWriter, r *http.Request) {
		fires, digest := src.Fires()
		if fires == nil {
			fires = []hazard.UpsertMessage{}
		}
		httpx.JSON(w, http.StatusOK, map[string]any{"count": len(fires), "digest": digest, "items": fires})
	})
	internal.HandleFunc("GET /internal/wildfire/late", func(w http.ResponseWriter, r *http.Request) {
		limit, err1 := intParam(r, "limit", 100)
		after, err2 := intParam(r, "after_offset", -1)
		if err1 != nil || err2 != nil || limit < 1 || limit > maxLimit {
			httpx.Error(w, http.StatusBadRequest, "invalid_parameter", "limit must be 1..1000 and after_offset an integer")
			return
		}
		all := src.Late()
		items := []fire.LateDetection{}
		for _, l := range all {
			if l.Offset > int64(after) {
				items = append(items, l)
			}
		}
		var next *int64
		if len(items) > limit {
			items = items[:limit]
			n := items[limit-1].Offset
			next = &n
		}
		httpx.JSON(w, http.StatusOK, map[string]any{"total": len(all), "items": items, "next_after_offset": next})
	})

	root := http.NewServeMux()
	root.HandleFunc("GET /health", httpx.Health("wildfire-processor"))
	root.Handle("/internal/wildfire/", auth(internal))
	return root
}

func intParam(r *http.Request, name string, def int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	return strconv.Atoi(raw)
}
