package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/analytics"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/audit"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/loader"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/store"
)

type noLocator struct{}

func (noLocator) ShardOf(context.Context, string) (string, error) { return "", nil }

func seed(t *testing.T, st store.Store) {
	t.Helper()
	var buf bytes.Buffer
	if err := loader.Generate(&buf, 3000, 11); err != nil {
		t.Fatal(err)
	}
	src, err := loader.Open(&buf, "csv")
	if err != nil {
		t.Fatal(err)
	}
	cols, err := loader.ResolveColumns(src.Header(), loader.Columns{})
	if err != nil {
		t.Fatal(err)
	}
	conv := loader.NewConverter(cols, "", time.UTC)
	for {
		rec, err := src.Next()
		if err == io.EOF {
			break
		}
		msg, err := conv.Message(rec)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := st.Upsert(context.Background(), msg.Event()); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	for i, typ := range []hazard.Type{hazard.TypeVolcanic, hazard.TypeWildfire, hazard.TypeCoastal, hazard.TypeWildfire} {
		src := map[hazard.Type]hazard.Source{hazard.TypeVolcanic: hazard.SourcePVMBG, hazard.TypeWildfire: hazard.SourceSatelit, hazard.TypeCoastal: hazard.SourceSensorPantai}[typ]
		ref := string(typ) + "-" + string(rune('a'+i))
		e := hazard.Event{HazardID: hazard.NewID(src, ref), Source: src, SourceRefID: ref, HazardType: typ, Severity: hazard.SeveritySiaga, AreaName: "Kalimantan Tengah", OccurredAt: at.AddDate(0, i, 0), Version: 1}
		if _, _, err := st.Upsert(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	quake := hazard.Event{HazardID: hazard.NewID(hazard.SourceBMKG, "tsu-1"), Source: hazard.SourceBMKG, SourceRefID: "tsu-1", HazardType: hazard.TypeSeismic,
		Severity: hazard.SeverityAwas, AreaName: "Barat Daya Mentawai", OccurredAt: at, Version: 1, Attributes: map[string]any{"potential_tsunami": true}}
	if _, _, err := st.Upsert(context.Background(), quake); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Retract(context.Background(), hazard.SourceSatelit, "WILDFIRE-d", 2); err != nil {
		t.Fatal(err)
	}
}

func TestVerifierMatchesDashboardAgainstAudit(t *testing.T) {
	st := store.NewMemory()
	seed(t, st)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	passthrough := func(h http.Handler) http.Handler { return h }

	mux := http.NewServeMux()
	mux.HandleFunc("POST /login", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"access_token":"dash-token","refresh_token":"r"}`)
	})
	dash := analytics.NewHandler(st, false, log).Routes(passthrough)
	mux.Handle("/analytics/", dash)
	mux.Handle("/internal/audit/", audit.New(st, noLocator{}, nil, audit.Config{Tokens: []string{"audit-token"}}, log))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if err := run(srv.URL, "analytics-viewer", "pw", "audit-token", "week"); err != nil {
		t.Fatalf("verifier must report a match: %v", err)
	}

	tsunami, _ := st.Analytics(context.Background(), analytics.Query{AllTime: true, Bucket: "month"}, false)
	if tsunami.ByCategory[analytics.CategoryTsunami] == 0 || tsunami.ByCategory[analytics.CategoryWildfire] != 1 {
		t.Fatalf("seed must exercise tsunami and the retracted fire: %v", tsunami.ByCategory)
	}

	drift := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/analytics/data") {
			_, _ = io.WriteString(w, `{"total":1,"by_category":{"GEMPA":1},"by_severity":{},"by_area":[],"buckets":[],"series":{}}`)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	defer drift.Close()
	if err := run(drift.URL, "analytics-viewer", "pw", "audit-token", "week"); err == nil || !strings.Contains(err.Error(), "mismatches") {
		t.Fatalf("a wrong dashboard must be reported, err = %v", err)
	}
}
