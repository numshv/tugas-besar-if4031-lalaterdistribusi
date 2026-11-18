package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/hazard"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/audit"
)

type captureSender struct {
	batches [][]hazard.EncodedUpsert
}

func (c *captureSender) Send(_ context.Context, msgs []hazard.EncodedUpsert) error {
	c.batches = append(c.batches, append([]hazard.EncodedUpsert(nil), msgs...))
	return nil
}

func (c *captureSender) decoded(t *testing.T) []hazard.UpsertMessage {
	t.Helper()
	var out []hazard.UpsertMessage
	for _, b := range c.batches {
		for _, m := range b {
			msg, err := hazard.DecodeUpsert(m.Value)
			if err != nil {
				t.Fatalf("loader produced a message the aggregator would park: %v", err)
			}
			out = append(out, msg)
		}
	}
	return out
}

func loadString(t *testing.T, data, format string, cols Columns, opt Options) (Summary, []hazard.UpsertMessage, error) {
	t.Helper()
	src, err := Open(strings.NewReader(data), format)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveColumns(src.Header(), cols)
	if err != nil {
		t.Fatal(err)
	}
	cs := &captureSender{}
	sum, err := Load(context.Background(), src, NewConverter(resolved, "", time.UTC), cs, opt)
	return sum, cs.decoded(t), err
}

const catalogCSV = `id,time,latitude,longitude,depth,mag,place
EQ-1,2024-01-02 03:04:05,-8.1,107.6,10,6.8,Selatan Jawa Barat
 EQ-2 ,2024-01-02T03:04:06Z,-2.4,99.6,35,5.1,
EQ-3,not-a-time,-2.4,99.6,35,4.0,Mentawai
EQ-4,2024-01-02 03:04:07,-2.4,99.6,35,4.2,Mentawai
EQ-1,2024-01-02 03:04:05,-8.1,107.6,10,6.8,Selatan Jawa Barat
`

func TestLoadCSVMapsRowsLikeLiveBMKG(t *testing.T) {
	sum, msgs, err := loadString(t, catalogCSV, "auto", Columns{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Read != 5 || sum.Valid != 4 || sum.Invalid != 1 || sum.Duplicates != 1 || sum.Count != 3 || sum.Sent != 4 {
		t.Fatalf("summary = %+v", sum)
	}
	if sum.FirstInvalid[0].Row != 4 || !strings.Contains(sum.FirstInvalid[0].Error, "not-a-time") {
		t.Fatalf("invalid row report = %+v", sum.FirstInvalid)
	}
	if want := audit.Checksum([]string{"EQ-1", " EQ-2 ", "EQ-4"}); sum.Checksum != want {
		t.Fatalf("checksum = %s, want %s (ids must be verbatim, unique)", sum.Checksum, want)
	}

	first := msgs[0]
	if first.Source != hazard.SourceBMKG || first.HazardType != hazard.TypeSeismic || first.Version != 1 ||
		first.Severity != hazard.SeveritySiaga || first.AreaName != "Selatan Jawa Barat" ||
		!first.OccurredAt.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("first message = %+v", first)
	}
	if first.Attributes["magnitude"] != 6.8 || first.Attributes["depth_km"] != 10.0 {
		t.Fatalf("attributes = %v", first.Attributes)
	}
	second := msgs[1]
	if second.SourceRefID != " EQ-2 " || second.Severity != hazard.SeverityWaspada {
		t.Fatalf("second = %+v", second)
	}
	if second.AreaName != "Sumatera Barat" {
		t.Fatalf("empty place must fall back to the nearest province, got %q", second.AreaName)
	}
}

func TestStrictStopsAtFirstInvalidRow(t *testing.T) {
	sum, _, err := loadString(t, catalogCSV, "csv", Columns{}, Options{Strict: true})
	if err == nil || !strings.Contains(err.Error(), "row 4") || sum.Read != 3 {
		t.Fatalf("strict: err=%v sum=%+v", err, sum)
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	src, _ := Open(strings.NewReader(catalogCSV), "csv")
	cols, _ := ResolveColumns(src.Header(), Columns{})
	cs := &captureSender{}
	sum, err := Load(context.Background(), src, NewConverter(cols, "", time.UTC), cs, Options{DryRun: true})
	if err != nil || len(cs.batches) != 0 || sum.Sent != 0 || sum.Checksum == "" {
		t.Fatalf("dry run: err=%v batches=%d sum=%+v", err, len(cs.batches), sum)
	}
}

func TestJSONAndColumnOverrides(t *testing.T) {
	arr := `[{"kode":"A1","tgl":"2024-05-01","jam":"10:00:00","lintang":-6.6,"bujur":105.2,"m":5.5,"lokasi":"Selat Sunda","extra":{"src":"x"}}]`
	cols := Columns{ID: "kode", Date: "tgl", Clock: "jam"}
	_, msgs, err := loadString(t, arr, "auto", cols, Options{})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("json array: err=%v msgs=%d", err, len(msgs))
	}
	m := msgs[0]
	if m.SourceRefID != "A1" || m.AreaName != "Selat Sunda" || !m.OccurredAt.Equal(time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("message = %+v", m)
	}

	jsonl := "{\"id\":7,\"time\":\"2024-05-01T10:00:00+07:00\",\"lat\":-6.6,\"lon\":105.2,\"mag\":4.9}\n{\"id\":8,\"time\":\"2024-05-01T11:00:00Z\",\"lat\":-6.6,\"lon\":105.2,\"mag\":3.0}\n"
	sum, msgs, err := loadString(t, jsonl, "jsonl", Columns{}, Options{})
	if err != nil || sum.Valid != 2 {
		t.Fatalf("jsonl: err=%v sum=%+v", err, sum)
	}
	if msgs[0].SourceRefID != "7" || !msgs[0].OccurredAt.Equal(time.Date(2024, 5, 1, 3, 0, 0, 0, time.UTC)) {
		t.Fatalf("numeric id must keep its text and offsets must convert to UTC: %+v", msgs[0])
	}
}

func TestTimeZoneForNaiveTimestamps(t *testing.T) {
	src, _ := Open(strings.NewReader("id,time,lat,lon,mag\nX,2024-01-01 07:00:00,-6,106,4\n"), "csv")
	cols, _ := ResolveColumns(src.Header(), Columns{})
	jakarta, _ := time.LoadLocation("Asia/Jakarta")
	cs := &captureSender{}
	if _, err := Load(context.Background(), src, NewConverter(cols, "", jakarta), cs, Options{}); err != nil {
		t.Fatal(err)
	}
	if got := cs.decoded(t)[0].OccurredAt; !got.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("WIB 07:00 must be stored as 00:00Z, got %v", got)
	}
}

func TestMissingColumnsAreReported(t *testing.T) {
	src, _ := Open(strings.NewReader("kode,waktu_kejadian,x,y\n"), "csv")
	if _, err := ResolveColumns(src.Header(), Columns{}); err == nil || !strings.Contains(err.Error(), "--col-") {
		t.Fatalf("err = %v", err)
	}
}

func TestSpikeAndCleanupShareIDs(t *testing.T) {
	s := Spike{Run: "s1", Count: 50, Region: "Selatan Jawa Barat", Lat: -8.1, Lon: 107.6, RadiusKM: 50, Seed: 3, Now: time.Now()}
	a, err := s.Messages()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := s.Messages()
	purge := PurgeMessages("s1", 50)
	for i := range a {
		if a[i].SourceRefID != b[i].SourceRefID || a[i].Latitude != b[i].Latitude {
			t.Fatal("same seed must give the same spike")
		}
		if !strings.HasPrefix(a[i].SourceRefID, "sim-spike-s1-") || a[i].AreaName != "Selatan Jawa Barat" {
			t.Fatalf("spike message = %+v", a[i])
		}
		if purge[i].SourceRefID != a[i].SourceRefID || purge[i].Op != hazard.OpPurge || purge[i].Validate() != nil {
			t.Fatalf("cleanup must rebuild the spike ids: %+v", purge[i])
		}
		if dist := (a[i].Latitude - -8.1) * 111; dist > 51 || dist < -51 {
			t.Fatalf("spike point %v,%v is outside the radius", a[i].Latitude, a[i].Longitude)
		}
	}
	cs := &captureSender{}
	sum, err := SendAll(context.Background(), "cleanup", purge, cs, Options{})
	if err != nil || sum.Sent != 50 || len(cs.batches) != 1 {
		t.Fatalf("cleanup send: err=%v sum=%+v", err, sum)
	}
}

func TestGenerateIsDeterministicAndSkewed(t *testing.T) {
	var a, b bytes.Buffer
	if err := Generate(&a, 3000, 9); err != nil {
		t.Fatal(err)
	}
	_ = Generate(&b, 3000, 9)
	if a.String() != b.String() {
		t.Fatal("same seed must give the same file")
	}
	sum, msgs, err := loadString(t, a.String(), "csv", Columns{}, Options{})
	if err != nil || sum.Valid != 3000 || sum.Invalid != 0 {
		t.Fatalf("generated file must load cleanly: err=%v sum=%+v", err, sum)
	}
	subduction := 0
	for _, m := range msgs {
		if strings.HasPrefix(m.SourceRefID, hazard.SimPrefix) {
			t.Fatal("catalog ids must never look synthetic-purgeable")
		}
		for _, z := range zones[:8] {
			if m.AreaName == z.name {
				subduction++
			}
		}
	}
	if share := float64(subduction) / 3000; share < 0.62 || share > 0.76 {
		t.Fatalf("Sumatra–Java share = %.2f, want about 0.69", share)
	}
	raw, _ := json.Marshal(msgs[0])
	if !strings.Contains(string(raw), `"op":"upsert"`) {
		t.Fatalf("message json = %s", raw)
	}
}
