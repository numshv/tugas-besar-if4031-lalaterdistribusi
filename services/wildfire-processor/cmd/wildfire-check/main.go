// wildfire-check runs the wildfire engine over a recorded feed (JSON lines
// with id, type and data, in offset order) and checks it against a brute-force
// recomputation. It prints a summary and can write the fires as JSON, for the
// comparison with the assistants' reference.
//
//	go run ./services/wildfire-processor/cmd/wildfire-check -in events.jsonl [-out fires.json]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/satfeed"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/wildfire-processor/internal/fire"
)

func main() {
	in := flag.String("in", "", "recorded feed, one JSON event per line")
	out := flag.String("out", "", "write the fires here as JSON")
	lateness := flag.Duration("lateness", 360*time.Minute, "allowed lateness")
	flag.Parse()
	if err := run(*in, *out, *lateness); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(in, out string, lateness time.Duration) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer f.Close()

	cfg := fire.DefaultConfig()
	cfg.AllowedLateness = lateness
	e := fire.New(cfg)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	start := time.Now()
	for sc.Scan() {
		var line struct {
			ID   string          `json:"id"`
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil || line.ID == "" {
			continue
		}
		if err := e.Apply(satfeed.Event{ID: line.ID, Type: line.Type, Data: line.Data}, time.Now()); err != nil {
			fmt.Fprintln(os.Stderr, "warn:", err)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	elapsed := time.Since(start)

	fires := e.Fires()
	sum := e.Summary()
	refStart := time.Now()
	match := reflect.DeepEqual(e.Clusters(), e.ReferenceClusters())
	fmt.Printf("events            %d (%.0f/s)\n", sum.Events, float64(sum.Events)/elapsed.Seconds())
	fmt.Printf("detections        %d unique, %d duplicates, %d low confidence, %d retracted\n", sum.Detections, sum.Duplicates, sum.LowConfidence, sum.RetractedFeedRows)
	fmt.Printf("late              %d (allowed lateness %s)\n", sum.Late, lateness)
	fmt.Printf("fires             %d (%d final, %d still open), %d retracted IDs\n", len(fires), sum.FiresFinal, sum.FiresActive, sum.FiresRetracted)
	fmt.Printf("single detections %d\n", sum.SingleDetections)
	fmt.Printf("gaps / malformed  %d / %d\n", sum.Gaps, sum.Malformed)
	fmt.Printf("digest            %s\n", fire.Digest(fires))
	fmt.Printf("brute force match %v (%.1fs)\n", match, time.Since(refStart).Seconds())

	if out != "" {
		b, err := json.MarshalIndent(fires, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(out, b, 0o644); err != nil {
			return err
		}
	}
	if !match {
		return fmt.Errorf("streaming result differs from the brute-force reference")
	}
	return nil
}
