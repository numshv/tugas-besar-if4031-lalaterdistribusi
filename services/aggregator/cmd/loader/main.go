package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/lalaterdistribusi/tugas-besar-if4031/pkg/envx"
	"github.com/lalaterdistribusi/tugas-besar-if4031/services/aggregator/internal/loader"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loader:", err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "load", "load | spike | cleanup | generate")
	file := flag.String("file", "", "dataset path, or - for stdin")
	url := flag.String("url", "", "download the dataset from this http(s) URL")
	format := flag.String("format", "auto", "csv | json | jsonl | auto")
	dryRun := flag.Bool("dry-run", false, "validate and print the expected checksum without sending")
	strict := flag.Bool("strict", false, "stop at the first invalid row")
	ratePerSec := flag.Float64("rate", 0, "messages per second (0 = as fast as Kafka accepts)")
	brokers := flag.String("brokers", envx.String("KAFKA_BROKERS", "kafka:9092"), "Kafka brokers, comma separated")
	topic := flag.String("topic", envx.String("KAFKA_UPSERTS_TOPIC", "bnpb.hazard-upserts.v1"), "hazard-upserts topic")
	timeLayout := flag.String("time-layout", "", "Go time layout of the time column (default: try common layouts)")
	tz := flag.String("tz", "UTC", "time zone of timestamps without an offset")
	var cols loader.Columns
	flag.StringVar(&cols.ID, "col-id", "", "id column")
	flag.StringVar(&cols.Time, "col-time", "", "timestamp column")
	flag.StringVar(&cols.Date, "col-date", "", "date column (with --col-clock)")
	flag.StringVar(&cols.Clock, "col-clock", "", "time-of-day column (with --col-date)")
	flag.StringVar(&cols.Lat, "col-lat", "", "latitude column")
	flag.StringVar(&cols.Lon, "col-lon", "", "longitude column")
	flag.StringVar(&cols.Mag, "col-mag", "", "magnitude column")
	flag.StringVar(&cols.Depth, "col-depth", "", "depth column")
	flag.StringVar(&cols.Region, "col-region", "", "region / area column")
	flag.StringVar(&cols.Tsunami, "col-tsunami", "", "tsunami flag column (1/true marks a potential tsunami)")
	runID := flag.String("run", "", "spike/cleanup run id (part of every sim-spike id)")
	count := flag.Int("count", 1000, "events to generate (spike, cleanup, generate)")
	region := flag.String("region", "Selatan Jawa Barat", "spike: area_name of the burst")
	lat := flag.Float64("lat", -8.1, "spike: centre latitude")
	lon := flag.Float64("lon", 107.6, "spike: centre longitude")
	radius := flag.Float64("radius-km", 50, "spike: radius around the centre")
	seed := flag.Uint64("seed", 1, "random seed (generate, spike)")
	out := flag.String("out", "-", "generate: output path, or - for stdout")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opt := loader.Options{DryRun: *dryRun, Strict: *strict, Rate: *ratePerSec}

	if *mode == "generate" {
		w := io.Writer(os.Stdout)
		if *out != "-" {
			f, err := os.Create(*out)
			if err != nil {
				return err
			}
			defer f.Close()
			w = f
		}
		return loader.Generate(w, *count, *seed)
	}

	var sender *loader.KafkaSender
	if !*dryRun {
		sender = loader.NewKafkaSender(envx.SplitList(*brokers), *topic)
		defer sender.Close()
	}

	var sum loader.Summary
	var err error
	switch *mode {
	case "load":
		sum, err = load(ctx, *file, *url, *format, cols, *timeLayout, *tz, sender, opt)
	case "spike", "cleanup":
		if *runID == "" {
			return errors.New("--run is required for spike and cleanup, so cleanup can rebuild the same ids")
		}
		if *mode == "spike" {
			s := loader.Spike{Run: *runID, Count: *count, Region: *region, Lat: *lat, Lon: *lon, RadiusKM: *radius, Seed: *seed, Now: time.Now()}
			msgs, merr := s.Messages()
			if merr != nil {
				return merr
			}
			sum, err = loader.SendAll(ctx, "spike", msgs, senderOrNil(sender), opt)
		} else {
			sum, err = loader.SendAll(ctx, "cleanup", loader.PurgeMessages(*runID, *count), senderOrNil(sender), opt)
		}
	default:
		return fmt.Errorf("unknown --mode %q", *mode)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(sum)
	return err
}

func senderOrNil(s *loader.KafkaSender) loader.Sender {
	if s == nil {
		return nil
	}
	return s
}

func load(ctx context.Context, file, url, format string, cols loader.Columns, layout, tz string, sender *loader.KafkaSender, opt loader.Options) (loader.Summary, error) {
	var in io.Reader
	switch {
	case url != "":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return loader.Summary{}, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return loader.Summary{}, fmt.Errorf("download dataset: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return loader.Summary{}, fmt.Errorf("download dataset: HTTP %d", resp.StatusCode)
		}
		in = resp.Body
	case file == "-":
		in = os.Stdin
	case file != "":
		f, err := os.Open(file)
		if err != nil {
			return loader.Summary{}, err
		}
		defer f.Close()
		in = f
	default:
		return loader.Summary{}, errors.New("give --file, --file=- or --url")
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return loader.Summary{}, fmt.Errorf("--tz: %w", err)
	}
	src, err := loader.Open(in, format)
	if err != nil {
		return loader.Summary{}, err
	}
	resolved, err := loader.ResolveColumns(src.Header(), cols)
	if err != nil {
		return loader.Summary{}, err
	}
	fmt.Fprintf(os.Stderr, "loader: columns id=%q time=%q date=%q clock=%q lat=%q lon=%q mag=%q depth=%q region=%q\n",
		resolved.ID, resolved.Time, resolved.Date, resolved.Clock, resolved.Lat, resolved.Lon, resolved.Mag, resolved.Depth, resolved.Region)
	return loader.Load(ctx, src, loader.NewConverter(resolved, layout, loc), senderOrNil(sender), opt)
}
