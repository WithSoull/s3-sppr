package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/traces"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	var c traces.Config
	source := flag.String("source", "meta", "meta, ibm-cos or docker")
	zone := flag.String("zone", "", "required Docker zone, e.g. syd01")
	from := flag.String("from", "", "Docker first filename day, YYYY-MM-DD inclusive")
	to := flag.String("to", "", "Docker last filename day, YYYY-MM-DD exclusive")
	origin := flag.String("origin", "", "required IBM COS absolute origin, RFC3339 (calendar time is artificial)")
	flag.StringVar(&c.Input, "input", "", "local trace input (no downloads); IBM COS tar gzip/Unix compress or text")
	flag.StringVar(&c.SourceURL, "source-url", "", "original URL of this input")
	flag.StringVar(&c.Bucket, "bucket", "meta-eag", "catalog bucket: one Meta region, COS trace file or Docker zone per dataset")
	flag.Float64Var(&c.SourceShare, "source-share", 0, "required source file share: eag/prn=0.2, nha=0.8")
	flag.Float64Var(&c.KeyShare, "key-share", 1, "deterministic additional key sampling in (0,1]")
	flag.StringVar(&c.SampleSeed, "sample-seed", "0", "SHA256 sampling seed")
	flag.BoolVar(&c.AllowInferredSuccess, "allow-inferred-success", false, "acknowledge absent HTTP status in Meta/COS; accept the documented success proxy")
	out := flag.String("out", "", "new output directory")
	flag.Parse()
	bucketGiven := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "bucket" {
			bucketGiven = true
		}
	})
	if *out == "" || flag.NArg() != 0 {
		return fmt.Errorf("supply -input, -out and source-specific flags; see docs/traces.md")
	}
	abs, err := filepath.Abs(*out)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(abs); !os.IsNotExist(err) {
		return fmt.Errorf("output exists or cannot be inspected: %s", abs)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(abs), ".sppr-trace-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var m traces.Manifest
	switch *source {
	case "meta":
		m, err = traces.Convert(ctx, c, staging)
	case "ibm-cos":
		var at time.Time
		at, err = time.Parse(time.RFC3339Nano, *origin)
		if err != nil {
			return fmt.Errorf("-origin: %w", err)
		}
		if !bucketGiven {
			return fmt.Errorf("IBM COS requires explicit -bucket, e.g. ibm-cos-001")
		}
		m, err = traces.ConvertCOS(ctx, traces.COSConfig{Config: c, Origin: at}, staging)
	case "docker":
		if !bucketGiven {
			c.Bucket = "docker-" + *zone
		}
		m, err = traces.ConvertDocker(ctx, traces.DockerConfig{Config: c, Zone: *zone, From: *from, To: *to}, staging)
	default:
		return fmt.Errorf("unknown -source %q", *source)
	}
	if err != nil {
		return err
	}
	if _, err := os.Lstat(abs); !os.IsNotExist(err) {
		return fmt.Errorf("output appeared: %s", abs)
	}
	if err := os.Rename(staging, abs); err != nil {
		return err
	}
	fmt.Printf("%s\nevents=%d versions=%d\n", abs, m.Counts["events"], m.Counts["versions"])
	return nil
}
