package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

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
	flag.StringVar(&c.Input, "input", "", "local Meta CDN CSV or CSV.zst (no downloads)")
	flag.StringVar(&c.SourceURL, "source-url", "", "original URL of this input")
	flag.StringVar(&c.Bucket, "bucket", "meta-eag", "catalog bucket, use one region per dataset")
	flag.Float64Var(&c.SourceShare, "source-share", 0, "required source file share: eag/prn=0.2, nha=0.8")
	flag.Float64Var(&c.KeyShare, "key-share", 1, "deterministic additional key sampling in (0,1]")
	flag.StringVar(&c.SampleSeed, "sample-seed", "0", "SHA256 sampling seed")
	flag.BoolVar(&c.AllowInferredSuccess, "allow-inferred-success", false, "acknowledge absent HTTP status; use GET with positive body as success proxy")
	out := flag.String("out", "", "new output directory")
	flag.Parse()
	if *out == "" || flag.NArg() != 0 {
		return fmt.Errorf("supply -input, -source-url, -out and -allow-inferred-success; see docs/traces.md")
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
	m, err := traces.Convert(ctx, c, staging)
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
