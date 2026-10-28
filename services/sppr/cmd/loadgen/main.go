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

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/loadgen"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	metadata "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	config := flag.String("config", "", "load config JSON (defaults: synthetic stable)")
	gateway := flag.String("gateway", "http://localhost:8080", "Gateway URL")
	md := flag.String("metadata", "localhost:50052", "Metadata for bucket creation")
	bucket := flag.String("bucket", "loadgen-"+uuid.NewString(), "dedicated bucket")
	out := flag.String("out", "loadgen.json", "new result JSON")
	raw := flag.String("raw", "", "optional new raw JSONL gzip path")
	seed := flag.Bool("prepare", true, "create bucket through Metadata and PUT objects through Gateway")
	duration := flag.Duration("duration", 0, "override arrival duration")
	compression := flag.Float64("time-compression", 0, "override logical seconds per real second")
	rate := flag.Float64("rate", 0, "override constant reads/s")
	objects := flag.Int("objects", 0, "override object count (at least 10)")
	size := flag.Int64("size-bytes", 0, "override full object size")
	scenario := flag.String("scenario", "", "override synthetic scenario")
	generatorSeed := flag.Uint64("seed", 0, "override generator seed")
	flag.Parse()
	c := loadgen.Config{Generator: sim.DefaultConfig().Generator, Schedule: "synthetic", DurationSeconds: 60, TimeCompression: 1, Slots: 8, RequestTimeoutSeconds: 300}
	var err error
	if *config != "" {
		var f *os.File
		f, err = os.Open(*config)
		if err == nil {
			d := json.NewDecoder(f)
			d.DisallowUnknownFields()
			err = d.Decode(&c)
			if err == nil && d.Decode(new(any)) != io.EOF {
				err = fmt.Errorf("config must contain one JSON object")
			}
			f.Close()
		}
	}
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "objects":
			c.Generator.Objects = *objects
		case "size-bytes":
			c.Generator.SizeBytes = *size
		case "scenario":
			c.Generator.Scenario = *scenario
		case "seed":
			c.Generator.Seed = *generatorSeed
		case "duration":
			c.DurationSeconds = duration.Seconds()
		case "time-compression":
			c.TimeCompression = *compression
		case "rate":
			c.Generator.ReadsPerSecond = *rate
			c.Schedule = "constant"
		}
	})
	if err == nil {
		if _, e := os.Stat(*out); e == nil {
			err = fmt.Errorf("new output required: %s", *out)
		} else if !os.IsNotExist(e) {
			err = e
		}
	}
	if err == nil {
		err = c.Validate()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	client := &http.Client{Timeout: time.Duration(c.RequestTimeoutSeconds * 1e9), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	if err == nil && *seed {
		var conn *grpc.ClientConn
		conn, err = grpc.NewClient(*md, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			_, err = metadata.NewMetadataServiceClient(conn).CreateBucket(ctx, &metadata.CreateBucketRequest{Name: *bucket, OwnerId: uuid.NewString()})
			conn.Close()
		}
		if err == nil {
			err = loadgen.Seed(ctx, client, *gateway, *bucket, c)
		}
	}
	if err == nil {
		var r loadgen.Result
		r, err = loadgen.Run(ctx, client, *gateway, *bucket, c, *raw)
		f, e := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if e == nil {
			e = json.NewEncoder(f).Encode(r)
			e = errors.Join(e, f.Close())
		}
		err = errors.Join(err, e)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
