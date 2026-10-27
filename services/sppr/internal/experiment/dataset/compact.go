package dataset

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type rowWriter interface {
	Write(Row) error
	Close() error
	Names() []string
}

type jsonRowWriter struct{ lines *jsonLines }

func (w *jsonRowWriter) Write(r Row) error { return w.lines.encoder.Encode(r) }
func (w *jsonRowWriter) Close() error      { return w.lines.close() }
func (*jsonRowWriter) Names() []string     { return []string{"slices.jsonl"} }

// Feature columns follow model.Features JSON names and declaration order.
// Empty nullable fields retain the JSONL null semantics.
func CompactColumns() []string {
	cols := []string{"bucket", "object_key", "version_id", "at", "size_bytes", "rules_version"}
	t := reflect.TypeOf(model.Features{})
	for i := 0; i < t.NumField(); i++ {
		cols = append(cols, t.Field(i).Tag.Get("json"))
	}
	return append(cols, "label_reads", "label_has_read", "eligible", "split", "ewma_forecast", "feature_usable", "feature_reason", "label_usable", "label_reason", "forecast_usable", "forecast_reason", "label_available_at", "horizon_seconds", "method_version")
}

func compactValues(r Row) []string {
	s := r.Snapshot
	values := []string{s.Bucket, s.ObjectKey, s.VersionID, s.At.Format(time.RFC3339Nano), strconv.FormatInt(s.SizeBytes, 10), s.RulesVersion}
	v := reflect.ValueOf(s.Features)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Pointer {
			if field.IsNil() {
				values = append(values, "")
				continue
			}
			field = field.Elem()
		}
		if field.Kind() == reflect.Int64 {
			values = append(values, strconv.FormatInt(field.Int(), 10))
		} else {
			values = append(values, strconv.FormatFloat(field.Float(), 'g', -1, 64))
		}
	}
	reads, has := "", ""
	if r.Label.Reads != nil {
		reads = strconv.FormatInt(*r.Label.Reads, 10)
	}
	if r.Label.HasRead != nil {
		has = strconv.FormatBool(*r.Label.HasRead)
	}
	return append(values, reads, has, strconv.FormatBool(r.Eligible), r.Split, strconv.FormatFloat(r.Forecast.ExpectedReads, 'g', -1, 64), strconv.FormatBool(s.Quality.Usable), s.Quality.Reason, strconv.FormatBool(r.Label.Quality.Usable), r.Label.Quality.Reason, strconv.FormatBool(r.Forecast.Quality.Usable), r.Forecast.Quality.Reason, r.Label.AvailableAt.Format(time.RFC3339Nano), strconv.FormatInt(r.Forecast.HorizonSeconds, 10), r.Forecast.MethodVersion)
}

type csvSplit struct {
	f   *os.File
	gz  *gzip.Writer
	csv *csv.Writer
}
type compactWriter struct{ splits map[string]*csvSplit }

func openRows(dir, format string) (rowWriter, error) {
	if format == "jsonl" {
		w, err := openLines(filepath.Join(dir, "slices.jsonl"))
		return &jsonRowWriter{w}, err
	}
	w := &compactWriter{splits: map[string]*csvSplit{}}
	for _, split := range []string{"train", "validation", "test", "purged"} {
		f, err := os.OpenFile(filepath.Join(dir, split+".csv.gz"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			_ = w.Close()
			return nil, err
		}
		gz := gzip.NewWriter(f)
		c := csv.NewWriter(gz)
		w.splits[split] = &csvSplit{f, gz, c}
		if err := c.Write(CompactColumns()); err != nil {
			_ = w.Close()
			return nil, err
		}
	}
	return w, nil
}
func (w *compactWriter) Write(r Row) error {
	s, ok := w.splits[r.Split]
	if !ok {
		return fmt.Errorf("unknown split %q", r.Split)
	}
	return s.csv.Write(compactValues(r))
}
func (w *compactWriter) Close() error {
	var first error
	for _, s := range w.splits {
		s.csv.Flush()
		for _, err := range []error{s.csv.Error(), s.gz.Close(), s.f.Close()} {
			if first == nil {
				first = err
			}
		}
	}
	return first
}
func (*compactWriter) Names() []string {
	return []string{"train.csv.gz", "validation.csv.gz", "test.csv.gz", "purged.csv.gz"}
}
