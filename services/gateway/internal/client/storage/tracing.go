package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func finishSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil && !errors.Is(err, io.EOF) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func withTraceContext(ctx context.Context) context.Context {
	header := make(http.Header)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
	outgoing, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		outgoing = outgoing.Copy()
	} else {
		outgoing = metadata.New(nil)
	}
	for key, values := range header {
		outgoing.Append(strings.ToLower(key), values...)
	}
	return metadata.NewOutgoingContext(ctx, outgoing)
}
