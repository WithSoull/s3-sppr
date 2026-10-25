package tracing

import (
	"context"
	"io"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type scriptedStream struct {
	grpc.ClientStream
	recv []error
}

func (s *scriptedStream) SendMsg(any) error { return nil }

func (s *scriptedStream) RecvMsg(any) error {
	err := s.recv[0]
	s.recv = s.recv[1:]
	return err
}

// newTracedStream runs the interceptor over a scripted stream and returns the stream, the spans
// recorded so far and the outgoing metadata the stream was opened with.
func newTracedStream(t *testing.T, ctx context.Context, desc *grpc.StreamDesc, recv ...error) (grpc.ClientStream, *tracetest.SpanRecorder, *metadata.MD) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	var outgoing metadata.MD
	stream, err := StreamClientInterceptor("test")(ctx, desc, nil, "/svc/Method",
		func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			outgoing, _ = metadata.FromOutgoingContext(ctx)
			return &scriptedStream{recv: recv}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return stream, recorder, &outgoing
}

func TestStreamClientInterceptor(t *testing.T) {
	failure := status.Error(codes.Internal, "boom")
	tests := []struct {
		name      string
		desc      *grpc.StreamDesc
		recv      []error
		reads     int
		wantEnded bool
		wantError bool
	}{
		{name: "server stream is open until EOF", desc: &grpc.StreamDesc{ServerStreams: true}, recv: []error{nil, nil, io.EOF}, reads: 2},
		{name: "server stream ends at EOF", desc: &grpc.StreamDesc{ServerStreams: true}, recv: []error{nil, io.EOF}, reads: 2, wantEnded: true},
		{name: "server stream ends on failure", desc: &grpc.StreamDesc{ServerStreams: true}, recv: []error{nil, failure}, reads: 2, wantEnded: true, wantError: true},
		{name: "client stream ends with its single response", desc: &grpc.StreamDesc{ClientStreams: true}, recv: []error{nil}, reads: 1, wantEnded: true},
		{name: "client stream response failure", desc: &grpc.StreamDesc{ClientStreams: true}, recv: []error{failure}, reads: 1, wantEnded: true, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, recorder, _ := newTracedStream(t, context.Background(), tt.desc, tt.recv...)
			for i := 0; i < tt.reads; i++ {
				_ = stream.RecvMsg(nil)
			}
			ended := recorder.Ended()
			if (len(ended) == 1) != tt.wantEnded {
				t.Fatalf("ended spans = %d, want ended=%v", len(ended), tt.wantEnded)
			}
			if tt.wantEnded && (ended[0].Status().Code == 0) == tt.wantError {
				t.Fatalf("span status = %v, want error=%v", ended[0].Status(), tt.wantError)
			}
		})
	}
}

func TestStreamClientInterceptorEndsSpanWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, recorder, _ := newTracedStream(t, ctx, &grpc.StreamDesc{ServerStreams: true}, nil)
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for len(recorder.Ended()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("span was not ended after the stream context was cancelled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if code := recorder.Ended()[0].Status().Code; code != 0 {
		t.Fatalf("cancellation by the caller is recorded as an error: %v", code)
	}
}

func TestStreamClientInterceptorPropagatesTraceContext(t *testing.T) {
	_, _, outgoing := newTracedStream(t, context.Background(), &grpc.StreamDesc{ServerStreams: true}, io.EOF)
	if len(outgoing.Get("traceparent")) != 1 {
		t.Fatalf("outgoing metadata = %v, want a traceparent", *outgoing)
	}
}
