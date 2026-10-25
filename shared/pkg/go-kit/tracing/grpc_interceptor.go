package tracing

import (
	"context"
	"errors"
	"io"
	"sync"

	traceidctx "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/traceIDctx"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const TraceIDHeader string = "x-trace_id"

func UnaryServerInterceptor(serviceName string) grpc.UnaryServerInterceptor {
	tracer := otel.GetTracerProvider().Tracer(serviceName)
	propagator := otel.GetTextMapPropagator()

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.New(nil)
		}

		ctx = propagator.Extract(ctx, metadataCarrier(md))

		ctx, span := tracer.Start(
			ctx,
			info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		ctx = AddTraceIDToResponse(ctx)

		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
		}

		return resp, err
	}
}

func UnaryClientInterceptor(serviceName string) grpc.UnaryClientInterceptor {
	tracer := otel.GetTracerProvider().Tracer(serviceName)
	propagator := otel.GetTextMapPropagator()

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		spanName := formatSpanName(ctx, method)

		ctx, span := tracer.Start(
			ctx,
			spanName,
			trace.WithSpanKind(trace.SpanKindClient),
		)
		defer span.End()

		carrier := metadataCarrier(traceidctx.ExtractOutgoingMetadata(ctx))
		propagator.Inject(ctx, carrier)
		ctx = metadata.NewOutgoingContext(ctx, metadata.MD(carrier))

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			trace.SpanFromContext(ctx).RecordError(err)
		}

		return err
	}
}

// StreamClientInterceptor traces streaming client calls. The span lasts as long as the stream:
// it ends when the final response arrives, when Recv fails or returns io.EOF, or when the stream
// context is done, so callers must cancel the context of a stream they stop reading.
func StreamClientInterceptor(serviceName string) grpc.StreamClientInterceptor {
	tracer := otel.GetTracerProvider().Tracer(serviceName)
	propagator := otel.GetTextMapPropagator()

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, span := tracer.Start(
			ctx,
			formatSpanName(ctx, method),
			trace.WithSpanKind(trace.SpanKindClient),
		)

		carrier := metadataCarrier(traceidctx.ExtractOutgoingMetadata(ctx))
		propagator.Inject(ctx, carrier)
		ctx = metadata.NewOutgoingContext(ctx, metadata.MD(carrier))

		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			endSpan(span, err)
			return nil, err
		}

		traced := &tracedClientStream{ClientStream: stream, desc: desc, span: span, done: make(chan struct{})}
		go func() {
			select {
			case <-ctx.Done():
				// A reader that stops early cancels the stream; that is not a failure of the call.
				if err := ctx.Err(); !errors.Is(err, context.Canceled) {
					traced.finish(err)
					return
				}
				traced.finish(nil)
			case <-traced.done:
			}
		}()
		return traced, nil
	}
}

type tracedClientStream struct {
	grpc.ClientStream
	desc *grpc.StreamDesc
	span trace.Span
	once sync.Once
	done chan struct{}
}

func (s *tracedClientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	// io.EOF here only says the server ended the stream; the real status comes from RecvMsg.
	if err != nil && !errors.Is(err, io.EOF) {
		s.finish(err)
	}
	return err
}

func (s *tracedClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil || !s.desc.ServerStreams {
		s.finish(err)
	}
	return err
}

func (s *tracedClientStream) finish(err error) {
	s.once.Do(func() {
		close(s.done)
		endSpan(s.span, err)
	})
}

func endSpan(span trace.Span, err error) {
	if err != nil && !errors.Is(err, io.EOF) {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	span.End()
}

func formatSpanName(ctx context.Context, method string) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return "client." + method
	}

	return method
}

// AddTraceIDToResponse adds the trace ID to the outgoing gRPC response metadata.
// This enables the client to retrieve the trace ID for subsequent lookup in the tracing system.
func AddTraceIDToResponse(ctx context.Context) context.Context {
	traceID, ok := traceidctx.ExtractTraceIDFromSpan(ctx)
	if !ok {
		return ctx
	}

	md := traceidctx.ExtractOutgoingMetadata(ctx)

	md.Set(TraceIDHeader, traceID)
	return metadata.NewOutgoingContext(ctx, md)
}
