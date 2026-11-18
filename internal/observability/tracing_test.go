package observability

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Trace context survives map → AMQP headers → context, the path a trace
// takes from the outbox row to the worker.
func TestTraceContextRoundTrip(t *testing.T) {
	_, err := SetupTracing(context.Background(), "test", "")
	require.NoError(t, err)
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "api request")
	defer span.End()

	outbox := map[string]string{}
	InjectMap(ctx, outbox)
	require.Contains(t, outbox, "traceparent")

	headers := amqp.Table{}
	for k, v := range outbox {
		headers[k] = v
	}
	got := trace.SpanContextFromContext(ExtractAMQP(context.Background(), headers))
	require.Equal(t, span.SpanContext().TraceID(), got.TraceID())

	again := amqp.Table{}
	InjectAMQP(ExtractMap(context.Background(), outbox), again)
	require.Equal(t, outbox["traceparent"], again["traceparent"])
}
