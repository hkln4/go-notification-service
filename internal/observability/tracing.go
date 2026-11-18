package observability

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/hkln4/go-notification-service"

// SetupTracing exports spans over OTLP/HTTP to endpoint (Jaeger accepts
// OTLP directly). With an empty endpoint spans are not exported, but trace
// context is still propagated, so a downstream service can pick it up.
// The returned function flushes buffered spans; call it on shutdown.
func SetupTracing(ctx context.Context, service, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(service))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the service's tracer from the global provider, so it picks
// up whatever SetupTracing (or a test) installed.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// InjectMap writes the current trace context into m (outbox headers).
func InjectMap(ctx context.Context, m map[string]string) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(m))
}

// ExtractMap returns ctx carrying the trace context stored in m.
func ExtractMap(ctx context.Context, m map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m))
}

// InjectAMQP writes the current trace context into message headers.
func InjectAMQP(ctx context.Context, h amqp.Table) {
	otel.GetTextMapPropagator().Inject(ctx, amqpCarrier(h))
}

// ExtractAMQP returns ctx carrying the trace context from message headers.
func ExtractAMQP(ctx context.Context, h amqp.Table) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, amqpCarrier(h))
}

// amqpCarrier adapts AMQP headers to the propagation API.
type amqpCarrier amqp.Table

func (c amqpCarrier) Get(key string) string {
	v, _ := c[key].(string)
	return v
}

func (c amqpCarrier) Set(key, value string) { c[key] = value }

func (c amqpCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
