// Package telemetry provides shared OpenTelemetry tracer setup,
// W3C TraceContext propagation, and structured context-aware logging.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracer initializes an OTLP gRPC trace exporter and registers the global TracerProvider
// and W3C TextMapPropagator. It returns a cleanup function that flushes and stops the provider.
func InitTracer(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	// Strip scheme if provided by environment
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create otlp trace exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("service.version", "1.0.0"),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	shutdown := func(shutdownCtx context.Context) error {
		return tp.Shutdown(shutdownCtx)
	}

	return tp, shutdown, nil
}

// InitMeter initializes an OTLP gRPC metric exporter and registers the global MeterProvider.
// It configures a periodic reader with a 5-second interval (configurable via OTEL_METRIC_EXPORT_INTERVAL).
func InitMeter(ctx context.Context, serviceName string) (*sdkmetric.MeterProvider, func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create otlp metric exporter: %w", err)
	}

	interval := 5 * time.Second
	if envInterval := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); envInterval != "" {
		if parsed, err := time.ParseDuration(envInterval); err == nil && parsed > 0 {
			interval = parsed
		}
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval))

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("service.version", "1.0.0"),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create metric resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	otel.SetMeterProvider(mp)

	shutdown := func(shutdownCtx context.Context) error {
		return mp.Shutdown(shutdownCtx)
	}

	return mp, shutdown, nil
}

// InitTelemetry initializes both TracerProvider and MeterProvider, registering them as globals.
// It returns a unified shutdown function that flushes and stops both providers.
func InitTelemetry(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	_, traceShutdown, traceErr := InitTracer(ctx, serviceName)
	_, metricShutdown, metricErr := InitMeter(ctx, serviceName)

	shutdown := func(shutdownCtx context.Context) error {
		var errs []string
		if traceShutdown != nil {
			if err := traceShutdown(shutdownCtx); err != nil {
				errs = append(errs, fmt.Sprintf("trace shutdown: %v", err))
			}
		}
		if metricShutdown != nil {
			if err := metricShutdown(shutdownCtx); err != nil {
				errs = append(errs, fmt.Sprintf("metric shutdown: %v", err))
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("%s", strings.Join(errs, "; "))
		}
		return nil
	}

	if traceErr != nil && metricErr != nil {
		return shutdown, fmt.Errorf("trace init error: %v, metric init error: %v", traceErr, metricErr)
	}
	if traceErr != nil {
		return shutdown, fmt.Errorf("trace init error: %w", traceErr)
	}
	if metricErr != nil {
		return shutdown, fmt.Errorf("metric init error: %w", metricErr)
	}

	return shutdown, nil
}

// InjectTraceparent serializes the active span context from ctx into a W3C traceparent string.
func InjectTraceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ExtractTraceparent deserializes a W3C traceparent string into a context.Context.
// If traceparent is empty, it returns the provided parentCtx unchanged.
func ExtractTraceparent(parentCtx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return parentCtx
	}
	carrier := propagation.MapCarrier{
		"traceparent": traceparent,
	}
	return otel.GetTextMapPropagator().Extract(parentCtx, carrier)
}
