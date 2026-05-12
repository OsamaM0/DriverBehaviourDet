"""OpenTelemetry tracing setup. Call `init_tracing()` once per process."""
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from packages.common.config import settings

_initialized = False


def init_tracing(service_name: str | None = None) -> trace.Tracer:
    global _initialized
    name = service_name or settings.otel_service_name
    if not _initialized:
        provider = TracerProvider(resource=Resource.create({"service.name": name, "service.env": settings.env}))
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint, insecure=True))
        )
        trace.set_tracer_provider(provider)
        _initialized = True
    return trace.get_tracer(name)


def tracer(name: str | None = None) -> trace.Tracer:
    return trace.get_tracer(name or settings.otel_service_name)
