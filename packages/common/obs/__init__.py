"""Process bootstrap: configure logging, tracing, and Prom server in one call."""
from __future__ import annotations

from packages.common.obs.logging import configure_logging, get_logger
from packages.common.obs.metrics import start_metrics_server
from packages.common.obs.tracing import init_tracing, tracer


def bootstrap(service: str, metrics_port: int | None = None):
    configure_logging()
    init_tracing(service)
    start_metrics_server(metrics_port)
    log = get_logger(service)
    log.info("service_bootstrapped", service=service)
    return log


__all__ = ["bootstrap", "get_logger", "tracer"]
