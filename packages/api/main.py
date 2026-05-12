"""FastAPI app: tenants/streams CRUD, events, evidence, live WS."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from packages.api.routes import events, evidence, health, models, streams, tenants
from packages.api.ws.live_events import router as ws_router
from packages.common.config import settings
from packages.common.kafka import bus
from packages.common.obs import bootstrap


@asynccontextmanager
async def lifespan(app: FastAPI):
    bootstrap("api", metrics_port=9100)
    yield
    await bus.close()


app = FastAPI(title="Driver Analytics API", version="0.1.0", lifespan=lifespan)

origins = [o.strip() for o in settings.api_cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, tags=["health"])
app.include_router(tenants.router, prefix="/v1/tenants", tags=["tenants"])
app.include_router(streams.router, prefix="/v1/streams", tags=["streams"])
app.include_router(events.router, prefix="/v1/events", tags=["events"])
app.include_router(evidence.router, prefix="/v1/evidence", tags=["evidence"])
app.include_router(models.router, prefix="/v1/models", tags=["models"])
app.include_router(ws_router, tags=["ws"])
