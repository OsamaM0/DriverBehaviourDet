"""
Async Triton client wrapper.

We use the official `tritonclient.grpc.aio` client and discover model I/O
metadata once on first call. Each call submits a single request — Triton
itself does dynamic batching server-side, which is the right place for it
(per-model `max_batch_size` + `max_queue_delay_microseconds`).

For request-side batching across many concurrent stream consumers we rely
on Triton's batcher, not on a Python coroutine fan-in (avoids GIL pressure).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import tritonclient.grpc.aio as triton_aio
from tritonclient.utils import np_to_triton_dtype

from packages.common.config import settings
from packages.common.obs import get_logger
from packages.common.obs.metrics import INFER_BATCH_SIZE, INFER_LATENCY, INFER_REQUESTS

log = get_logger(__name__)


@dataclass(slots=True)
class ModelIO:
    inputs: list[tuple[str, str, list[int]]]   # (name, dtype, shape)
    outputs: list[str]


class TritonClient:
    """One client per process; safe across asyncio tasks (gRPC is multiplexed)."""

    def __init__(self, url: str | None = None) -> None:
        self._url = url or settings.triton_url
        self._client: triton_aio.InferenceServerClient | None = None
        self._meta: dict[str, ModelIO] = {}

    async def _ensure(self) -> triton_aio.InferenceServerClient:
        if self._client is None:
            self._client = triton_aio.InferenceServerClient(url=self._url, verbose=False)
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def metadata(self, model: str, version: str = "") -> ModelIO:
        if model in self._meta:
            return self._meta[model]
        c = await self._ensure()
        meta: dict[str, Any] = await c.get_model_metadata(model_name=model, model_version=version, as_json=True)
        cfg = await c.get_model_config(model_name=model, model_version=version, as_json=True)
        # config holds dims; metadata holds dtype + names
        cfg_inputs = {i["name"]: i for i in cfg["config"]["input"]}
        inputs = []
        for inp in meta["inputs"]:
            name = inp["name"]
            dtype = inp["datatype"]
            dims = [int(d) for d in cfg_inputs[name]["dims"]]
            inputs.append((name, dtype, dims))
        outputs = [o["name"] for o in meta["outputs"]]
        io = ModelIO(inputs=inputs, outputs=outputs)
        self._meta[model] = io
        log.info("triton_model_loaded", model=model, inputs=inputs, outputs=outputs)
        return io

    async def infer(
        self,
        model: str,
        feeds: dict[str, np.ndarray],
        version: str = "",
    ) -> list[np.ndarray]:
        """Run inference. `feeds` keys must match model input names."""
        c = await self._ensure()
        meta = await self.metadata(model, version)

        triton_inputs = []
        first_batch = next(iter(feeds.values())).shape[0]
        for name, _dtype, _dims in meta.inputs:
            arr = feeds[name]
            ti = triton_aio.InferInput(name, list(arr.shape), np_to_triton_dtype(arr.dtype))
            ti.set_data_from_numpy(arr)
            triton_inputs.append(ti)
        triton_outputs = [triton_aio.InferRequestedOutput(o) for o in meta.outputs]

        INFER_BATCH_SIZE.labels(model=model).observe(first_batch)
        t0 = time.perf_counter()
        try:
            res = await c.infer(
                model_name=model,
                inputs=triton_inputs,
                outputs=triton_outputs,
                model_version=version,
            )
            INFER_REQUESTS.labels(model=model, result="ok").inc()
        except Exception:
            INFER_REQUESTS.labels(model=model, result="error").inc()
            raise
        finally:
            INFER_LATENCY.labels(model=model).observe(time.perf_counter() - t0)

        return [res.as_numpy(o) for o in meta.outputs]


_singleton: TritonClient | None = None


def get_triton() -> TritonClient:
    global _singleton
    if _singleton is None:
        _singleton = TritonClient()
    return _singleton
