from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@router.get("/readyz")
async def readyz() -> dict:
    # TODO: add checks for kafka, redis, postgres, triton
    return {"ok": True}
