from collections import defaultdict, deque
from threading import Lock
from time import monotonic
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="Q11 Orders API")

TOTAL_ORDERS = 54
RATE_LIMIT = 16
WINDOW_SECONDS = 10

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Fixed catalogue used by cursor pagination: IDs 1 through 54.
catalog = [
    {"id": i, "product": f"Product {i}", "quantity": 1}
    for i in range(1, TOTAL_ORDERS + 1)
]

# In-memory idempotency store.
idempotency_keys: dict[str, dict[str, Any]] = {}
next_order_id = 1
order_lock = Lock()

# Independent sliding-window rate-limit bucket per client.
client_requests = defaultdict(deque)
rate_limit_lock = Lock()


@app.middleware("http")
async def per_client_rate_limit(request: Request, call_next):
    # CORS preflight requests should not consume the client's quota.
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id")

    if client_id:
        now = monotonic()

        with rate_limit_lock:
            timestamps = client_requests[client_id]

            while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
                timestamps.popleft()

            if len(timestamps) >= RATE_LIMIT:
                retry_after = max(
                    1,
                    int(WINDOW_SECONDS - (now - timestamps[0]) + 0.999),
                )

                return JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded"},
                    headers={"Retry-After": str(retry_after)},
                )

            timestamps.append(now)

    return await call_next(request)


@app.get("/")
def home():
    return {"message": "Q11 Orders API is running"}


@app.post("/orders", status_code=201)
def create_order(
    order: dict[str, Any],
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    global next_order_id

    if not idempotency_key.strip():
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key cannot be empty",
        )

    with order_lock:
        # Reuse the original result for repeated keys.
        if idempotency_key in idempotency_keys:
            return idempotency_keys[idempotency_key]

        if next_order_id > TOTAL_ORDERS:
            raise HTTPException(
                status_code=409,
                detail="Order catalogue is full",
            )

        new_order = {
            **order,
            "id": next_order_id,
        }

        idempotency_keys[idempotency_key] = new_order
        next_order_id += 1

        return new_order


@app.get("/orders")
def list_orders(
    limit: int = Query(default=10, ge=1, le=TOTAL_ORDERS),
    cursor: str | None = None,
):
    start_id = 0

    if cursor is not None:
        try:
            start_id = int(cursor)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail="Invalid cursor",
            )

        if not 0 <= start_id <= TOTAL_ORDERS:
            raise HTTPException(
                status_code=400,
                detail="Invalid cursor",
            )

    page = [
        item for item in catalog if item["id"] > start_id
    ][:limit]

    next_cursor = str(page[-1]["id"]) if page else None

    return {
        "items": page,
        "next_cursor": next_cursor,
    }
