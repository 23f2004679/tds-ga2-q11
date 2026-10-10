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
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=[
        "Content-Type",
        "Authorization",
        "Idempotency-Key",
        "X-Client-Id",
    ],
    expose_headers=["Retry-After"],
)

catalog = [
    {"id": i, "product": f"Product {i}", "quantity": 1}
    for i in range(1, TOTAL_ORDERS + 1)
]

idempotency_keys: dict[str, dict[str, Any]] = {}
next_order_id = 1
order_lock = Lock()

client_requests = defaultdict(deque)
rate_limit_lock = Lock()


@app.middleware("http")
async def per_client_rate_limit(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id")

    if not client_id:
        response = await call_next(request)
        if response.status_code == 429 and "Retry-After" not in response.headers:
            response.headers["Retry-After"] = str(WINDOW_SECONDS)
        return response

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

            response = JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
            )
            response.headers["Retry-After"] = str(retry_after)
            response.headers["Access-Control-Allow-Origin"] = "*"

            return response

        timestamps.append(now)

    response = await call_next(request)

    # Ensure every HTTP 429 response includes Retry-After.
    if response.status_code == 429 and "Retry-After" not in response.headers:
        response.headers["Retry-After"] = str(WINDOW_SECONDS)

    return response


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
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid cursor")

        if not 0 <= start_id <= TOTAL_ORDERS:
            raise HTTPException(status_code=400, detail="Invalid cursor")

    page = [
        item for item in catalog if item["id"] > start_id
    ][:limit]

    next_cursor = str(page[-1]["id"]) if page else None

    return {
        "items": page,
        "next_cursor": next_cursor,
    }
