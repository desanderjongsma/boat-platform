import os
import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'boatplatform')}"
    f":{os.getenv('DB_PASSWORD', 'changeme_db')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'boats')}"
)

db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    for attempt in range(30):
        try:
            db_pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
            logger.info("Database pool created")
            break
        except Exception:
            logger.warning("DB not ready, retrying... (%d/30)", attempt + 1)
            await asyncio.sleep(2)
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(title="Connected Boat Platform API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/vessels")
async def list_vessels():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM vessel_status")
        return [dict(r) for r in rows]

@app.get("/api/vessels/{vessel_id}")
async def get_vessel(vessel_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM vessel_status WHERE vessel_id = $1", vessel_id)
        if not row:
            raise HTTPException(404, "Vessel not found")
        return dict(row)

@app.get("/api/vessels/{vessel_id}/telemetry")
async def get_telemetry(vessel_id: str, topic: str = None, limit: int = 100):
    async with db_pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                "SELECT * FROM telemetry WHERE vessel_id = $1 AND topic = $2 ORDER BY time DESC LIMIT $3",
                vessel_id, topic, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM telemetry WHERE vessel_id = $1 ORDER BY time DESC LIMIT $2",
                vessel_id, limit
            )
        return [dict(r) for r in rows]

@app.websocket("/ws/{vessel_id}")
async def websocket_endpoint(websocket: WebSocket, vessel_id: str):
    await websocket.accept()
    try:
        while True:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM vessel_status WHERE vessel_id = $1", vessel_id)
                if row:
                    data = dict(row)
                    for key, val in data.items():
                        if isinstance(val, datetime):
                            data[key] = val.isoformat()
                    await websocket.send_json(data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

@app.get("/api/health")
async def health_check():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception:
        raise HTTPException(503, "Database unavailable")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
