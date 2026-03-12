import os
import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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

MIGRATE_SQL = """
CREATE TABLE IF NOT EXISTS vessel_profiles (
    vessel_id               TEXT PRIMARY KEY,
    name                    TEXT,
    vessel_type             TEXT,
    flag                    TEXT,
    call_sign               TEXT,
    mmsi                    TEXT,
    imo                     TEXT,
    registration_number     TEXT,
    year_built              INTEGER,
    builder                 TEXT,
    loa_m                   FLOAT,
    beam_m                  FLOAT,
    draft_m                 FLOAT,
    hull_material           TEXT,
    engine_manufacturer     TEXT,
    engine_model            TEXT,
    engine_type             TEXT,
    engine_power_kw         FLOAT,
    engine_serial           TEXT,
    engine_year             INTEGER,
    num_engines             INTEGER DEFAULT 1,
    battery_capacity_ah     FLOAT,
    battery_type            TEXT,
    nmea_product_name       TEXT,
    nmea_manufacturer_code  TEXT,
    nmea_model_id           TEXT,
    nmea_software_version   TEXT,
    nmea_serial_number      TEXT,
    first_seen              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    notes                   TEXT
);
"""

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
    async with db_pool.acquire() as conn:
        await conn.execute(MIGRATE_SQL)
        logger.info("Migrations applied")
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
        rows = await conn.fetch("SELECT vessel_id, MAX(time) as last_seen FROM telemetry GROUP BY vessel_id")
        return [dict(r) for r in rows]

@app.get("/api/vessels/{vessel_id}")
async def get_vessel(vessel_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT vessel_id, MAX(time) as last_seen FROM telemetry WHERE vessel_id = $1 GROUP BY vessel_id", vessel_id)
        if not row:
            raise HTTPException(404, "Vessel not found")
        return dict(row)

@app.get("/api/vessels/{vessel_id}/telemetry")
async def get_telemetry(vessel_id: str, topic: str = None, limit: int = 500, since: Optional[str] = None):
    async with db_pool.acquire() as conn:
        since_dt = datetime.fromisoformat(since) if since else None
        if topic and since_dt:
            rows = await conn.fetch(
                "SELECT * FROM telemetry WHERE vessel_id = $1 AND topic = $2 AND time >= $3 ORDER BY time DESC LIMIT $4",
                vessel_id, topic, since_dt, limit
            )
        elif topic:
            rows = await conn.fetch(
                "SELECT * FROM telemetry WHERE vessel_id = $1 AND topic = $2 ORDER BY time DESC LIMIT $3",
                vessel_id, topic, limit
            )
        elif since_dt:
            rows = await conn.fetch(
                "SELECT * FROM telemetry WHERE vessel_id = $1 AND time >= $2 ORDER BY time DESC LIMIT $3",
                vessel_id, since_dt, limit
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
                row = await conn.fetchrow(
                    "SELECT vessel_id, MAX(time) as last_seen FROM telemetry WHERE vessel_id = $1 GROUP BY vessel_id",
                    vessel_id
                )
                if row:
                    data = {"vessel_id": vessel_id, "online": True}
                    data["last_seen"] = row["last_seen"].isoformat()
                    topics = await conn.fetch(
                        """SELECT DISTINCT ON (topic) topic, payload
                           FROM telemetry WHERE vessel_id = $1
                           ORDER BY topic, time DESC""",
                        vessel_id
                    )
                    import json as _json
                    for t in topics:
                        p = _json.loads(t["payload"]) if isinstance(t["payload"], str) else t["payload"]
                        fields = p.get("fields", {})
                        topic = t["topic"]
                        if topic == "engine/rapid":
                            data["rpm"] = fields.get("rpm")
                        elif topic == "engine/parameters":
                            data["coolant_temp_c"] = fields.get("coolant_temp_c")
                        elif topic == "electrical/battery":
                            data["battery_voltage"] = fields.get("voltage")
                            data["battery_soc"] = fields.get("state_of_charge")
                        elif topic == "navigation/gnss":
                            data["latitude"] = fields.get("latitude")
                            data["longitude"] = fields.get("longitude")
                        elif topic == "navigation/depth":
                            data["depth_m"] = fields.get("depth_m")
                    await websocket.send_json(data)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

# ── Pydantic models ───────────────────────────────────────────────────────────

class AlertRuleCreate(BaseModel):
    name: str
    metric: str
    operator: str
    threshold: float
    severity: str = "warning"

class AlertRuleToggle(BaseModel):
    enabled: bool

class VesselProfileUpdate(BaseModel):
    name: Optional[str] = None
    vessel_type: Optional[str] = None
    flag: Optional[str] = None
    call_sign: Optional[str] = None
    mmsi: Optional[str] = None
    imo: Optional[str] = None
    registration_number: Optional[str] = None
    year_built: Optional[int] = None
    builder: Optional[str] = None
    loa_m: Optional[float] = None
    beam_m: Optional[float] = None
    draft_m: Optional[float] = None
    hull_material: Optional[str] = None
    engine_manufacturer: Optional[str] = None
    engine_model: Optional[str] = None
    engine_type: Optional[str] = None
    engine_power_kw: Optional[float] = None
    engine_serial: Optional[str] = None
    engine_year: Optional[int] = None
    num_engines: Optional[int] = None
    battery_capacity_ah: Optional[float] = None
    battery_type: Optional[str] = None
    notes: Optional[str] = None

# ── Vessel profiles ───────────────────────────────────────────────────────────

@app.get("/api/settings/profiles")
async def list_profiles():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT vp.*, t.last_seen
            FROM vessel_profiles vp
            LEFT JOIN (
                SELECT vessel_id, MAX(time) as last_seen
                FROM telemetry GROUP BY vessel_id
            ) t ON t.vessel_id = vp.vessel_id
            ORDER BY t.last_seen DESC NULLS LAST
        """)
        result = []
        for r in rows:
            d = dict(r)
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            result.append(d)
        return result

@app.get("/api/vessels/{vessel_id}/profile")
async def get_profile(vessel_id: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vessel_profiles WHERE vessel_id = $1", vessel_id
        )
        if not row:
            raise HTTPException(404, "Profile not found")
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

@app.put("/api/vessels/{vessel_id}/profile")
async def upsert_profile(vessel_id: str, body: VesselProfileUpdate):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO vessel_profiles (vessel_id) VALUES ($1) ON CONFLICT DO NOTHING",
            vessel_id
        )
        if fields:
            set_parts = [f"{k} = ${i+2}" for i, k in enumerate(fields)]
            set_parts.append("updated_at = NOW()")
            await conn.execute(
                f"UPDATE vessel_profiles SET {', '.join(set_parts)} WHERE vessel_id = $1",
                vessel_id, *fields.values()
            )
        row = await conn.fetchrow("SELECT * FROM vessel_profiles WHERE vessel_id = $1", vessel_id)
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

# ── Alert rules ───────────────────────────────────────────────────────────────

@app.get("/api/vessels/{vessel_id}/alert-rules")
async def list_alert_rules(vessel_id: str):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM alert_rules WHERE vessel_id = $1 ORDER BY created_at DESC",
            vessel_id
        )
        return [dict(r) for r in rows]

@app.post("/api/vessels/{vessel_id}/alert-rules", status_code=201)
async def create_alert_rule(vessel_id: str, rule: AlertRuleCreate):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO alert_rules (vessel_id, name, metric, operator, threshold, severity)
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING *""",
            vessel_id, rule.name, rule.metric, rule.operator, rule.threshold, rule.severity
        )
        return dict(row)

@app.patch("/api/alert-rules/{rule_id}")
async def toggle_alert_rule(rule_id: int, body: AlertRuleToggle):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE alert_rules SET enabled = $1 WHERE id = $2 RETURNING *",
            body.enabled, rule_id
        )
        if not row:
            raise HTTPException(404, "Rule not found")
        return dict(row)

@app.delete("/api/alert-rules/{rule_id}", status_code=204)
async def delete_alert_rule(rule_id: int):
    async with db_pool.acquire() as conn:
        result = await conn.execute("DELETE FROM alert_rules WHERE id = $1", rule_id)
        if result == "DELETE 0":
            raise HTTPException(404, "Rule not found")

# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/api/alerts")
async def list_alerts(
    vessel_id: Optional[str] = None,
    active: Optional[bool] = None,
    limit: int = Query(default=50, le=200)
):
    async with db_pool.acquire() as conn:
        conditions, params = [], []
        if vessel_id:
            params.append(vessel_id)
            conditions.append(f"vessel_id = ${len(params)}")
        if active is True:
            conditions.append("resolved_at IS NULL")
        elif active is False:
            conditions.append("resolved_at IS NOT NULL")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = await conn.fetch(
            f"SELECT * FROM alerts {where} ORDER BY triggered_at DESC LIMIT ${len(params)}",
            *params
        )
        result = []
        for r in rows:
            d = dict(r)
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            result.append(d)
        return result

@app.post("/api/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE alerts SET acknowledged = TRUE, acknowledged_at = NOW()
               WHERE id = $1 RETURNING id""",
            alert_id
        )
        if not row:
            raise HTTPException(404, "Alert not found")
        return {"ok": True}

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