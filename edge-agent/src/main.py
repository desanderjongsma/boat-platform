import asyncio
import signal
import logging
import argparse
from pathlib import Path
import yaml
import can
import paho.mqtt.client as mqtt
import json
import time
import sqlite3
import struct
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger("edge_agent")

def decode_127488(data):
    if len(data) < 3: return {}
    rpm_raw = struct.unpack_from("<H", data, 1)[0]
    return {"rpm": round(rpm_raw * 0.25, 0) if rpm_raw != 0xFFFF else None}

def decode_127489(data):
    if len(data) < 8: return {}
    def u16(o): v = struct.unpack_from("<H", data, o)[0]; return None if v == 0xFFFF else v
    def kelvin(o): v = u16(o); return round(v * 0.01 - 273.15, 1) if v else None
    return {"oil_pressure_kpa": u16(1), "coolant_temp_c": kelvin(5)}

def decode_129029(data):
    if len(data) < 43: return {}
    lat = struct.unpack_from("<q", data, 7)[0] * 1e-16
    lon = struct.unpack_from("<q", data, 15)[0] * 1e-16
    return {"latitude": round(lat, 7), "longitude": round(lon, 7)}

def decode_129026(data):
    if len(data) < 8: return {}
    cog = struct.unpack_from("<H", data, 2)[0]
    sog = struct.unpack_from("<H", data, 4)[0]
    return {
        "cog_deg": round(cog * 0.0001 * 57.2958, 1) if cog != 0xFFFF else None,
        "sog_kn": round(sog * 0.01 * 1.944, 2) if sog != 0xFFFF else None
    }

def decode_128267(data):
    if len(data) < 5: return {}
    depth = struct.unpack_from("<I", data, 1)[0]
    return {"depth_m": round(depth * 0.01, 2) if depth != 0xFFFFFFFF else None}

def decode_127508(data):
    if len(data) < 8: return {}
    voltage = struct.unpack_from("<H", data, 2)[0]
    soc = struct.unpack_from("<H", data, 6)[0]
    return {
        "voltage": round(voltage * 0.01, 2) if voltage != 0xFFFF else None,
        "state_of_charge": round(soc * 0.004, 1) if soc != 0xFFFF else None
    }

PGN_DECODERS = {
    127488: decode_127488,
    127489: decode_127489,
    129029: decode_129029,
    129026: decode_129026,
    128267: decode_128267,
    127508: decode_127508,
}

def parse_n2k_frame(msg: can.Message):
    can_id = msg.arbitration_id
    pgn_raw = (can_id >> 8) & 0x3FFFF
    pgn = pgn_raw if (pgn_raw >> 8) >= 0xF0 else (pgn_raw & 0x1FF00)
    return pgn, can_id & 0xFF, 0xFF, msg.data

class Buffer:
    def __init__(self, db_path: str, max_messages: int = 100000):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("CREATE TABLE IF NOT EXISTS q (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT, payload TEXT, ts REAL)")
        self.conn.commit()
        self.max_messages = max_messages

    def push(self, topic: str, payload: str):
        self.conn.execute("INSERT INTO q (topic, payload, ts) VALUES (?,?,?)", (topic, payload, time.time()))
        self.conn.commit()

    def pop_batch(self, n: int = 50):
        return self.conn.execute("SELECT id, topic, payload FROM q ORDER BY id ASC LIMIT ?", (n,)).fetchall()

    def delete(self, ids):
        self.conn.execute(f"DELETE FROM q WHERE id IN ({','.join('?'*len(ids))})", ids)
        self.conn.commit()

    @property
    def size(self):
        return self.conn.execute("SELECT COUNT(*) FROM q").fetchone()[0]

class EdgeAgent:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.vessel_id = config["vessel"]["id"]
        self.running = False
        subs = config.get("pgn", {}).get("subscriptions", [])
        self.topic_map = {s["pgn"]: s["topic"] for s in subs}
        self.sample_intervals = {s["pgn"]: s.get("sample_interval", 0) for s in subs}
        self.last_publish: Dict[int, float] = {}
        buf_cfg = config.get("buffer", {})
        self.buffer = Buffer(buf_cfg.get("db_path", "/tmp/buffer.db"))
        mqtt_cfg = config["mqtt"]
        self.prefix = mqtt_cfg["topic_prefix"]
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=mqtt_cfg["client_id"])
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.connected = False
        self.client.connect_async(mqtt_cfg["broker"], mqtt_cfg["port"])
        self.client.loop_start()

    def _on_connect(self, client, userdata, flags, rc, props=None):
        self.connected = True
        logger.info("MQTT connected")

    def _on_disconnect(self, client, userdata, flags, rc, props=None):
        self.connected = False
        logger.warning("MQTT disconnected")

    def publish(self, sub_topic: str, payload: dict, pgn: int):
        now = time.time()
        if now - self.last_publish.get(pgn, 0) < self.sample_intervals.get(pgn, 0):
            return
        self.last_publish[pgn] = now
        topic = f"{self.prefix}/{sub_topic}"
        msg = json.dumps({"pgn": pgn, "timestamp": now, "fields": payload})
        if self.connected:
            self.client.publish(topic, msg, qos=1)
        else:
            self.buffer.push(topic, msg)

    async def drain_buffer(self):
        while self.running:
            if self.connected and self.buffer.size > 0:
                batch = self.buffer.pop_batch(50)
                for row_id, topic, payload in batch:
                    self.client.publish(topic, payload, qos=1)
                self.buffer.delete([r[0] for r in batch])
            await asyncio.sleep(2)

    async def heartbeat(self):
        interval = self.config.get("health", {}).get("heartbeat_interval", 30)
        while self.running:
            if self.connected:
                self.client.publish(
                    f"{self.prefix}/system/status",
                    json.dumps({"status": "online", "buffer_size": self.buffer.size, "timestamp": time.time()}),
                    qos=1
                )
            await asyncio.sleep(interval)

    async def run(self):
        self.running = True
        can_cfg = self.config["can"]
        bus = can.interface.Bus(
            channel=can_cfg["interface"],
            interface=can_cfg["bustype"],
            bitrate=can_cfg["bitrate"]
        )
        logger.info("CAN bus open on %s", can_cfg["interface"])
        loop = asyncio.get_event_loop()
        asyncio.create_task(self.drain_buffer())
        asyncio.create_task(self.heartbeat())
        loop.add_signal_handler(signal.SIGTERM, lambda: setattr(self, "running", False))
        loop.add_signal_handler(signal.SIGINT, lambda: setattr(self, "running", False))
        logger.info("Edge agent running — vessel: %s", self.vessel_id)
        while self.running:
            msg = await loop.run_in_executor(None, bus.recv, 1.0)
            if msg is None:
                continue
            pgn, src, dst, data = parse_n2k_frame(msg)
            decoder = PGN_DECODERS.get(pgn)
            if decoder:
                sub_topic = self.topic_map.get(pgn)
                if sub_topic:
                    fields = decoder(data)
                    if fields:
                        self.publish(sub_topic, fields, pgn)
        bus.shutdown()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="/opt/edge-agent/config/config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    logging.getLogger().setLevel(config.get("logging", {}).get("level", "INFO"))
    asyncio.run(EdgeAgent(config).run())

if __name__ == "__main__":
    main()
