import os
import json
import time
import logging
import psycopg2
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bridge")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "boats")
DB_USER = os.getenv("DB_USER", "boatplatform")
DB_PASSWORD = os.getenv("DB_PASSWORD", "changeme_db")

def get_db_connection():
    for attempt in range(30):
        try:
            return psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD
            )
        except Exception:
            logger.warning("DB not ready, retrying... (%d/30)", attempt + 1)
            time.sleep(2)
    raise RuntimeError("Could not connect to database")

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe("vessels/#", qos=1)
    else:
        logger.error("MQTT connection failed: %s", reason_code)

def on_message(client, userdata, msg):
    conn = userdata["db"]
    try:
        payload = json.loads(msg.payload.decode())
    except Exception:
        return

    parts = msg.topic.split("/")
    if len(parts) < 3 or parts[0] != "vessels":
        return

    vessel_id = parts[1]
    sub_topic = "/".join(parts[2:])
    pgn = payload.get("pgn", 0)
    fields = payload.get("fields", {})

    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO telemetry (time, vessel_id, topic, pgn, payload) VALUES (NOW(), %s, %s, %s, %s)",
                (vessel_id, sub_topic, pgn, json.dumps(payload))
            )

            updates = {}
            if sub_topic == "engine/rapid":
                updates["rpm"] = fields.get("rpm")
            elif sub_topic == "engine/parameters":
                updates["coolant_temp_c"] = fields.get("coolant_temp_c")
            elif sub_topic == "electrical/battery":
                updates["battery_voltage"] = fields.get("voltage")
                updates["battery_soc"] = fields.get("state_of_charge")
            elif sub_topic == "navigation/gnss":
                updates["latitude"] = fields.get("latitude")
                updates["longitude"] = fields.get("longitude")
            elif sub_topic == "navigation/depth":
                updates["depth_m"] = fields.get("depth_m")

            if updates:
                set_clause = ", ".join([f"{k} = %s" for k in updates])
                cur.execute(
                    f"""INSERT INTO vessel_status (vessel_id, online, last_seen, updated_at, {', '.join(updates)})
                        VALUES (%s, TRUE, NOW(), NOW(), {', '.join(['%s'] * len(updates))})
                        ON CONFLICT (vessel_id) DO UPDATE SET online=TRUE, last_seen=NOW(), updated_at=NOW(), {set_clause}""",
                    [vessel_id] + list(updates.values()) + list(updates.values())
                )
        conn.commit()
    except Exception:
        logger.exception("DB write failed")
        conn.rollback()

def main():
    conn = get_db_connection()
    logger.info("Connected to TimescaleDB")

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id="boat-bridge")
    client.user_data_set({"db": conn})
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_forever()

if __name__ == "__main__":
    main()
