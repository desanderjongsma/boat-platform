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

def check_condition(value, operator, threshold):
    if operator == 'gt':  return value > threshold
    if operator == 'lt':  return value < threshold
    if operator == 'gte': return value >= threshold
    if operator == 'lte': return value <= threshold
    return False

def evaluate_alerts(conn, vessel_id):
    """Check all enabled alert rules for a vessel against current status."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM vessel_status WHERE vessel_id = %s", (vessel_id,))
            row = cur.fetchone()
            if not row:
                return
            status = dict(zip([d[0] for d in cur.description], row))

            cur.execute(
                "SELECT id, name, metric, operator, threshold, severity "
                "FROM alert_rules WHERE vessel_id = %s AND enabled = TRUE",
                (vessel_id,)
            )
            rules = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

            for rule in rules:
                val = status.get(rule['metric'])
                if val is None:
                    continue
                triggered = check_condition(val, rule['operator'], rule['threshold'])

                cur.execute(
                    "SELECT id FROM alerts WHERE rule_id = %s AND resolved_at IS NULL",
                    (rule['id'],)
                )
                active = cur.fetchone()

                if triggered and not active:
                    cur.execute(
                        "INSERT INTO alerts (vessel_id, rule_id, rule_name, metric, value, "
                        "threshold, operator, severity) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (vessel_id, rule['id'], rule['name'], rule['metric'],
                         val, rule['threshold'], rule['operator'], rule['severity'])
                    )
                    logger.info("Alert triggered: %s (vessel=%s, value=%s)", rule['name'], vessel_id, val)
                elif not triggered and active:
                    cur.execute(
                        "UPDATE alerts SET resolved_at = NOW() WHERE id = %s", (active[0],)
                    )
                    logger.info("Alert resolved: %s (vessel=%s)", rule['name'], vessel_id)
        conn.commit()
    except Exception:
        logger.exception("Alert evaluation failed")
        conn.rollback()

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
        if updates:
            evaluate_alerts(conn, vessel_id)
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
