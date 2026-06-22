import os
import json
import logging
import asyncio
import threading
from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.tl.functions.channels import GetForumTopicsRequest
from telethon.tl.types import InputChannel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Telegram Personal Account Credentials ──────────────────────────────────
API_ID = int(os.environ.get("API_ID", "30752070"))
API_HASH = os.environ.get("API_HASH", "45d346751438ce944b988fb54bed5ae1")
PHONE = os.environ.get("PHONE", "+447520676563")

# ── Vantage Group Settings ──────────────────────────────────────────────────
VANTAGE_GROUP_ID = int(os.environ.get("VANTAGE_GROUP_ID", "-1002147262822"))
VANTAGE_TOPIC_ID = int(os.environ.get("VANTAGE_TOPIC_ID", "0"))  # Set after first run

# ── Session file stored in /data for persistence ────────────────────────────
SESSION_PATH = "/data/charlie_session"

# ── Global Telethon client ───────────────────────────────────────────────────
client = TelegramClient(SESSION_PATH, API_ID, API_HASH)
loop = asyncio.new_event_loop()


def run_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=run_loop, daemon=True).start()


async def start_client():
    await client.start(phone=PHONE)
    logger.info("✅ Telethon client started as personal account")


async def get_topics():
    """List all forum topics in Vantage group to find Chat-General ID"""
    try:
        result = await client(GetForumTopicsRequest(
            channel=VANTAGE_GROUP_ID,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=50
        ))
        topics = []
        for topic in result.topics:
            topics.append({
                "id": topic.id,
                "title": topic.title
            })
        return topics
    except Exception as e:
        logger.error(f"Error getting topics: {e}")
        return []


async def send_to_vantage(message_text):
    """Send message to Vantage Chat-General as personal account"""
    try:
        entity = await client.get_entity(VANTAGE_GROUP_ID)

        if VANTAGE_TOPIC_ID and VANTAGE_TOPIC_ID > 0:
            # Send to specific topic (Chat-General)
            await client.send_message(
                entity,
                message_text,
                reply_to=VANTAGE_TOPIC_ID
            )
        else:
            # Send to general chat if no topic ID set
            await client.send_message(entity, message_text)

        logger.info(f"✅ Message sent to Vantage group")
        return True
    except Exception as e:
        logger.error(f"❌ Error sending to Vantage: {e}")
        return False


# ── Start client on boot ─────────────────────────────────────────────────────
future = asyncio.run_coroutine_threadsafe(start_client(), loop)
try:
    future.result(timeout=30)
except Exception as e:
    logger.error(f"Client start error: {e}")


# ── Flask Routes ─────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Charlie Vantage Forwarder running ✅",
        "vantage_group": VANTAGE_GROUP_ID,
        "topic_id": VANTAGE_TOPIC_ID
    })


@app.route("/get_topics", methods=["GET"])
def list_topics():
    """Call this once to find the Chat-General topic ID"""
    future = asyncio.run_coroutine_threadsafe(get_topics(), loop)
    try:
        topics = future.result(timeout=15)
        return jsonify({"topics": topics})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/forward_tp", methods=["POST"])
def forward_tp():
    """
    Receives TP message from your signals bot and sends to Vantage group.
    Expected JSON:
    {
        "close_type": "TP1",
        "pair": "XAUUSD",
        "message": "GOLD SMASHED TP1 ✅✅✅ ..."
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data"}), 400

    close_type = data.get("close_type", "")
    pair = data.get("pair", "")
    message = data.get("message", "")

    if not message:
        return jsonify({"error": "No message provided"}), 400

    # Only forward TP messages, not SL or BE
    allowed = ["TP1", "TP2", "TP3"]
    if close_type not in allowed:
        return jsonify({"status": "skipped", "reason": f"{close_type} not forwarded"}), 200

    # Build clean message without any buttons (plain text only)
    vantage_message = message

    future = asyncio.run_coroutine_threadsafe(
        send_to_vantage(vantage_message), loop
    )
    try:
        success = future.result(timeout=15)
        if success:
            return jsonify({"status": "sent ✅", "close_type": close_type})
        else:
            return jsonify({"status": "failed ❌"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["POST"])
def test_message():
    """Send a test message to Vantage group to verify everything works"""
    data = request.get_json() or {}
    message = data.get("message", "✅ Test message from Charlie's signals system")

    future = asyncio.run_coroutine_threadsafe(
        send_to_vantage(message), loop
    )
    try:
        success = future.result(timeout=15)
        if success:
            return jsonify({"status": "Test message sent ✅"})
        else:
            return jsonify({"status": "Failed ❌"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
