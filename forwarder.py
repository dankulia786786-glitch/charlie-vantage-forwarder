import os
import json
import logging
import asyncio
import threading
from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Credentials ─────────────────────────────────────────────────────────────
API_ID = int(os.environ.get("API_ID", "30752070"))
API_HASH = os.environ.get("API_HASH", "45d346751438ce944b988fb54bed5ae1")
PHONE = os.environ.get("PHONE", "+447520676563")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

# ── Vantage Settings ─────────────────────────────────────────────────────────
VANTAGE_GROUP_ID = int(os.environ.get("VANTAGE_GROUP_ID", "-1002147262822"))
VANTAGE_TOPIC_ID = int(os.environ.get("VANTAGE_TOPIC_ID", "0"))

# ── Global state ─────────────────────────────────────────────────────────────
client = None
loop = asyncio.new_event_loop()
phone_code_hash = None

def run_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=run_loop, daemon=True).start()


async def init_client():
    global client
    if SESSION_STRING:
        # Use saved session string — no login needed
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            logger.info("✅ Logged in via session string")
            return True
        else:
            logger.error("❌ Session string invalid")
            return False
    else:
        # No session yet — client will need to login via /send_code and /verify
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        logger.info("⚠️ No session string — login required via /send_code")
        return False


# Start client on boot
future = asyncio.run_coroutine_threadsafe(init_client(), loop)
try:
    future.result(timeout=30)
except Exception as e:
    logger.error(f"Init error: {e}")


async def send_to_vantage(message_text):
    global client
    if not client or not await client.is_user_authorized():
        logger.error("Not logged in")
        return False
    try:
        entity = await client.get_entity(VANTAGE_GROUP_ID)
        if VANTAGE_TOPIC_ID and VANTAGE_TOPIC_ID > 0:
            await client.send_message(entity, message_text, reply_to=VANTAGE_TOPIC_ID)
        else:
            await client.send_message(entity, message_text)
        logger.info("✅ Message sent to Vantage")
        return True
    except Exception as e:
        logger.error(f"❌ Send error: {e}")
        return False


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Charlie Vantage Forwarder running ✅",
        "logged_in": SESSION_STRING != "",
        "vantage_group": VANTAGE_GROUP_ID,
        "topic_id": VANTAGE_TOPIC_ID
    })


@app.route("/send_code", methods=["GET"])
def send_code():
    """Step 1 — Request login code to be sent to your Telegram"""
    global phone_code_hash, client
    async def _send():
        global phone_code_hash
        result = await client.send_code_request(PHONE)
        phone_code_hash = result.phone_code_hash
        return result.phone_code_hash
    try:
        future = asyncio.run_coroutine_threadsafe(_send(), loop)
        future.result(timeout=15)
        return jsonify({"status": "Code sent to your Telegram ✅", "next": "Call /verify?code=XXXXX with your code"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/verify", methods=["GET"])
def verify():
    """Step 2 — Enter the code you received"""
    global phone_code_hash, client
    code = request.args.get("code", "")
    if not code:
        return jsonify({"error": "Provide ?code=XXXXX"}), 400

    async def _verify():
        await client.sign_in(PHONE, code, phone_code_hash=phone_code_hash)
        session_string = client.session.save()
        return session_string

    try:
        future = asyncio.run_coroutine_threadsafe(_verify(), loop)
        session_string = future.result(timeout=15)
        return jsonify({
            "status": "Logged in successfully ✅",
            "SESSION_STRING": session_string,
            "instruction": "Copy SESSION_STRING value and add it as a Railway environment variable called SESSION_STRING"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_topics", methods=["GET"])
def get_topics():
    """List Vantage group topics to find Chat-General ID"""
    async def _get():
        from telethon.tl.functions.channels import GetForumTopicsRequest
        entity = await client.get_entity(VANTAGE_GROUP_ID)
        result = await client(GetForumTopicsRequest(
            channel=entity, offset_date=0, offset_id=0,
            offset_topic=0, limit=50
        ))
        return [{"id": t.id, "title": t.title} for t in result.topics]
    try:
        future = asyncio.run_coroutine_threadsafe(_get(), loop)
        topics = future.result(timeout=15)
        return jsonify({"topics": topics})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["POST"])
def test_message():
    data = request.get_json() or {}
    message = data.get("message", "✅ Test message from Charlie's signals")
    future = asyncio.run_coroutine_threadsafe(send_to_vantage(message), loop)
    try:
        success = future.result(timeout=15)
        return jsonify({"status": "Sent ✅" if success else "Failed ❌"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/forward_tp", methods=["POST"])
def forward_tp():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data"}), 400
    close_type = data.get("close_type", "")
    message = data.get("message", "")
    if not message:
        return jsonify({"error": "No message"}), 400
    if close_type not in ["TP1", "TP2", "TP3"]:
        return jsonify({"status": "skipped"}), 200
    future = asyncio.run_coroutine_threadsafe(send_to_vantage(message), loop)
    try:
        success = future.result(timeout=15)
        return jsonify({"status": "sent ✅" if success else "failed ❌"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
