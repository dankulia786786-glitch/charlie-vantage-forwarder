import os
import json
import logging
import asyncio
import threading
import random
import time
import requests
from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

API_ID = int(os.environ.get("API_ID", "30752070"))
API_HASH = os.environ.get("API_HASH", "45d346751438ce944b988fb54bed5ae1")
PHONE = os.environ.get("PHONE", "+447520676563")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "ddc4ee93da1a40af9bd45f29eeb5d26d")

VANTAGE_GROUP_ID = int(os.environ.get("VANTAGE_GROUP_ID", "-1002147262822"))
VANTAGE_TOPIC_ID = int(os.environ.get("VANTAGE_TOPIC_ID", "1144"))

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
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            logger.info("✅ Logged in via session string")
            return True
        else:
            logger.error("❌ Session string invalid")
            return False
    else:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        logger.info("⚠️ No session — login via /send_code")
        return False

future = asyncio.run_coroutine_threadsafe(init_client(), loop)
try:
    future.result(timeout=30)
except Exception as e:
    logger.error(f"Init error: {e}")


def get_live_data(symbol):
    try:
        r = requests.get(f"https://api.twelvedata.com/price?symbol={symbol}&apikey={TWELVE_DATA_KEY}", timeout=8)
        price = float(r.json().get("price", 0))
        r2 = requests.get(f"https://api.twelvedata.com/rsi?symbol={symbol}&interval=1h&outputsize=1&apikey={TWELVE_DATA_KEY}", timeout=8)
        rsi_data = r2.json().get("values", [{}])
        rsi = float(rsi_data[0].get("rsi", 50)) if rsi_data else 50.0
        r3 = requests.get(f"https://api.twelvedata.com/ema?symbol={symbol}&interval=1h&time_period=50&outputsize=1&apikey={TWELVE_DATA_KEY}", timeout=8)
        ema_data = r3.json().get("values", [{}])
        ema = float(ema_data[0].get("ema", 0)) if ema_data else 0.0
        return {"price": price, "rsi": round(rsi, 1), "ema": round(ema, 2)}
    except Exception as e:
        logger.error(f"Data fetch error for {symbol}: {e}")
        return None


def generate_gold_message(data):
    price = data["price"]
    rsi = data["rsi"]
    ema = data["ema"]
    above_below = "above" if price > ema else "below"
    bull_bear = "bullish" if price > ema else "bearish"
    rsi_desc = "oversold" if rsi < 35 else "overbought" if rsi > 65 else "neutral"
    templates = [
        f"gold sitting at {price:.2f} right now, RSI on the 1H is {rsi} which is looking {rsi_desc} 👀 still {above_below} the 50 EMA at {ema:.2f} — structure is {bull_bear} for now, let's see if it holds",
        f"xauusd update 🔔 price at {price:.2f}, we're {above_below} the 1H 50 EMA ({ema:.2f}) — RSI cooling down at {rsi}, could see a small pullback before next move. watching closely 📊",
        f"gold {price:.2f} — momentum looking {bull_bear} on the hourly. RSI at {rsi} and price is {above_below} EMA50. if we hold this level next hour could be interesting 🟡",
        f"keeping an eye on gold here at {price:.2f} 👁️ RSI {rsi} on 1H, {above_below} the 50 EMA at {ema:.2f}. {bull_bear} bias until structure changes. not financial advice just my view 📈",
        f"xau update — {price:.2f} currently, 1H RSI at {rsi}. we're trading {above_below} EMA50 ({ema:.2f}) which confirms the {bull_bear} trend. let's see how NY session plays out 🇺🇸",
        f"quick gold check 🥇 {price:.2f} on the board, RSI {rsi} — {rsi_desc} territory on the hourly. EMA50 at {ema:.2f}, price is {above_below} it. {bull_bear} for now imo",
        f"gold analysis 📉📈 price: {price:.2f} | 1H RSI: {rsi} | EMA50: {ema:.2f}\nprice is {above_below} the EMA which gives us a {bull_bear} bias — RSI at {rsi} suggests {rsi_desc} conditions. watching next candle close 🕯️",
        f"not much changed on gold — still at {price:.2f}, RSI {rsi} on the 1H 📊 {above_below} EMA50 ({ema:.2f}). market looking {bull_bear}, waiting for a clear signal before adding 🙏",
    ]
    return random.choice(templates)


def generate_btc_message(data):
    price = data["price"]
    rsi = data["rsi"]
    ema = data["ema"]
    above_below = "above" if price > ema else "below"
    bull_bear = "bullish" if price > ema else "bearish"
    rsi_desc = "oversold" if rsi < 35 else "overbought" if rsi > 65 else "neutral"
    templates = [
        f"btc update 🟠 price at {price:,.0f}, RSI {rsi} on the 1H — {rsi_desc} zone. trading {above_below} EMA50 ({ema:,.0f}) so bias is {bull_bear} right now. crypto market still choppy 🌊",
        f"bitcoin {price:,.0f} 👀 1H RSI sitting at {rsi}, {above_below} the 50 EMA at {ema:,.0f}. {bull_bear} structure intact — let's see if this level gets respected 📊",
        f"quick btc check — {price:,.0f} currently 🔔 RSI {rsi} on hourly, EMA50 at {ema:,.0f}. we're {above_below} the EMA which is {bull_bear}. watching for volume to confirm ⚡",
        f"btcusd at {price:,.0f} rn, RSI {rsi} — {rsi_desc} on 1H 📈 {above_below} EMA50 ({ema:,.0f}), {bull_bear} bias. crypto following gold moves today interesting to watch 🥇",
        f"bitcoin analysis 🔍 {price:,.0f} | RSI: {rsi} | EMA50: {ema:,.0f}\n{above_below} the EMA = {bull_bear} momentum. RSI at {rsi} is {rsi_desc}. keep stops tight in this market 🙏",
    ]
    return random.choice(templates)


def generate_dxy_message(data):
    price = data["price"]
    rsi = data["rsi"]
    ema = data["ema"]
    above_below = "above" if price > ema else "below"
    bull_bear = "bullish" if price > ema else "bearish"
    templates = [
        f"dxy at {price:.2f} 💵 RSI {rsi} on 1H, {above_below} EMA50 ({ema:.2f}) — dollar is looking {bull_bear}. this directly impacts gold so worth watching if you're trading xau 👀",
        f"dollar index update — {price:.2f}, RSI {rsi} 📊 {above_below} the 50 EMA at {ema:.2f}. {bull_bear} dollar = {('bearish' if bull_bear == 'bullish' else 'bullish')} gold generally. keep this in mind 🔔",
        f"keeping tabs on dxy here 💵 {price:.2f}, 1H RSI at {rsi}, {above_below} EMA50 ({ema:.2f}). {bull_bear} structure on the dollar — inverse correlation with gold is key right now 🥇",
        f"dxy check 📉 {price:.2f} | RSI: {rsi} | EMA50: {ema:.2f}\ndollar {above_below} EMA = {bull_bear} bias. if dollar weakens further gold should push higher. watching this correlation closely 👁️",
    ]
    return random.choice(templates)


def generate_gbpusd_message(data):
    price = data["price"]
    rsi = data["rsi"]
    ema = data["ema"]
    above_below = "above" if price > ema else "below"
    bull_bear = "bullish" if price > ema else "bearish"
    templates = [
        f"gbpusd at {price:.4f} 🇬🇧 RSI {rsi} on 1H, {above_below} EMA50 ({ema:.4f}) — cable looking {bull_bear}. UK data driving some moves today 📊",
        f"quick cable update — {price:.4f}, 1H RSI {rsi} 📈 {above_below} the 50 EMA at {ema:.4f}. {bull_bear} momentum for now, watching BOE comments for direction 🔔",
        f"gbpusd {price:.4f} rn 👀 RSI {rsi}, {above_below} EMA50 ({ema:.4f}). {bull_bear} structure — pound has been interesting this week. watching 4H for clearer picture 🕯️",
    ]
    return random.choice(templates)


async def send_to_vantage(message_text):
    global client
    try:
        if not client or not await client.is_user_authorized():
            logger.error("Not logged in")
            return False
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


ASSETS = [
    {"symbol": "XAU/USD", "name": "gold", "generator": generate_gold_message},
    {"symbol": "BTC/USD", "name": "bitcoin", "generator": generate_btc_message},
    {"symbol": "DXY", "name": "dxy", "generator": generate_dxy_message},
    {"symbol": "GBP/USD", "name": "gbpusd", "generator": generate_gbpusd_message},
]

broadcaster_running = False


async def broadcast_loop():
    global broadcaster_running
    broadcaster_running = True
    logger.info("🚀 Auto broadcaster started")
    while broadcaster_running:
        try:
            asset = random.choice(ASSETS)
            data = get_live_data(asset["symbol"])
            if data and data["price"] > 0:
                message = asset["generator"](data)
                await send_to_vantage(message)
                logger.info(f"📤 Sent {asset['name']} analysis")
            else:
                logger.warning(f"No data for {asset['name']}, skipping")
            wait_minutes = random.randint(14, 60)
            if random.random() < 0.3:
                wait_minutes = random.randint(14, 25)
            logger.info(f"⏳ Next message in {wait_minutes} minutes")
            await asyncio.sleep(wait_minutes * 60)
        except Exception as e:
            logger.error(f"Broadcaster error: {e}")
            await asyncio.sleep(60)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Charlie Vantage Forwarder running ✅",
        "logged_in": SESSION_STRING != "",
        "broadcaster": "running" if broadcaster_running else "stopped",
        "vantage_group": VANTAGE_GROUP_ID,
        "topic_id": VANTAGE_TOPIC_ID
    })


@app.route("/start_broadcaster", methods=["GET"])
def start_broadcaster():
    global broadcaster_running
    if broadcaster_running:
        return jsonify({"status": "Already running ✅"})
    asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)
    return jsonify({"status": "Broadcaster started ✅"})


@app.route("/stop_broadcaster", methods=["GET"])
def stop_broadcaster():
    global broadcaster_running
    broadcaster_running = False
    return jsonify({"status": "Broadcaster stopped ⏹️"})


@app.route("/send_code", methods=["GET"])
def send_code():
    global phone_code_hash, client
    async def _send():
        global phone_code_hash
        result = await client.send_code_request(PHONE)
        phone_code_hash = result.phone_code_hash
    try:
        future = asyncio.run_coroutine_threadsafe(_send(), loop)
        future.result(timeout=15)
        return jsonify({"status": "Code sent ✅", "next": "/verify?code=XXXXX"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/verify", methods=["GET"])
def verify():
    global phone_code_hash, client
    code = request.args.get("code", "")
    if not code:
        return jsonify({"error": "Provide ?code=XXXXX"}), 400
    async def _verify():
        await client.sign_in(PHONE, code, phone_code_hash=phone_code_hash)
        return client.session.save()
    try:
        future = asyncio.run_coroutine_threadsafe(_verify(), loop)
        session_string = future.result(timeout=15)
        return jsonify({"status": "Logged in ✅", "SESSION_STRING": session_string})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["POST"])
def test_message():
    data = request.get_json() or {}
    message = data.get("message", "gold looking strong above 4180, watching for continuation 👀")
    future = asyncio.run_coroutine_threadsafe(send_to_vantage(message), loop)
    try:
        success = future.result(timeout=15)
        return jsonify({"status": "Sent ✅" if success else "Failed ❌"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test_analysis", methods=["GET"])
def test_analysis():
    asset = random.choice(ASSETS)
    data = get_live_data(asset["symbol"])
    if not data:
        return jsonify({"error": "Could not fetch live data"}), 500
    message = asset["generator"](data)
    future = asyncio.run_coroutine_threadsafe(send_to_vantage(message), loop)
    try:
        success = future.result(timeout=15)
        return jsonify({"status": "Sent ✅" if success else "Failed ❌", "asset": asset["name"], "message": message})
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


def start_broadcaster_on_boot():
    time.sleep(10)
    asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)
    logger.info("🚀 Broadcaster auto-started on boot")

threading.Thread(target=start_broadcaster_on_boot, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
