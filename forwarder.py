import os
import logging
import asyncio
import threading
import random
import time
import requests
import io
from flask import Flask, request, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
PHONE = os.environ.get("PHONE", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

TWELVE_DATA_KEY = os.environ.get("TWELVE_DATA_KEY", "")
CHART_IMG_KEY = os.environ.get("CHART_IMG_KEY", "")

VANTAGE_GROUP_ID = int(os.environ.get("VANTAGE_GROUP_ID", "0"))
VANTAGE_TOPIC_ID = int(os.environ.get("VANTAGE_TOPIC_ID", "0"))

AUTO_START_BROADCASTER = os.environ.get("AUTO_START_BROADCASTER", "false").lower() == "true"

client = None
loop = asyncio.new_event_loop()
phone_code_hash = None
broadcaster_running = False


def run_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()


threading.Thread(target=run_loop, daemon=True).start()


async def init_client():
    global client

    if not API_ID or not API_HASH:
        logger.error("API_ID or API_HASH missing")
        return False

    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()

        if await client.is_user_authorized():
            logger.info("Logged in via session string")
            return True

        logger.error("Session string invalid")
        return False

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    logger.info("No session string found. Login via /send_code")
    return False


future = asyncio.run_coroutine_threadsafe(init_client(), loop)

try:
    future.result(timeout=30)
except Exception as e:
    logger.error(f"Init error: {e}")


def calculate_ema(values, period):
    if not values:
        return 0.0

    if len(values) < period:
        return values[-1]

    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period

    for price in values[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


def calculate_rsi(values, period=14):
    if not values or len(values) <= period:
        return 50.0

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]
        gain = max(change, 0)
        loss = abs(min(change, 0))

        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


def yahoo_symbol(symbol):
    mapping = {
        "XAU/USD": "GC=F",
        "BTC/USD": "BTC-USD",
        "DXY": "DX-Y.NYB",
        "GBP/USD": "GBPUSD=X",
    }

    return mapping.get(symbol, symbol)


def yahoo_interval(interval):
    mapping = {
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "45min": "30m",
        "1h": "1h",
        "4h": "1h",
        "1day": "1d",
        "1week": "1wk",
    }

    return mapping.get(interval, "1h")


def chart_interval(interval):
    mapping = {
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "45min": "45m",
        "1h": "1h",
        "4h": "4h",
        "1day": "1D",
        "1week": "1W",
    }

    return mapping.get(interval, "1h")


def get_live_data_from_yahoo(symbol, interval):
    try:
        y_symbol = yahoo_symbol(symbol)
        y_interval = yahoo_interval(interval)

        if y_interval in ["5m", "15m", "30m"]:
            range_value = "5d"
        elif y_interval == "1h":
            range_value = "30d"
        elif y_interval == "1d":
            range_value = "1y"
        else:
            range_value = "5y"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{y_symbol}"

        params = {
            "interval": y_interval,
            "range": range_value,
        }

        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        data = response.json()

        result = data.get("chart", {}).get("result", [])

        if not result:
            return None

        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [float(x) for x in closes if x is not None]

        if not closes:
            return None

        price = closes[-1]
        ema50 = calculate_ema(closes, 50)
        ema200 = calculate_ema(closes, 200)
        rsi = calculate_rsi(closes, 14)

        return {
            "price": round(price, 4),
            "rsi": round(rsi, 1),
            "ema": round(ema50, 4),
            "ema200": round(ema200, 4),
            "source": "yahoo"
        }

    except Exception as e:
        logger.error(f"Yahoo data error for {symbol}: {e}")
        return None


def get_live_data_from_twelve_data(symbol, interval):
    try:
        if not TWELVE_DATA_KEY:
            return None

        price_response = requests.get(
            "https://api.twelvedata.com/price",
            params={
                "symbol": symbol,
                "apikey": TWELVE_DATA_KEY,
            },
            timeout=8
        )

        price_json = price_response.json()
        price = float(price_json.get("price", 0))

        rsi_response = requests.get(
            "https://api.twelvedata.com/rsi",
            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": 1,
                "apikey": TWELVE_DATA_KEY,
            },
            timeout=8
        )

        rsi_data = rsi_response.json().get("values", [{}])
        rsi = float(rsi_data[0].get("rsi", 50)) if rsi_data else 50.0

        ema50_response = requests.get(
            "https://api.twelvedata.com/ema",
            params={
                "symbol": symbol,
                "interval": interval,
                "time_period": 50,
                "outputsize": 1,
                "apikey": TWELVE_DATA_KEY,
            },
            timeout=8
        )

        ema50_data = ema50_response.json().get("values", [{}])
        ema50 = float(ema50_data[0].get("ema", 0)) if ema50_data else 0.0

        ema200_response = requests.get(
            "https://api.twelvedata.com/ema",
            params={
                "symbol": symbol,
                "interval": interval,
                "time_period": 200,
                "outputsize": 1,
                "apikey": TWELVE_DATA_KEY,
            },
            timeout=8
        )

        ema200_data = ema200_response.json().get("values", [{}])
        ema200 = float(ema200_data[0].get("ema", 0)) if ema200_data else 0.0

        if price <= 0:
            return None

        return {
            "price": round(price, 4),
            "rsi": round(rsi, 1),
            "ema": round(ema50, 4),
            "ema200": round(ema200, 4),
            "source": "twelve_data"
        }

    except Exception as e:
        logger.error(f"Twelve Data error for {symbol}: {e}")
        return None


def get_live_data(symbol, interval):
    data = get_live_data_from_twelve_data(symbol, interval)

    if data:
        return data

    return get_live_data_from_yahoo(symbol, interval)


def tradingview_symbol(asset_name):
    mapping = {
        "gold": "OANDA:XAUUSD",
        "bitcoin": "COINBASE:BTCUSD",
        "dxy": "TVC:DXY",
        "gbpusd": "OANDA:GBPUSD",
    }

    return mapping.get(asset_name, "OANDA:XAUUSD")


def get_chart_image(asset_name, interval):
    try:
        if not CHART_IMG_KEY:
            return None

        params = [
            ("symbol", tradingview_symbol(asset_name)),
            ("interval", chart_interval(interval)),
            ("theme", "dark"),
            ("width", "900"),
            ("height", "700"),
            ("style", "candle"),
            ("format", "png"),
            ("studies", "EMA:50"),
            ("studies", "EMA:200"),
            ("studies", "RSI"),
            ("studies", "Volume"),
            ("key", CHART_IMG_KEY),
        ]

        headers = {
            "Authorization": f"Bearer {CHART_IMG_KEY}",
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(
            "https://api.chart-img.com/v1/tradingview/advanced-chart",
            params=params,
            headers=headers,
            timeout=30
        )

        content_type = response.headers.get("content-type", "")

        if response.status_code == 200 and "image" in content_type:
            image = io.BytesIO(response.content)
            image.name = f"{asset_name}_chart.png"
            return image

        logger.error(f"Chart image error: {response.status_code} {response.text[:300]}")
        return None

    except Exception as e:
        logger.error(f"Chart image fetch error: {e}")
        return None


def price_format(asset_name, price):
    if asset_name == "bitcoin":
        return f"{price:,.0f}"

    if asset_name == "gbpusd":
        return f"{price:.4f}"

    return f"{price:.2f}"


def level_format(asset_name, level):
    if asset_name == "bitcoin":
        return f"{level:,.0f}"

    if asset_name == "gbpusd":
        return f"{level:.4f}"

    return f"{level:.2f}"


def display_asset(asset_name):
    mapping = {
        "gold": "Gold",
        "bitcoin": "BTC",
        "dxy": "DXY",
        "gbpusd": "GBP/USD",
    }

    return mapping.get(asset_name, asset_name.upper())


def rsi_comment(asset_name, rsi, price, ema50, ema200):
    if rsi >= 70:
        if asset_name == "gold":
            return f"RSI is near {rsi}, so Gold is looking stretched and I would not chase it too high without a clean pullback."
        return f"RSI is near {rsi}, so momentum is strong but the move is getting a little stretched."

    if rsi <= 30:
        if asset_name == "gold":
            return f"RSI is near {rsi}, so sellers have control but Gold is getting close to a reaction area."
        return f"RSI is near {rsi}, so sellers are still in control but the move is getting slightly stretched."

    if 45 <= rsi <= 55:
        return f"RSI is near {rsi}, so momentum is balanced and the next clean break matters more."

    if price > ema50 and price > ema200:
        return f"RSI is near {rsi}, so buyers still have control, but we need to see if momentum can keep building."

    if price < ema50 and price < ema200:
        return f"RSI is near {rsi}, so sellers still have control, but price may start reacting around the next support."

    return f"RSI is near {rsi}, so momentum is still mixed and the chart needs a cleaner move."


def generate_market_message(asset_name, data, interval):
    name = display_asset(asset_name)
    price = data["price"]
    rsi = data["rsi"]
    ema50 = data["ema"]
    ema200 = data["ema200"]

    price_text = price_format(asset_name, price)
    ema50_text = level_format(asset_name, ema50)
    ema200_text = level_format(asset_name, ema200)
    visible_interval = chart_interval(interval)

    above_50 = price > ema50
    above_200 = price > ema200

    if above_50 and above_200:
        line1 = f"{name} is trading around {price_text} on the {visible_interval} chart, holding above the EMA 50 at {ema50_text} and EMA 200 at {ema200_text}."
        line2 = rsi_comment(asset_name, rsi, price, ema50, ema200)
        line3 = f"If {name} keeps holding above {ema50_text}, the next push higher can stay active."

    elif not above_50 and not above_200:
        line1 = f"{name} is trading around {price_text} on the {visible_interval} chart, still sitting below the EMA 50 at {ema50_text} and EMA 200 at {ema200_text}."
        line2 = rsi_comment(asset_name, rsi, price, ema50, ema200)
        line3 = f"If {name} fails to reclaim {ema50_text}, the next move lower can stay active."

    else:
        line1 = f"{name} is trading around {price_text} on the {visible_interval} chart, sitting between the EMA 50 at {ema50_text} and EMA 200 at {ema200_text}."
        line2 = rsi_comment(asset_name, rsi, price, ema50, ema200)
        key_level = max(ema50, ema200)
        key_level_text = level_format(asset_name, key_level)
        line3 = f"If {name} clears {key_level_text}, buyers can take more control, but rejection keeps it choppy."

    return f"**🔔 Market Update**\n\n{line1}\n{line2}\n{line3}"


def choose_asset():
    asset_pool = []

    asset_pool += [
        {
            "symbol": "XAU/USD",
            "name": "gold",
            "interval": random.choice(["5min", "15min", "15min", "1h", "1h", "4h", "1week"])
        }
        for _ in range(70)
    ]

    asset_pool += [
        {
            "symbol": "BTC/USD",
            "name": "bitcoin",
            "interval": random.choice(["15min", "1h", "1h", "4h"])
        }
        for _ in range(25)
    ]

    asset_pool += [
        {
            "symbol": "DXY",
            "name": "dxy",
            "interval": random.choice(["1h", "4h", "1day"])
        }
        for _ in range(3)
    ]

    asset_pool += [
        {
            "symbol": "GBP/USD",
            "name": "gbpusd",
            "interval": random.choice(["1h", "4h", "1day"])
        }
        for _ in range(2)
    ]

    return random.choice(asset_pool)


def choose_wait_minutes():
    return random.choice([10, 15, 18, 22, 25, 30, 45, 60])


async def send_to_vantage(message_text, chart_image=None):
    global client

    try:
        if not client or not await client.is_user_authorized():
            logger.error("Not logged in")
            return False

        if not VANTAGE_GROUP_ID:
            logger.error("VANTAGE_GROUP_ID missing")
            return False

        entity = await client.get_entity(VANTAGE_GROUP_ID)

        kwargs = {
            "parse_mode": "md"
        }

        if VANTAGE_TOPIC_ID and VANTAGE_TOPIC_ID > 0:
            kwargs["reply_to"] = VANTAGE_TOPIC_ID

        if chart_image:
            await client.send_file(
                entity,
                chart_image,
                caption=message_text,
                force_document=False,
                **kwargs
            )
        else:
            await client.send_message(
                entity,
                message_text,
                **kwargs
            )

        logger.info("Message sent to Vantage")
        return True

    except Exception as e:
        logger.error(f"Send error: {e}")
        return False


async def create_and_send_market_update(send=True):
    asset = choose_asset()
    data = get_live_data(asset["symbol"], asset["interval"])

    if not data or data["price"] <= 0:
        logger.warning(f"No data for {asset['name']}")
        return {
            "ok": False,
            "asset": asset,
            "error": "Could not fetch live data"
        }

    message = generate_market_message(asset["name"], data, asset["interval"])
    chart_image = get_chart_image(asset["name"], asset["interval"])

    if not send:
        return {
            "ok": True,
            "asset": asset,
            "data": data,
            "message": message,
            "chart_image": "available" if chart_image else "not available"
        }

    success = await send_to_vantage(message, chart_image=chart_image)

    return {
        "ok": success,
        "asset": asset,
        "data": data,
        "message": message,
        "chart_image": "sent" if chart_image else "not sent"
    }


async def broadcast_loop():
    global broadcaster_running

    broadcaster_running = True
    logger.info("Auto broadcaster started")

    while broadcaster_running:
        try:
            result = await create_and_send_market_update(send=True)
            logger.info(f"Broadcast result: {result}")

            wait_minutes = choose_wait_minutes()
            logger.info(f"Next message in {wait_minutes} minutes")
            await asyncio.sleep(wait_minutes * 60)

        except Exception as e:
            logger.error(f"Broadcaster error: {e}")
            await asyncio.sleep(60)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Charlie Vantage Forwarder running",
        "logged_in": SESSION_STRING != "",
        "broadcaster": "running" if broadcaster_running else "stopped",
        "vantage_group": VANTAGE_GROUP_ID,
        "topic_id": VANTAGE_TOPIC_ID,
        "auto_start": AUTO_START_BROADCASTER,
        "chart_img_enabled": CHART_IMG_KEY != "",
        "twelve_data_enabled": TWELVE_DATA_KEY != ""
    })


@app.route("/start_broadcaster", methods=["GET"])
def start_broadcaster():
    global broadcaster_running

    if broadcaster_running:
        return jsonify({"status": "Already running"})

    asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)

    return jsonify({"status": "Broadcaster started"})


@app.route("/stop_broadcaster", methods=["GET"])
def stop_broadcaster():
    global broadcaster_running

    broadcaster_running = False

    return jsonify({"status": "Broadcaster stopped"})


@app.route("/preview_analysis", methods=["GET"])
def preview_analysis():
    future = asyncio.run_coroutine_threadsafe(create_and_send_market_update(send=False), loop)

    try:
        result = future.result(timeout=30)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test_analysis", methods=["GET"])
def test_analysis():
    future = asyncio.run_coroutine_threadsafe(create_and_send_market_update(send=True), loop)

    try:
        result = future.result(timeout=45)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["POST"])
def test_message():
    data = request.get_json() or {}

    message = data.get(
        "message",
        "**🔔 Market Update**\n\nGold is trading around 4182.40 on the 15m chart, still sitting below the EMA 50 at 4191.20 and EMA 200 at 4204.80.\nRSI is near 38, so sellers still have control, but price is getting close to an area where buyers may try to react.\nIf Gold fails to reclaim 4191.20, the next move lower can stay active."
    )

    future = asyncio.run_coroutine_threadsafe(send_to_vantage(message), loop)

    try:
        success = future.result(timeout=20)
        return jsonify({"status": "Sent" if success else "Failed"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        return jsonify({"status": "Code sent", "next": "/verify?code=XXXXX"})
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
        return jsonify({"status": "Logged in", "SESSION_STRING": session_string})
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
        return jsonify({"status": "sent" if success else "failed"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def start_broadcaster_on_boot():
    if not AUTO_START_BROADCASTER:
        logger.info("Auto start broadcaster disabled")
        return

    time.sleep(10)
    asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)
    logger.info("Broadcaster auto started on boot")


threading.Thread(target=start_broadcaster_on_boot, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
