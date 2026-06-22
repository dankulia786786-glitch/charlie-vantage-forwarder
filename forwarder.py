import os
import logging
import asyncio
import threading
import random
import time
import requests
import io
from flask import Flask, request, jsonify, Response
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
ENABLE_GROUP_SEND = os.environ.get("ENABLE_GROUP_SEND", "false").lower() == "true"

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

        response = requests.get(url, params=params, headers=headers, timeout=15)
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
            timeout=10
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
            timeout=10
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
            timeout=10
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
            timeout=10
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


def chart_asset_config(asset_name, interval):
    symbol = tradingview_symbol(asset_name)
    interval_value = chart_interval(interval)

    payload = {
        "symbol": symbol,
        "interval": interval_value,
        "theme": "dark",
        "width": 800,
        "height": 600,
        "timezone": "Europe/London",
        "override": {
            "showStudyLastValue": True,
            "showSeriesLastValue": True,
            "showSeriesOHLC": True,
            "showBarChange": True,
            "showLegendValues": True,
            "mainPaneHeight": 420
        },
        "studies": [
            {
                "name": "Moving Average Exponential",
                "input": {
                    "length": 50,
                    "source": "close",
                    "offset": 0,
                    "smoothingLine": "SMA",
                    "smoothingLength": 9
                },
                "override": {
                    "Plot.linewidth": 2,
                    "Plot.plottype": "line",
                    "Plot.color": "rgb(255,193,7)"
                }
            },
            {
                "name": "Moving Average Exponential",
                "input": {
                    "length": 200,
                    "source": "close",
                    "offset": 0,
                    "smoothingLine": "SMA",
                    "smoothingLength": 9
                },
                "override": {
                    "Plot.linewidth": 2,
                    "Plot.plottype": "line",
                    "Plot.color": "rgb(33,150,243)"
                }
            },
            {
                "name": "Relative Strength Index",
                "input": {
                    "length": 14,
                    "source": "close"
                }
            }
        ]
    }

    return payload


def get_chart_image_result(asset_name, interval):
    try:
        if not CHART_IMG_KEY:
            return {
                "ok": False,
                "image": None,
                "error": "CHART_IMG_KEY missing"
            }

        payload = chart_asset_config(asset_name, interval)

        headers = {
            "x-api-key": CHART_IMG_KEY,
            "content-type": "application/json",
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.post(
            "https://api.chart-img.com/v2/tradingview/advanced-chart",
            json=payload,
            headers=headers,
            timeout=75
        )

        content_type = response.headers.get("content-type", "")

        if response.status_code == 200 and "image" in content_type:
            image = io.BytesIO(response.content)
            image.name = f"{asset_name}_chart.png"

            return {
                "ok": True,
                "image": image,
                "error": None
            }

        return {
            "ok": False,
            "image": None,
            "error": f"Chart IMG error {response.status_code}: {response.text[:500]}"
        }

    except Exception as e:
        logger.error(f"Chart image fetch error: {e}")

        return {
            "ok": False,
            "image": None,
            "error": str(e)
        }


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


def bias_details(asset_name, price, ema50, ema200, rsi):
    above_50 = price > ema50
    above_200 = price > ema200

    if above_50 and above_200:
        return {
            "bias": "bullish",
            "trade_word": "buys",
            "opposite_word": "sells",
            "key_level": ema50,
            "tone": "buyers are still showing control"
        }

    if not above_50 and not above_200:
        return {
            "bias": "bearish",
            "trade_word": "sells",
            "opposite_word": "buys",
            "key_level": ema50,
            "tone": "sellers still have control"
        }

    if price >= ema50:
        return {
            "bias": "mixed but slightly bullish",
            "trade_word": "buys",
            "opposite_word": "sells",
            "key_level": max(ema50, ema200),
            "tone": "buyers are trying to build momentum"
        }

    return {
        "bias": "mixed but slightly bearish",
        "trade_word": "sells",
        "opposite_word": "buys",
        "key_level": max(ema50, ema200),
        "tone": "sellers are still keeping pressure on the chart"
    }


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

    details = bias_details(asset_name, price, ema50, ema200, rsi)
    bias = details["bias"]
    trade_word = details["trade_word"]
    opposite_word = details["opposite_word"]
    key_level = details["key_level"]
    key_level_text = level_format(asset_name, key_level)
    tone = details["tone"]

    line1_options = [
        f"✅ {name} is around {price_text} on the {visible_interval} chart, and the overall picture is still leaning {bias}.",
        f"✅ {name} is trading near {price_text}, with price reacting around an important area on the {visible_interval} chart.",
        f"✅ {name} is currently near {price_text}, and the market is showing a {bias} tone for now.",
        f"✅ {name} is moving around {price_text} on the {visible_interval}, and the chart is not fully clean yet.",
        f"✅ {name} is sitting around {price_text}, and we are still waiting for a stronger confirmation from this zone.",
        f"✅ {name} is trading close to {price_text}, and the current structure is giving a {bias} feel."
    ]

    line2_options = [
        f"✅ Price is around EMA 50 at {ema50_text} and EMA 200 at {ema200_text}, so {tone}.",
        f"✅ The main levels I’m watching are {ema50_text} and {ema200_text}, because they are guiding the next direction.",
        f"✅ As long as price respects the {key_level_text} area, the current bias can stay active.",
        f"✅ The chart is still respecting the key EMA zone, so I would not rush against the current move yet.",
        f"✅ RSI is near {rsi}, which shows the market is not too stretched and still has room for the next move.",
        f"✅ Momentum is fairly balanced with RSI near {rsi}, so the next clean break is important."
    ]

    line3_options = [
        f"✅ For now, {trade_word} make more sense while price stays around this structure, but I would change view if {name} breaks back through {key_level_text}.",
        f"✅ Bias stays with {trade_word} for now, but if {name} clears {key_level_text} cleanly, {opposite_word} can start looking better.",
        f"✅ I would be more comfortable looking for {trade_word} unless price gives a strong break above {key_level_text}.",
        f"✅ At the moment, {trade_word} still look cleaner, but confirmation is important before forcing any entry.",
        f"✅ If {name} rejects this area again, {trade_word} can stay in play, but a clean break changes the picture.",
        f"✅ Overall, I would keep the focus on {trade_word} for now, while watching {key_level_text} as the level that can shift momentum."
    ]

    line1 = random.choice(line1_options)
    line2 = random.choice(line2_options)
    line3 = random.choice(line3_options)

    return f"**🔔 Market Update**\n\n{line1}\n\n{line2}\n\n{line3}"


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


def choose_asset_from_query():
    requested_asset = request.args.get("asset", "").lower().strip()
    requested_interval = request.args.get("interval", "").lower().strip()

    asset_map = {
        "gold": {
            "symbol": "XAU/USD",
            "name": "gold"
        },
        "xau": {
            "symbol": "XAU/USD",
            "name": "gold"
        },
        "xauusd": {
            "symbol": "XAU/USD",
            "name": "gold"
        },
        "btc": {
            "symbol": "BTC/USD",
            "name": "bitcoin"
        },
        "bitcoin": {
            "symbol": "BTC/USD",
            "name": "bitcoin"
        },
        "dxy": {
            "symbol": "DXY",
            "name": "dxy"
        },
        "gbpusd": {
            "symbol": "GBP/USD",
            "name": "gbpusd"
        },
        "gbp": {
            "symbol": "GBP/USD",
            "name": "gbpusd"
        }
    }

    valid_intervals = ["5min", "15min", "30min", "45min", "1h", "4h", "1day", "1week"]

    if requested_asset in asset_map:
        asset = asset_map[requested_asset]
        asset["interval"] = requested_interval if requested_interval in valid_intervals else "15min"
        return asset

    return choose_asset()


def choose_wait_minutes():
    return random.choice([10, 15, 18, 22, 25, 30, 45, 60])


async def send_message_to_entity(entity_target, message_text, chart_image=None, reply_to=None):
    global client

    try:
        if not client or not await client.is_user_authorized():
            logger.error("Not logged in")
            return False

        entity = await client.get_entity(entity_target)

        kwargs = {
            "parse_mode": "md"
        }

        if reply_to:
            kwargs["reply_to"] = reply_to

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

        logger.info("Message sent")
        return True

    except Exception as e:
        logger.error(f"Send error: {e}")
        return False


async def send_to_saved_messages(message_text, chart_image=None):
    return await send_message_to_entity("me", message_text, chart_image=chart_image)


async def send_to_vantage(message_text, chart_image=None):
    if not ENABLE_GROUP_SEND:
        logger.warning("Group sending is locked")
        return False

    if not VANTAGE_GROUP_ID:
        logger.error("VANTAGE_GROUP_ID missing")
        return False

    if not chart_image:
        logger.error("Chart image missing. Refusing to send to group.")
        return False

    return await send_message_to_entity(
        VANTAGE_GROUP_ID,
        message_text,
        chart_image=chart_image,
        reply_to=VANTAGE_TOPIC_ID if VANTAGE_TOPIC_ID and VANTAGE_TOPIC_ID > 0 else None
    )


async def create_market_update(send_mode="preview"):
    asset = choose_asset_from_query()
    data = get_live_data(asset["symbol"], asset["interval"])

    if not data or data["price"] <= 0:
        return {
            "ok": False,
            "asset": asset,
            "error": "Could not fetch live data"
        }

    message = generate_market_message(asset["name"], data, asset["interval"])
    chart_result = get_chart_image_result(asset["name"], asset["interval"])

    chart_status = "available" if chart_result["ok"] else "not available"
    chart_error = chart_result["error"]

    if send_mode == "saved":
        if not chart_result["ok"]:
            return {
                "ok": False,
                "asset": asset,
                "data": data,
                "message": message,
                "chart_image": chart_status,
                "chart_error": chart_error,
                "sent": "not sent"
            }

        success = await send_to_saved_messages(message, chart_image=chart_result["image"])

        return {
            "ok": success,
            "asset": asset,
            "data": data,
            "message": message,
            "chart_image": chart_status,
            "sent": "saved_messages" if success else "failed"
        }

    if send_mode == "vantage":
        if not ENABLE_GROUP_SEND:
            return {
                "ok": False,
                "asset": asset,
                "data": data,
                "message": message,
                "chart_image": chart_status,
                "chart_error": chart_error,
                "sent": "locked",
                "unlock_needed": "Set ENABLE_GROUP_SEND=true in Railway Variables when ready"
            }

        if not chart_result["ok"]:
            return {
                "ok": False,
                "asset": asset,
                "data": data,
                "message": message,
                "chart_image": chart_status,
                "chart_error": chart_error,
                "sent": "not sent because chart image is missing"
            }

        success = await send_to_vantage(message, chart_image=chart_result["image"])

        return {
            "ok": success,
            "asset": asset,
            "data": data,
            "message": message,
            "chart_image": chart_status,
            "sent": "vantage" if success else "failed"
        }

    return {
        "ok": True,
        "asset": asset,
        "data": data,
        "message": message,
        "chart_image": chart_status,
        "chart_error": chart_error
    }


async def broadcast_loop():
    global broadcaster_running

    if not ENABLE_GROUP_SEND:
        logger.warning("Broadcaster blocked because ENABLE_GROUP_SEND is false")
        broadcaster_running = False
        return

    broadcaster_running = True
    logger.info("Auto broadcaster started")

    while broadcaster_running:
        try:
            result = await create_market_update(send_mode="vantage")
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
        "group_send_enabled": ENABLE_GROUP_SEND,
        "chart_img_enabled": CHART_IMG_KEY != "",
        "twelve_data_enabled": TWELVE_DATA_KEY != "",
        "safe_test_saved_messages": "/send_saved_test",
        "safe_chart_preview": "/preview_chart",
        "safe_text_preview": "/preview_analysis"
    })


@app.route("/start_broadcaster", methods=["GET"])
def start_broadcaster():
    global broadcaster_running

    if broadcaster_running:
        return jsonify({"status": "Already running"})

    if not ENABLE_GROUP_SEND:
        return jsonify({
            "status": "blocked",
            "reason": "ENABLE_GROUP_SEND is false, so the group is protected"
        }), 403

    asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)

    return jsonify({"status": "Broadcaster started"})


@app.route("/stop_broadcaster", methods=["GET"])
def stop_broadcaster():
    global broadcaster_running

    broadcaster_running = False

    return jsonify({"status": "Broadcaster stopped"})


@app.route("/preview_analysis", methods=["GET"])
def preview_analysis():
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="preview"), loop)

    try:
        result = future.result(timeout=90)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/preview_chart", methods=["GET"])
def preview_chart():
    asset = choose_asset_from_query()
    chart_result = get_chart_image_result(asset["name"], asset["interval"])

    if not chart_result["ok"]:
        return jsonify({
            "ok": False,
            "asset": asset,
            "chart_error": chart_result["error"]
        }), 500

    image_bytes = chart_result["image"].getvalue()

    return Response(image_bytes, mimetype="image/png")


@app.route("/send_saved_test", methods=["GET"])
def send_saved_test():
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="saved"), loop)

    try:
        result = future.result(timeout=100)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test_analysis", methods=["GET"])
def test_analysis():
    return jsonify({
        "status": "safe_locked",
        "message": "This endpoint does not send to Vantage anymore. Use /send_saved_test for safe testing in Saved Messages."
    })


@app.route("/send_vantage_once", methods=["GET"])
def send_vantage_once():
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="vantage"), loop)

    try:
        result = future.result(timeout=100)
        status_code = 200 if result.get("ok") else 403
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["POST"])
def test_message():
    data = request.get_json() or {}

    message = data.get(
        "message",
        "**🔔 Market Update**\n\n✅ Gold is around 4182.40 on the 15m chart, and the overall picture is still leaning bearish.\n\n✅ RSI is near 38, so the market still has room for the next move.\n\n✅ For now, sells make more sense while Gold stays below the key level."
    )

    future = asyncio.run_coroutine_threadsafe(send_to_saved_messages(message), loop)

    try:
        success = future.result(timeout=20)
        return jsonify({"status": "Sent to Saved Messages" if success else "Failed"})
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
    return jsonify({
        "status": "disabled",
        "reason": "This route is disabled in safe mode"
    }), 403


def start_broadcaster_on_boot():
    if not AUTO_START_BROADCASTER:
        logger.info("Auto start broadcaster disabled")
        return

    if not ENABLE_GROUP_SEND:
        logger.warning("Auto start blocked because ENABLE_GROUP_SEND is false")
        return

    time.sleep(10)
    asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)
    logger.info("Broadcaster auto started on boot")


threading.Thread(target=start_broadcaster_on_boot, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
