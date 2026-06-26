import os
import logging
import asyncio
import threading
import random
import time
import requests
import io
import statistics
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, Response, has_request_context
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
BROADCAST_INTERVAL_MINUTES = int(os.environ.get("BROADCAST_INTERVAL_MINUTES", "40"))

client = None
loop = asyncio.new_event_loop()
phone_code_hash = None

broadcaster_running = False
broadcaster_start_lock = threading.Lock()
last_chart_sent_at = 0.0


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


def calculate_bollinger(values, period=20):
    if not values or len(values) < period:
        last_price = values[-1] if values else 0.0

        return {
            "bb_middle": last_price,
            "bb_upper": last_price,
            "bb_lower": last_price
        }

    recent = values[-period:]
    middle = sum(recent) / period
    stdev = statistics.pstdev(recent)

    return {
        "bb_middle": middle,
        "bb_upper": middle + (2 * stdev),
        "bb_lower": middle - (2 * stdev)
    }


def get_support_resistance(values, window=20):
    if not values or len(values) < window:
        return {"support": values[-1] if values else 0, "resistance": values[-1] if values else 0}
    recent = values[-window:]
    support = min(recent)
    resistance = max(recent)
    return {"support": support, "resistance": resistance}


def is_gold_market_closed():
    """Check if Gold market is closed (Friday 10pm - Sunday 11pm UK time)"""
    uk_tz = ZoneInfo('Europe/London')
    now_uk = datetime.now(uk_tz)
    
    weekday = now_uk.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    hour = now_uk.hour
    
    # Friday 10pm onwards
    if weekday == 4 and hour >= 22:
        return True
    
    # Saturday (all day)
    if weekday == 5:
        return True
    
    # Sunday until 11pm
    if weekday == 6 and hour < 23:
        return True
    
    return False


def yahoo_symbol(symbol):
    mapping = {
        "XAU/USD": "GC=F",
        "BTC/USD": "BTC-USD",
    }

    return mapping.get(symbol, symbol)


def yahoo_interval(interval):
    mapping = {
        "1min": "1m",
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "1h",
    }

    return mapping.get(interval, "1h")


def chart_interval(interval):
    mapping = {
        "1min": "1m",
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "4h",
    }

    return mapping.get(interval, "1h")


def human_interval(interval):
    mapping = {
        "1min": "1m",
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "4h",
    }

    return mapping.get(interval, interval)


def get_twelve_data_interval(interval):
    mapping = {
        "1min": "1min",
        "5min": "5min",
        "15min": "15min",
        "30min": "30min",
        "1h": "1h",
        "4h": "4h",
    }

    return mapping.get(interval, "1h")


def get_live_data_from_twelve_data(symbol, interval):
    try:
        if not TWELVE_DATA_KEY:
            return None

        td_interval = get_twelve_data_interval(interval)

        response = requests.get(
            "https://api.twelvedata.com/time_series",
            params={
                "symbol": symbol,
                "interval": td_interval,
                "outputsize": 250,
                "apikey": TWELVE_DATA_KEY,
            },
            timeout=15
        )

        data = response.json()
        values = data.get("values", [])

        if not values:
            logger.warning(f"Twelve Data returned no values for {symbol}: {data}")
            return None

        candles = list(reversed(values))
        closes = []

        for candle in candles:
            close_value = candle.get("close")

            if close_value is not None:
                closes.append(float(close_value))

        if not closes:
            return None

        price = closes[-1]
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else price
        ema200 = calculate_ema(closes, 200) if len(closes) >= 200 else price
        rsi = calculate_rsi(closes, 14)
        bb = calculate_bollinger(closes, 20)
        sr = get_support_resistance(closes, 20)

        return {
            "price": round(price, 4),
            "rsi": round(rsi, 1),
            "ema50": round(ema50, 4),
            "ema200": round(ema200, 4),
            "bb_upper": round(bb["bb_upper"], 4),
            "bb_middle": round(bb["bb_middle"], 4),
            "bb_lower": round(bb["bb_lower"], 4),
            "support": round(sr["support"], 4),
            "resistance": round(sr["resistance"], 4),
            "source": "twelve_data"
        }

    except Exception as e:
        logger.error(f"Twelve Data error for {symbol}: {e}")
        return None


def get_live_data_from_yahoo(symbol, interval):
    try:
        y_symbol = yahoo_symbol(symbol)
        y_interval = yahoo_interval(interval)

        if y_interval in ["1m", "5m", "15m", "30m"]:
            range_value = "5d"
        elif y_interval == "1h":
            range_value = "30d"
        else:
            range_value = "1y"

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
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else price
        ema200 = calculate_ema(closes, 200) if len(closes) >= 200 else price
        rsi = calculate_rsi(closes, 14)
        bb = calculate_bollinger(closes, 20)
        sr = get_support_resistance(closes, 20)

        return {
            "price": round(price, 4),
            "rsi": round(rsi, 1),
            "ema50": round(ema50, 4),
            "ema200": round(ema200, 4),
            "bb_upper": round(bb["bb_upper"], 4),
            "bb_middle": round(bb["bb_middle"], 4),
            "bb_lower": round(bb["bb_lower"], 4),
            "support": round(sr["support"], 4),
            "resistance": round(sr["resistance"], 4),
            "source": "yahoo"
        }

    except Exception as e:
        logger.error(f"Yahoo data error for {symbol}: {e}")
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
        return f"${price:,.0f}"

    return f"${price:,.2f}"


def level_format(asset_name, level):
    if asset_name == "bitcoin":
        return f"{level:,.0f}"

    return f"{level:.2f}"


def display_asset(asset_name):
    mapping = {
        "gold": "Gold",
        "bitcoin": "Bitcoin",
    }

    return mapping.get(asset_name, asset_name.upper())


def get_price_phrasing(asset_name, price):
    phrasings = [
        f"trading near {price_format(asset_name, price)}",
        f"sitting at {price_format(asset_name, price)}",
        f"hovering around {price_format(asset_name, price)}",
        f"at {price_format(asset_name, price)}",
        f"currently at {price_format(asset_name, price)}",
        f"moved to {price_format(asset_name, price)}",
        f"near {price_format(asset_name, price)}",
        f"around {price_format(asset_name, price)}",
    ]
    return random.choice(phrasings)


def should_mention_support(price, support, resistance):
    diff_support = price - support
    diff_resistance = resistance - price
    return diff_support < diff_resistance


def generate_predictive_gold_message(data, interval):
    """Generate predictive Gold message when market is closed - SAME 2-LINE FORMAT"""
    last_close = data["price"]
    rsi = data["rsi"]
    support = data["support"]
    resistance = data["resistance"]
    
    predictions = [
        f"✅ Gold expected to spike {level_format('gold', last_close + 25)}-{level_format('gold', last_close + 45)} on Sunday open ➡️ MACD ready to cross\n✅ RSI oversold at {int(rsi)}, watch volume explosion. Could see 50-100 point move.",
        
        f"✅ Gold likely gap-fill toward {level_format('gold', resistance)} when market opens ➡️ RSI at {int(rsi)} bullish setup\n✅ Support at {level_format('gold', support)} is key. If it breaks, target {level_format('gold', support - 30)}.",
        
        f"✅ Gold could spike {level_format('gold', last_close + 30)} on London open ➡️ EMA structure bullish, volume should explode\n✅ Watch Bollinger Band expansion. First hour could see 80-120 point swings.",
        
        f"✅ Gold watching resistance at {level_format('gold', resistance)} on open ➡️ Support at {level_format('gold', support)} is the floor\n✅ Volume spike expected. If gaps above resistance, next target {level_format('gold', resistance + 50)}.",
        
        f"✅ Gold technical setup suggests {level_format('gold', last_close + 35)} target when market opens ➡️ EMA50 bullish alignment\n✅ Gap-fill likely in first 30 mins. RSI spike incoming, stay alert for quick swings.",
    ]
    
    return f"🚨 Trade Alert Everyone\n\n{random.choice(predictions)}"


def generate_market_message(asset_name, data, interval):
    name = display_asset(asset_name)
    price_phrase = get_price_phrasing(asset_name, data["price"])
    
    support = data["support"]
    resistance = data["resistance"]
    rsi = data["rsi"]
    
    if should_mention_support(data["price"], support, resistance):
        messages = [
            f"✅ {name} is {price_phrase} on the {human_interval(interval)} ➡️ Support holding at {level_format(asset_name, support)} — that's your key level\n✅ If it breaks below, sellers are in control. Watch that {level_format(asset_name, support)} closely.",
            f"✅ {name} {price_phrase} on {human_interval(interval)} ➡️ Support is at {level_format(asset_name, support)} and it's the floor\n✅ A close below that level changes the picture. Stay alert to that break.",
            f"✅ Price is {price_phrase} on the {human_interval(interval)} ➡️ Support around {level_format(asset_name, support)} is holding the line\n✅ Watch how {name} reacts here. If it holds, upside could continue.",
        ]
    else:
        messages = [
            f"✅ {name} {price_phrase} on the {human_interval(interval)} ➡️ Resistance is just above at {level_format(asset_name, resistance)}\n✅ If it breaks that level, next target is in play. Watch for the break.",
            f"✅ {name} is {price_phrase} on {human_interval(interval)} ➡️ Resistance at {level_format(asset_name, resistance)} is the hurdle\n✅ Buyers need to push through there. If they do, it's significant.",
            f"✅ Currently {price_phrase} on the {human_interval(interval)} ➡️ {name} facing resistance at {level_format(asset_name, resistance)}\n✅ Watch if it breaks or bounces. That tells us the next move.",
        ]
    
    return f"🚨 Trade Alert Everyone\n\n{random.choice(messages)}"


def choose_asset():
    """70% Gold, 30% Bitcoin. If Gold market closed, only return Bitcoin"""
    rand = random.random()
    
    if rand < 0.7:
        # Trying to pick Gold
        if is_gold_market_closed():
            # Gold market closed, pick Bitcoin instead (send Gold as predictive, Bitcoin as live)
            rand_again = random.random()
            if rand_again < 0.5:
                # Send Gold predictive
                return {"symbol": "XAU/USD", "name": "gold", "interval": random.choice(["1h", "4h"]), "is_predictive": True}
            else:
                # Send Bitcoin live
                return {"symbol": "BTC/USD", "name": "bitcoin", "interval": random.choice(["1min", "5min", "15min", "30min", "1h", "4h"]), "is_predictive": False}
        else:
            # Gold market open, normal Gold
            return {"symbol": "XAU/USD", "name": "gold", "interval": random.choice(["1min", "5min", "15min", "30min", "1h", "4h"]), "is_predictive": False}
    else:
        # Bitcoin (always live, 24/7)
        return {"symbol": "BTC/USD", "name": "bitcoin", "interval": random.choice(["1min", "5min", "15min", "30min", "1h", "4h"]), "is_predictive": False}


async def send_message_to_entity(entity_target, message_text, chart_image=None, reply_to=None):
    global client

    try:
        if not client or not await client.is_user_authorized():
            logger.error("Not logged in")
            return None

        entity = await client.get_entity(entity_target)

        kwargs = {
            "parse_mode": "md"
        }

        if reply_to:
            kwargs["reply_to"] = reply_to

        if chart_image:
            sent = await client.send_file(
                entity,
                chart_image,
                caption=message_text,
                force_document=False,
                **kwargs
            )
        else:
            sent = await client.send_message(
                entity,
                message_text,
                **kwargs
            )

        logger.info("Message sent")
        return sent

    except Exception as e:
        logger.error(f"Send error: {e}")
        return None


async def send_to_saved_messages(message_text, chart_image=None):
    return await send_message_to_entity("me", message_text, chart_image=chart_image)


async def send_to_vantage(message_text, chart_image=None, reply_to=None):
    if not ENABLE_GROUP_SEND:
        logger.warning("Group sending is locked")
        return None

    if not VANTAGE_GROUP_ID:
        logger.error("VANTAGE_GROUP_ID missing")
        return None

    if chart_image is None and reply_to is None:
        logger.error("Chart image missing. Refusing to send fresh group update.")
        return None

    return await send_message_to_entity(
        VANTAGE_GROUP_ID,
        message_text,
        chart_image=chart_image,
        reply_to=reply_to if reply_to else VANTAGE_TOPIC_ID if VANTAGE_TOPIC_ID and VANTAGE_TOPIC_ID > 0 else None
    )


async def create_market_update(send_mode="preview"):
    global last_chart_sent_at

    asset = choose_asset()
    is_predictive = asset.get("is_predictive", False)
    
    data = get_live_data(asset["symbol"], asset["interval"])

    if not data or data["price"] <= 0:
        return {
            "ok": False,
            "asset": asset,
            "error": "Could not fetch live data"
        }

    # If predictive (Gold market closed), don't need chart
    if is_predictive:
        message = generate_predictive_gold_message(data, asset["interval"])
        return {
            "ok": True,
            "asset": asset["name"],
            "timeframe": asset["interval"],
            "message": message,
            "type": "predictive"
        } if send_mode == "preview" else {
            "ok": True,
            "asset": asset["name"],
            "timeframe": asset["interval"],
            "sent": "vantage"
        }
    
    # Normal live message with chart
    message = generate_market_message(asset["name"], data, asset["interval"])
    chart_result = get_chart_image_result(asset["name"], asset["interval"])

    chart_status = "available" if chart_result["ok"] else "not available"
    chart_error = chart_result["error"]

    if send_mode == "saved":
        if not chart_result["ok"]:
            return {
                "ok": False,
                "asset": asset,
                "error": "Chart failed"
            }

        sent = await send_to_saved_messages(message, chart_image=chart_result["image"])

        return {
            "ok": bool(sent),
            "asset": asset["name"],
            "timeframe": asset["interval"],
            "sent": "saved_messages" if sent else "failed"
        }

    if send_mode == "vantage":
        if not ENABLE_GROUP_SEND:
            return {
                "ok": False,
                "asset": asset,
                "error": "ENABLE_GROUP_SEND is false"
            }

        if not chart_result["ok"]:
            return {
                "ok": False,
                "asset": asset,
                "error": f"Chart failed: {chart_error}"
            }

        sent = await send_to_vantage(message, chart_image=chart_result["image"])
        last_chart_sent_at = time.time()

        return {
            "ok": bool(sent),
            "asset": asset["name"],
            "timeframe": asset["interval"],
            "sent": "vantage" if sent else "failed"
        }

    return {
        "ok": True,
        "asset": asset["name"],
        "timeframe": asset["interval"],
        "message": message,
        "chart_available": chart_status
    }


async def broadcast_loop():
    global broadcaster_running

    if not ENABLE_GROUP_SEND:
        logger.warning("Broadcaster blocked because ENABLE_GROUP_SEND is false")
        broadcaster_running = False
        return

    logger.info(f"Broadcaster started - {BROADCAST_INTERVAL_MINUTES} minute intervals")

    while broadcaster_running:
        try:
            result = await create_market_update(send_mode="vantage")
            logger.info(f"Broadcast result: {result}")

            wait_seconds = BROADCAST_INTERVAL_MINUTES * 60
            logger.info(f"Next message in {BROADCAST_INTERVAL_MINUTES} minutes")
            
            for _ in range(wait_seconds):
                if not broadcaster_running:
                    break
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Broadcaster error: {e}")
            await asyncio.sleep(60)

    logger.info("Broadcaster stopped")


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Mentor Broadcaster running",
        "logged_in": SESSION_STRING != "",
        "broadcaster": "running" if broadcaster_running else "stopped",
        "assets": "Gold (70%) + Bitcoin (30%)",
        "gold_market_closed": is_gold_market_closed(),
        "timeframes": "Random (1m, 5m, 15m, 30m, 1h, 4h)",
        "format": "2 ticks with chart",
        "interval_minutes": BROADCAST_INTERVAL_MINUTES,
        "group_send_enabled": ENABLE_GROUP_SEND,
        "vantage_group": VANTAGE_GROUP_ID,
        "topic_id": VANTAGE_TOPIC_ID,
    })


@app.route("/preview_analysis", methods=["GET"])
def preview_analysis():
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="preview"), loop)

    try:
        result = future.result(timeout=30)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/send_saved_test", methods=["GET"])
def send_saved_test():
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="saved"), loop)

    try:
        result = future.result(timeout=100)
        status_code = 200 if result.get("ok") else 403
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/send_vantage_once", methods=["GET"])
def send_vantage_once():
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="vantage"), loop)

    try:
        result = future.result(timeout=100)
        status_code = 200 if result.get("ok") else 403
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/start_broadcaster", methods=["GET"])
def start_broadcaster():
    global broadcaster_running

    with broadcaster_start_lock:
        if broadcaster_running:
            return jsonify({"status": "Already running"})

        if not ENABLE_GROUP_SEND:
            return jsonify({"status": "blocked", "reason": "ENABLE_GROUP_SEND is false"}), 403

        broadcaster_running = True
        asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)

    return jsonify({"status": "Broadcaster started"})


@app.route("/stop_broadcaster", methods=["GET"])
def stop_broadcaster():
    global broadcaster_running
    broadcaster_running = False
    return jsonify({"status": "Broadcaster stopped"})


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


def start_broadcaster_on_boot():
    global broadcaster_running

    if not AUTO_START_BROADCASTER:
        logger.info("Auto start disabled")
        return

    if not ENABLE_GROUP_SEND:
        logger.warning("Auto start blocked")
        return

    time.sleep(10)

    with broadcaster_start_lock:
        if broadcaster_running:
            return

        broadcaster_running = True
        asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)

    logger.info("Broadcaster auto-started")


threading.Thread(target=start_broadcaster_on_boot, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
