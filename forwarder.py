import os
import logging
import asyncio
import threading
import random
import time
import requests
import io
import statistics
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
broadcaster_start_lock = threading.Lock()
rotation_index = 0
last_chart_sent_at = 0.0

MIN_CHART_GAP_SECONDS = 25 * 60


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


def yahoo_symbol(symbol):
    mapping = {
        "XAU/USD": "GC=F",
        "BTC/USD": "BTC-USD",
        "WTI/USD": "CL=F",
    }

    return mapping.get(symbol, symbol)


def yahoo_interval(interval):
    mapping = {
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "1h",
        "1day": "1d",
    }

    return mapping.get(interval, "1h")


def chart_interval(interval):
    mapping = {
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "4h",
        "1day": "1D",
    }

    return mapping.get(interval, "1h")


def human_interval(interval):
    mapping = {
        "5min": "5M",
        "15min": "15M",
        "30min": "30M",
        "1h": "1H",
        "4h": "4H",
        "1day": "Daily",
    }

    return mapping.get(interval, interval)


def get_twelve_data_interval(interval):
    mapping = {
        "5min": "5min",
        "15min": "15min",
        "30min": "30min",
        "1h": "1h",
        "4h": "4h",
        "1day": "1day",
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
        ema50 = calculate_ema(closes, 50)
        ema200 = calculate_ema(closes, 200)
        rsi = calculate_rsi(closes, 14)
        bb = calculate_bollinger(closes, 20)

        return {
            "price": round(price, 4),
            "rsi": round(rsi, 1),
            "ema": round(ema50, 4),
            "ema200": round(ema200, 4),
            "bb_upper": round(bb["bb_upper"], 4),
            "bb_middle": round(bb["bb_middle"], 4),
            "bb_lower": round(bb["bb_lower"], 4),
            "source": "twelve_data"
        }

    except Exception as e:
        logger.error(f"Twelve Data error for {symbol}: {e}")
        return None


def get_live_data_from_yahoo(symbol, interval):
    try:
        y_symbol = yahoo_symbol(symbol)
        y_interval = yahoo_interval(interval)

        if y_interval in ["5m", "15m", "30m"]:
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
        ema50 = calculate_ema(closes, 50)
        ema200 = calculate_ema(closes, 200)
        rsi = calculate_rsi(closes, 14)
        bb = calculate_bollinger(closes, 20)

        return {
            "price": round(price, 4),
            "rsi": round(rsi, 1),
            "ema": round(ema50, 4),
            "ema200": round(ema200, 4),
            "bb_upper": round(bb["bb_upper"], 4),
            "bb_middle": round(bb["bb_middle"], 4),
            "bb_lower": round(bb["bb_lower"], 4),
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
        "oil": "TVC:USOIL",
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

    return f"{price:.2f}"


def level_format(asset_name, level):
    if asset_name == "bitcoin":
        return f"{level:,.0f}"

    return f"{level:.2f}"


def display_asset(asset_name):
    mapping = {
        "gold": "Gold",
        "bitcoin": "BTC",
        "oil": "USOIL",
    }

    return mapping.get(asset_name, asset_name.upper())


def asset_context(asset_name):
    mapping = {
        "gold": "Gold reacts strongly to dollar movement, liquidity, and risk sentiment, so clean confirmation matters.",
        "bitcoin": "BTC can move fast around liquidity zones, so I’m watching momentum and structure together.",
        "oil": "USOIL is sensitive to supply headlines, dollar movement, and risk sentiment, so I’m watching reactions carefully.",
    }

    return mapping.get(asset_name, "The market is moving around an important zone, so confirmation matters.")


def momentum_phrase(rsi):
    if rsi >= 70:
        return f"RSI is around {rsi}, which shows the market is stretched into an overbought area, so I would be careful chasing late entries."

    if rsi >= 55:
        return f"RSI is around {rsi}, which shows buyers still have momentum without the chart looking too stretched yet."

    if rsi >= 45:
        return f"RSI is around {rsi}, which shows momentum is fairly balanced and the next clean break matters."

    if rsi >= 35:
        return f"RSI is around {rsi}, which shows sellers still have some pressure, but confirmation is still important."

    return f"RSI is around {rsi}, which shows sellers have momentum, but price is getting close to an oversold area."


def bias_details(asset_name, price, ema50, ema200, rsi, bb_upper, bb_lower):
    above_50 = price > ema50
    above_200 = price > ema200
    near_upper_band = price >= bb_upper * 0.998
    near_lower_band = price <= bb_lower * 1.002

    if above_50 and above_200 and rsi >= 52:
        return {
            "bias": "bullish",
            "trade_word": "buys",
            "opposite_word": "sells",
            "key_level": ema50,
            "position_phrase": "while price holds above the main EMA zone",
            "shift_phrase": "if price loses that EMA zone cleanly"
        }

    if not above_50 and not above_200 and rsi <= 50:
        return {
            "bias": "bearish",
            "trade_word": "sells",
            "opposite_word": "buys",
            "key_level": ema50,
            "position_phrase": "while price stays below the main EMA zone",
            "shift_phrase": "if price breaks back above that EMA zone cleanly"
        }

    if near_upper_band and rsi >= 60:
        return {
            "bias": "bullish but slightly stretched",
            "trade_word": "buys on pullbacks",
            "opposite_word": "sells",
            "key_level": ema50,
            "position_phrase": "while momentum remains strong but not overextended",
            "shift_phrase": "if price starts rejecting the upper Bollinger Band"
        }

    if near_lower_band and rsi <= 40:
        return {
            "bias": "bearish but slightly stretched",
            "trade_word": "sells on pullbacks",
            "opposite_word": "buys",
            "key_level": ema50,
            "position_phrase": "while sellers keep pressure near the lower band",
            "shift_phrase": "if price starts bouncing strongly from the lower Bollinger Band"
        }

    if price >= ema50:
        return {
            "bias": "mixed but slightly bullish",
            "trade_word": "buys",
            "opposite_word": "sells",
            "key_level": max(ema50, ema200),
            "position_phrase": "while price holds above the short term EMA area",
            "shift_phrase": "if price fails to hold above that short term support"
        }

    return {
        "bias": "mixed but slightly bearish",
        "trade_word": "sells",
        "opposite_word": "buys",
        "key_level": max(ema50, ema200),
        "position_phrase": "while price remains below the short term EMA area",
        "shift_phrase": "if price reclaims that level with strength"
    }


def generate_market_message(asset_name, data, interval):
    name = display_asset(asset_name)

    price = data["price"]
    rsi = data["rsi"]
    ema50 = data["ema"]
    ema200 = data["ema200"]
    bb_upper = data["bb_upper"]
    bb_middle = data["bb_middle"]
    bb_lower = data["bb_lower"]

    price_text = price_format(asset_name, price)
    ema50_text = level_format(asset_name, ema50)
    ema200_text = level_format(asset_name, ema200)
    bb_upper_text = level_format(asset_name, bb_upper)
    bb_middle_text = level_format(asset_name, bb_middle)
    bb_lower_text = level_format(asset_name, bb_lower)
    visible_interval = human_interval(interval)

    details = bias_details(asset_name, price, ema50, ema200, rsi, bb_upper, bb_lower)
    bias = details["bias"]
    trade_word = details["trade_word"]
    opposite_word = details["opposite_word"]
    key_level_text = level_format(asset_name, details["key_level"])
    position_phrase = details["position_phrase"]
    shift_phrase = details["shift_phrase"]

    context = asset_context(asset_name)
    momentum = momentum_phrase(rsi)

    line1_options = [
        f"✅ {name} is trading around {price_text} on the {visible_interval} timeframe, and the current view is leaning {bias}.",
        f"✅ Looking at the {visible_interval} chart, {name} is reacting around {price_text}, so this zone matters right now.",
        f"✅ {name} is sitting near {price_text} on the {visible_interval} timeframe, with the market still building its next move.",
        f"✅ The {visible_interval} picture on {name} is still developing, with price currently around {price_text}.",
        f"✅ {name} is moving close to {price_text} on the {visible_interval}, and confirmation is still important before forcing a trade."
    ]

    line2_options = [
        f"✅ EMA 50 is near {ema50_text} and EMA 200 is near {ema200_text}, which gives a cleaner view of the current structure.",
        f"✅ {momentum}",
        f"✅ Bollinger Bands are sitting between {bb_lower_text} and {bb_upper_text}, so I’m watching for reactions near those areas.",
        f"✅ Price is close to the Bollinger midline near {bb_middle_text}, which usually means the market still needs a stronger push.",
        f"✅ The EMA area around {ema50_text} and {ema200_text} is the key zone I’m watching for direction.",
        f"✅ {momentum} The EMA structure also shows why confirmation matters here."
    ]

    line3_options = [
        f"✅ For now, {trade_word} make more sense {position_phrase}, but I would change view {shift_phrase}.",
        f"✅ Bias stays with {trade_word} for now, with {key_level_text} acting as the level that can shift momentum.",
        f"✅ I would be more comfortable looking for {trade_word}, but only with confirmation around these levels.",
        f"✅ At the moment, {trade_word} still look cleaner, while {opposite_word} need a stronger break before looking attractive.",
        f"✅ If {name} respects this area again, {trade_word} can stay in play, but a clean break changes the picture.",
        f"✅ Overall, I would keep focus on {trade_word} for now, while watching {key_level_text} as the level that can change the bias.",
        f"✅ {context} For now, {trade_word} still look like the cleaner side unless the structure changes."
    ]

    return f"**🔔 Market Update**\n\n{random.choice(line1_options)}\n\n{random.choice(line2_options)}\n\n{random.choice(line3_options)}"


def generate_engagement_reply(asset_name, interval):
    name = display_asset(asset_name)
    visible_interval = human_interval(interval)

    replies = [
        f"What do you guys think on {name} here, bullish or bearish from this zone?",
        f"Team, would you wait for confirmation on {name} or are you already seeing the move?",
        f"Is everyone seeing the same structure on the {visible_interval} chart, or would you wait?",
        f"Would you prefer buys or sells on {name} from this area?",
        f"Interesting zone on {name}. Are you leaning with the trend or waiting for a cleaner break?",
        f"Who is watching this {visible_interval} setup closely?",
        f"Does this look like continuation to you, or a possible reversal area?",
        f"Let’s hear your thoughts, would you trade this or wait for more confirmation?",
        f"From this level, are you more confident with buys or sells?",
        f"Good learning setup here. What would you need to see before entering?"
    ]

    return random.choice(replies)


def rotation_assets():
    return [
        {
            "symbol": "XAU/USD",
            "name": "gold",
            "interval": random.choice(["15min", "30min", "1h", "4h"])
        },
        {
            "symbol": "BTC/USD",
            "name": "bitcoin",
            "interval": random.choice(["30min", "1h", "4h"])
        },
        {
            "symbol": "WTI/USD",
            "name": "oil",
            "interval": random.choice(["15min", "30min", "1h", "4h"])
        }
    ]


def choose_asset():
    global rotation_index

    assets = rotation_assets()
    asset = assets[rotation_index % len(assets)]
    rotation_index += 1

    return asset


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
        "oil": {
            "symbol": "WTI/USD",
            "name": "oil"
        },
        "usoil": {
            "symbol": "WTI/USD",
            "name": "oil"
        },
        "wti": {
            "symbol": "WTI/USD",
            "name": "oil"
        }
    }

    valid_intervals = ["5min", "15min", "30min", "1h", "4h", "1day"]

    if requested_asset in asset_map:
        asset = asset_map[requested_asset]
        asset["interval"] = requested_interval if requested_interval in valid_intervals else "30min"
        return asset

    return choose_asset()


def choose_wait_minutes():
    return random.choice([30, 45])


def choose_engagement_delay_minutes():
    return random.choice([12, 15, 18])


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


async def send_engagement_reply_later(asset_name, interval, reply_to_message_id):
    if not ENABLE_GROUP_SEND:
        return

    delay_minutes = choose_engagement_delay_minutes()
    logger.info(f"Engagement reply scheduled in {delay_minutes} minutes")

    await asyncio.sleep(delay_minutes * 60)

    if not broadcaster_running:
        logger.info("Engagement reply skipped because broadcaster stopped")
        return

    reply_text = generate_engagement_reply(asset_name, interval)

    sent = await send_to_vantage(
        reply_text,
        chart_image=None,
        reply_to=reply_to_message_id
    )

    if sent:
        logger.info("Engagement reply sent")


async def create_market_update(send_mode="preview"):
    global last_chart_sent_at

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

        sent = await send_to_saved_messages(message, chart_image=chart_result["image"])

        return {
            "ok": bool(sent),
            "asset": asset,
            "data": data,
            "message": message,
            "chart_image": chart_status,
            "sent": "saved_messages" if sent else "failed"
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

        sent = await send_to_vantage(message, chart_image=chart_result["image"])

        if sent:
            last_chart_sent_at = time.time()

            sent_id = sent.id if hasattr(sent, "id") else None

            if sent_id:
                asyncio.create_task(
                    send_engagement_reply_later(
                        asset["name"],
                        asset["interval"],
                        sent_id
                    )
                )

        return {
            "ok": bool(sent),
            "asset": asset,
            "data": data,
            "message": message,
            "chart_image": chart_status,
            "message_id": sent.id if sent and hasattr(sent, "id") else None,
            "engagement_reply": "scheduled" if sent else "not scheduled",
            "sent": "vantage" if sent else "failed"
        }

    return {
        "ok": True,
        "asset": asset,
        "data": data,
        "message": message,
        "chart_image": chart_status,
        "chart_error": chart_error
    }


async def wait_if_too_soon():
    global last_chart_sent_at

    if last_chart_sent_at <= 0:
        return

    seconds_since_last = time.time() - last_chart_sent_at

    if seconds_since_last < MIN_CHART_GAP_SECONDS:
        wait_seconds = int(MIN_CHART_GAP_SECONDS - seconds_since_last)
        logger.info(f"Too soon since last chart. Waiting {wait_seconds} seconds.")
        await asyncio.sleep(wait_seconds)


async def broadcast_loop():
    global broadcaster_running

    if not ENABLE_GROUP_SEND:
        logger.warning("Broadcaster blocked because ENABLE_GROUP_SEND is false")
        broadcaster_running = False
        return

    logger.info("Auto broadcaster started")

    while broadcaster_running:
        try:
            await wait_if_too_soon()

            if not broadcaster_running:
                break

            result = await create_market_update(send_mode="vantage")
            logger.info(f"Broadcast result: {result}")

            wait_minutes = choose_wait_minutes()
            logger.info(f"Next chart message in {wait_minutes} minutes")
            await asyncio.sleep(wait_minutes * 60)

        except Exception as e:
            logger.error(f"Broadcaster error: {e}")
            await asyncio.sleep(60)

    logger.info("Broadcaster loop exited")


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
        "rotation": "Gold, BTC, USOIL",
        "chart_post_interval_minutes": "30 or 45",
        "engagement_reply_delay_minutes": "12 to 18",
        "duplicate_loop_protection": True,
        "minimum_chart_gap_minutes": 25,
        "safe_test_saved_messages": "/send_saved_test",
        "safe_chart_preview": "/preview_chart",
        "safe_text_preview": "/preview_analysis"
    })


@app.route("/start_broadcaster", methods=["GET"])
def start_broadcaster():
    global broadcaster_running

    with broadcaster_start_lock:
        if broadcaster_running:
            return jsonify({"status": "Already running"})

        if not ENABLE_GROUP_SEND:
            return jsonify({
                "status": "blocked",
                "reason": "ENABLE_GROUP_SEND is false, so the group is protected"
            }), 403

        broadcaster_running = True
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
        "**🔔 Market Update**\n\n✅ Gold is trading around 4182.40 on the 15M timeframe, and the current view is leaning bearish.\n\n✅ RSI is around 38, while EMA 50 and EMA 200 are still acting as key levels.\n\n✅ For now, sells make more sense unless Gold breaks back above the main EMA zone."
    )

    future = asyncio.run_coroutine_threadsafe(send_to_saved_messages(message), loop)

    try:
        sent = future.result(timeout=20)
        return jsonify({"status": "Sent to Saved Messages" if sent else "Failed"})
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
    global broadcaster_running

    if not AUTO_START_BROADCASTER:
        logger.info("Auto start broadcaster disabled")
        return

    if not ENABLE_GROUP_SEND:
        logger.warning("Auto start blocked because ENABLE_GROUP_SEND is false")
        return

    time.sleep(10)

    with broadcaster_start_lock:
        if broadcaster_running:
            logger.info("Broadcaster already running on boot")
            return

        broadcaster_running = True
        asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)

    logger.info("Broadcaster auto started on boot")


threading.Thread(target=start_broadcaster_on_boot, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
