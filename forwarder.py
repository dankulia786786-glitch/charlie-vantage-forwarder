import os
import logging
import asyncio
import threading
import random
import time
import requests
import io
import statistics
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

client = None
loop = asyncio.new_event_loop()
phone_code_hash = None

broadcaster_running = False
broadcaster_start_lock = threading.Lock()
last_sent_at = 0.0

BROADCAST_INTERVAL_MINUTES = 25


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
        "1min": "1-minute",
        "5min": "5-minute",
        "15min": "15-minute",
        "30min": "30-minute",
        "1h": "1-hour",
        "4h": "4-hour",
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
            logger.warning(f"Twelve Data returned no values for {symbol}")
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
        params = {"interval": y_interval, "range": range_value}
        headers = {"User-Agent": "Mozilla/5.0"}
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
        },
        "studies": [
            {
                "name": "Moving Average Exponential",
                "input": {"length": 50, "source": "close"},
                "override": {
                    "Plot.linewidth": 2,
                    "Plot.plottype": "line",
                    "Plot.color": "rgb(255,193,7)"
                }
            },
            {
                "name": "Moving Average Exponential",
                "input": {"length": 200, "source": "close"},
                "override": {
                    "Plot.linewidth": 2,
                    "Plot.plottype": "line",
                    "Plot.color": "rgb(33,150,243)"
                }
            },
            {
                "name": "Relative Strength Index",
                "input": {"length": 14, "source": "close"}
            }
        ]
    }
    return payload


def get_chart_image_result(asset_name, interval):
    try:
        if not CHART_IMG_KEY:
            return {"ok": False, "image": None, "error": "CHART_IMG_KEY missing"}
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
            return {"ok": True, "image": image, "error": None}
        return {
            "ok": False,
            "image": None,
            "error": f"Chart IMG error {response.status_code}"
        }
    except Exception as e:
        logger.error(f"Chart image fetch error: {e}")
        return {"ok": False, "image": None, "error": str(e)}


def price_format(asset_name, price):
    if asset_name == "bitcoin":
        return f"${price:,.0f}"
    return f"${price:,.2f}"


def level_format(asset_name, level):
    if asset_name == "bitcoin":
        return f"{level:,.0f}"
    return f"{level:.2f}"


def display_asset(asset_name):
    mapping = {"gold": "Gold", "bitcoin": "Bitcoin"}
    return mapping.get(asset_name, asset_name.upper())


def choose_asset():
    """70% Gold, 30% Bitcoin"""
    rand = random.random()
    if rand < 0.7:
        return {"symbol": "XAU/USD", "name": "gold"}
    else:
        return {"symbol": "BTC/USD", "name": "bitcoin"}


def choose_timeframe():
    """Random timeframe: 1m, 5m, 15m, 30m, 1h, 4h"""
    return random.choice(["1min", "5min", "15min", "30min", "1h", "4h"])


# ==================== MESSAGE GENERATION ====================

def determine_market_bias(price, ema50, ema200, rsi, support, resistance):
    """Determine if market is bullish, bearish, or neutral"""
    bullish_count = 0
    bearish_count = 0
    
    if price > ema50:
        bullish_count += 1
    else:
        bearish_count += 1
    
    if price > ema200:
        bullish_count += 1
    else:
        bearish_count += 1
    
    if rsi > 55:
        bullish_count += 1
    elif rsi < 45:
        bearish_count += 1
    
    if bullish_count > bearish_count:
        return "bullish"
    elif bearish_count > bullish_count:
        return "bearish"
    else:
        return "neutral"


def should_mention_support(price, support, resistance):
    """Decide if support or resistance is more relevant"""
    diff_support = price - support
    diff_resistance = resistance - price
    return diff_support < diff_resistance


def generate_message_type_a(asset_name, price, timeframe, support, resistance):
    """Type A: Support/Resistance focus - price testing key level"""
    name = display_asset(asset_name)
    
    if should_mention_support(price, support, resistance):
        # Talking about support
        messages = [
            f"✅ {name} is at {price_format(asset_name, price)} on the {timeframe} ➡️ Support holding at {level_format(asset_name, support)} — that's your key level\n✅ If it breaks below, sellers are in control. Watch that {level_format(asset_name, support)} closely.",
            
            f"✅ {name} sitting at {price_format(asset_name, price)} on {timeframe} ➡️ Support is at {level_format(asset_name, support)} and it's the floor\n✅ A close below that level changes the picture. Stay alert to that break.",
            
            f"✅ Price is {price_format(asset_name, price)} on the {timeframe} ➡️ Support around {level_format(asset_name, support)} is holding the line\n✅ Watch how {name} reacts here. If it holds, upside could continue.",
        ]
    else:
        # Talking about resistance
        messages = [
            f"✅ {name} at {price_format(asset_name, price)} on the {timeframe} ➡️ Resistance is just above at {level_format(asset_name, resistance)}\n✅ If it breaks that level, next target is in play. Watch for the break.",
            
            f"✅ {name} is trading {price_format(asset_name, price)} on {timeframe} ➡️ Resistance at {level_format(asset_name, resistance)} is the hurdle\n✅ Buyers need to push through there. If they do, it's significant.",
            
            f"✅ Current level is {price_format(asset_name, price)} on the {timeframe} ➡️ {name} facing resistance at {level_format(asset_name, resistance)}\n✅ Watch if it breaks or bounces. That tells us the next move.",
        ]
    
    return random.choice(messages)


def generate_message_type_b(asset_name, price, timeframe, rsi, support, resistance):
    """Type B: RSI/Momentum focus"""
    name = display_asset(asset_name)
    
    if rsi >= 65:
        messages = [
            f"✅ {name} at {price_format(asset_name, price)} on the {timeframe} ➡️ RSI is strong at {int(rsi)}, buyers in control\n✅ Watch for pullbacks to {level_format(asset_name, support)} — that's where we'd want to see buyers step back in.",
            
            f"✅ Price is {price_format(asset_name, price)} on {timeframe} ➡️ Momentum is high at RSI {int(rsi)}\n✅ This could continue, but watch resistance at {level_format(asset_name, resistance)}. That's where it could get contested.",
        ]
    elif rsi <= 40:
        messages = [
            f"✅ {name} is {price_format(asset_name, price)} on the {timeframe} ➡️ RSI at {int(rsi)} shows selling pressure\n✅ Support at {level_format(asset_name, support)} matters here. If it holds, we could see a bounce.",
            
            f"✅ Price sitting at {price_format(asset_name, price)} on {timeframe} ➡️ RSI is {int(rsi)}, which means weakness\n✅ Watch that support level at {level_format(asset_name, support)}. A break would signal more downside.",
        ]
    else:
        messages = [
            f"✅ {name} at {price_format(asset_name, price)} on the {timeframe} ➡️ RSI is neutral at {int(rsi)}\n✅ No clear momentum yet. Next move from support at {level_format(asset_name, support)} or resistance at {level_format(asset_name, resistance)} will tell us.",
            
            f"✅ {name} trading {price_format(asset_name, price)} on {timeframe} ➡️ Momentum is balanced, RSI at {int(rsi)}\n✅ Price could test either side. Watch both {level_format(asset_name, support)} and {level_format(asset_name, resistance)}.",
        ]
    
    return random.choice(messages)


def generate_message_type_c(asset_name, price, timeframe, ema50, ema200, support):
    """Type C: Trend/Structure focus"""
    name = display_asset(asset_name)
    
    if price > ema50 > ema200:
        messages = [
            f"✅ {name} is {price_format(asset_name, price)} on the {timeframe} ➡️ Price above both moving averages — bullish setup\n✅ The trend is up. Only break back below the EMAs would change that.",
            
            f"✅ Price {price_format(asset_name, price)} on {timeframe} ➡️ Structure is bullish, price above the 50 and 200\n✅ Dips to {level_format(asset_name, support)} would be buying opportunities.",
        ]
    elif price < ema50 < ema200:
        messages = [
            f"✅ {name} at {price_format(asset_name, price)} on the {timeframe} ➡️ Price below both EMAs — bearish structure\n✅ The trend is down. Watch that support at {level_format(asset_name, support)} closely.",
            
            f"✅ Trading {price_format(asset_name, price)} on {timeframe} ➡️ Price below moving averages, structure is bearish\n✅ Weakness is the story here. Support at {level_format(asset_name, support)} is key to watch.",
        ]
    else:
        messages = [
            f"✅ {name} is {price_format(asset_name, price)} on the {timeframe} ➡️ Structure is mixed, in between the EMAs\n✅ Waiting for a clean break. If it takes support at {level_format(asset_name, support)}, we have clarity.",
            
            f"✅ Price at {price_format(asset_name, price)} on {timeframe} ➡️ No clear direction from the moving averages yet\n✅ Watch how it reacts to {level_format(asset_name, support)}. That will tell us the next move.",
        ]
    
    return random.choice(messages)


def generate_message_type_d(asset_name, price, timeframe, bb_upper, bb_lower, support, resistance):
    """Type D: Volatility/Range focus"""
    name = display_asset(asset_name)
    
    if price >= bb_upper * 0.98:
        messages = [
            f"✅ {name} at {price_format(asset_name, price)} on the {timeframe} ➡️ Touching the upper band, price is hot\n✅ Watch for pullbacks. Support at {level_format(asset_name, support)} is where pullbacks matter.",
            
            f"✅ Price {price_format(asset_name, price)} on {timeframe} ➡️ Upper Bollinger Band is being tested\n✅ Could reject here or break through. That resistance at {level_format(asset_name, resistance)} is important.",
        ]
    elif price <= bb_lower * 1.02:
        messages = [
            f"✅ {name} is {price_format(asset_name, price)} on the {timeframe} ➡️ Lower band territory, volatility is extended\n✅ Bounces from here can be sharp. Watch resistance at {level_format(asset_name, resistance)} for the pullback target.",
            
            f"✅ Trading {price_format(asset_name, price)} on {timeframe} ➡️ At the lower Bollinger Band\n✅ This is usually a bounce zone. Support at {level_format(asset_name, support)} is where the base is.",
        ]
    else:
        messages = [
            f"✅ {name} at {price_format(asset_name, price)} on the {timeframe} ➡️ Normal range, between the bands\n✅ Room to move in either direction. Support at {level_format(asset_name, support)}, resistance at {level_format(asset_name, resistance)}.",
            
            f"✅ Price {price_format(asset_name, price)} on {timeframe} ➡️ Comfortable range, volatility is steady\n✅ Watch the key levels. {level_format(asset_name, support)} is support, {level_format(asset_name, resistance)} is resistance.",
        ]
    
    return random.choice(messages)


def generate_mentor_message(asset_name, data, interval):
    """Generate context-aware message based on market conditions"""
    
    name = display_asset(asset_name)
    price = data["price"]
    rsi = data["rsi"]
    support = data["support"]
    resistance = data["resistance"]
    ema50 = data["ema50"]
    ema200 = data["ema200"]
    
    # Determine which message type makes most sense
    bias = determine_market_bias(price, ema50, ema200, rsi, support, resistance)
    
    # Pick a message type (rotate through A, B, C, D)
    message_type = random.choice(["A", "B", "C", "D"])
    
    if message_type == "A":
        content = generate_message_type_a(asset_name, price, interval, support, resistance)
    elif message_type == "B":
        content = generate_message_type_b(asset_name, price, interval, rsi, support, resistance)
    elif message_type == "C":
        content = generate_message_type_c(asset_name, price, interval, ema50, ema200, support)
    else:  # D
        content = generate_message_type_d(asset_name, price, interval, data["bb_upper"], data["bb_lower"], support, resistance)
    
    # Format with siren and space
    message = f"🚨 Trade Alert Everyone\n\n{content}"
    
    return message


async def send_message_to_entity(entity_target, message_text, chart_image=None):
    """Send message to Telegram entity"""
    global client
    
    try:
        if not client or not await client.is_user_authorized():
            logger.error("Not logged in")
            return None
        
        entity = await client.get_entity(entity_target)
        
        if chart_image:
            sent = await client.send_file(
                entity,
                chart_image,
                caption=message_text,
                force_document=False,
                parse_mode="md"
            )
        else:
            sent = await client.send_message(
                entity,
                message_text,
                parse_mode="md"
            )
        
        logger.info("Message sent successfully")
        return sent
    
    except Exception as e:
        logger.error(f"Send error: {e}")
        return None


async def send_to_saved_messages(message_text, chart_image=None):
    """Send to Tony's Saved Messages"""
    return await send_message_to_entity("me", message_text, chart_image=chart_image)


async def send_to_vantage(message_text, chart_image=None):
    """Send to Vantage group"""
    if not ENABLE_GROUP_SEND:
        logger.warning("Group sending is locked")
        return None
    
    if not VANTAGE_GROUP_ID:
        logger.error("VANTAGE_GROUP_ID missing")
        return None
    
    return await send_message_to_entity(
        VANTAGE_GROUP_ID,
        message_text,
        chart_image=chart_image
    )


async def create_market_update(send_mode="preview"):
    """Create and optionally send market update"""
    
    # Choose asset and timeframe
    asset = choose_asset()
    timeframe = choose_timeframe()
    
    # Get live data
    data = get_live_data(asset["symbol"], timeframe)
    
    if not data or data["price"] <= 0:
        return {
            "ok": False,
            "asset": asset["name"],
            "timeframe": timeframe,
            "error": "Could not fetch live data"
        }
    
    # Generate message
    message = generate_mentor_message(asset["name"], data, timeframe)
    
    # Get chart
    chart_result = get_chart_image_result(asset["name"], timeframe)
    chart_ok = chart_result["ok"]
    
    if send_mode == "preview":
        return {
            "ok": True,
            "asset": asset["name"],
            "timeframe": timeframe,
            "price": data["price"],
            "message": message,
            "chart_available": chart_ok
        }
    
    elif send_mode == "saved":
        if not chart_ok:
            return {
                "ok": False,
                "asset": asset["name"],
                "timeframe": timeframe,
                "error": "Chart image failed to generate"
            }
        
        sent = await send_to_saved_messages(message, chart_image=chart_result["image"])
        
        return {
            "ok": bool(sent),
            "asset": asset["name"],
            "timeframe": timeframe,
            "sent_to": "saved_messages" if sent else "failed"
        }
    
    elif send_mode == "vantage":
        if not ENABLE_GROUP_SEND:
            return {
                "ok": False,
                "asset": asset["name"],
                "error": "ENABLE_GROUP_SEND is false. Set it to true in Railway Variables."
            }
        
        if not chart_ok:
            return {
                "ok": False,
                "asset": asset["name"],
                "timeframe": timeframe,
                "error": "Chart image failed"
            }
        
        sent = await send_to_vantage(message, chart_image=chart_result["image"])
        
        return {
            "ok": bool(sent),
            "asset": asset["name"],
            "timeframe": timeframe,
            "sent_to": "vantage_group" if sent else "failed"
        }


async def broadcast_loop():
    """Main broadcast loop - sends every 25 minutes"""
    global broadcaster_running, last_sent_at
    
    if not ENABLE_GROUP_SEND:
        logger.warning("Broadcaster blocked because ENABLE_GROUP_SEND is false")
        broadcaster_running = False
        return
    
    logger.info("Mentor broadcaster started — 25 minute intervals")
    
    while broadcaster_running:
        try:
            result = await create_market_update(send_mode="vantage")
            logger.info(f"Broadcast result: {result}")
            
            last_sent_at = time.time()
            
            # Sleep for 25 minutes
            wait_seconds = BROADCAST_INTERVAL_MINUTES * 60
            logger.info(f"Next update in {BROADCAST_INTERVAL_MINUTES} minutes")
            
            for _ in range(wait_seconds):
                if not broadcaster_running:
                    break
                await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Broadcaster error: {e}")
            await asyncio.sleep(60)
    
    logger.info("Broadcaster stopped")


# ==================== FLASK ROUTES ====================

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Mentor Market Broadcaster running",
        "logged_in": SESSION_STRING != "",
        "broadcaster": "running" if broadcaster_running else "stopped",
        "assets": "Gold (70%) + Bitcoin (30%)",
        "timeframes": "Random (1m, 5m, 15m, 30m, 1h, 4h)",
        "format": "2 ticks with chart image",
        "interval_minutes": 25,
        "group_send_enabled": ENABLE_GROUP_SEND,
        "style": "Human mentor tone - educational, varied wording",
        "safe_test_saved": "/send_saved_test",
        "safe_preview": "/preview_analysis",
        "send_once_to_group": "/send_vantage_once",
        "start_auto": "/start_broadcaster",
        "stop_auto": "/stop_broadcaster"
    })


@app.route("/preview_analysis", methods=["GET"])
def preview_analysis():
    """Preview message without sending"""
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="preview"), loop)
    try:
        result = future.result(timeout=30)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/send_saved_test", methods=["GET"])
def send_saved_test():
    """Send to Tony's Saved Messages for testing"""
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="saved"), loop)
    try:
        result = future.result(timeout=90)
        status_code = 200 if result.get("ok") else 403
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/send_vantage_once", methods=["GET"])
def send_vantage_once():
    """Send once to Vantage group"""
    future = asyncio.run_coroutine_threadsafe(create_market_update(send_mode="vantage"), loop)
    try:
        result = future.result(timeout=90)
        status_code = 200 if result.get("ok") else 403
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/start_broadcaster", methods=["GET"])
def start_broadcaster():
    """Start auto-broadcast every 25 minutes"""
    global broadcaster_running
    
    with broadcaster_start_lock:
        if broadcaster_running:
            return jsonify({"status": "Already running"})
        
        if not ENABLE_GROUP_SEND:
            return jsonify({
                "status": "blocked",
                "reason": "ENABLE_GROUP_SEND is false"
            }), 403
        
        broadcaster_running = True
        asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)
    
    return jsonify({"status": "Broadcaster started"})


@app.route("/stop_broadcaster", methods=["GET"])
def stop_broadcaster():
    """Stop auto-broadcast"""
    global broadcaster_running
    broadcaster_running = False
    return jsonify({"status": "Broadcaster stopped"})


@app.route("/send_code", methods=["GET"])
def send_code():
    """Request Telegram login code"""
    global phone_code_hash, client
    
    async def _send():
        global phone_code_hash
        result = await client.send_code_request(PHONE)
        phone_code_hash = result.phone_code_hash
    
    try:
        future = asyncio.run_coroutine_threadsafe(_send(), loop)
        future.result(timeout=15)
        return jsonify({"status": "Code sent to phone", "next": "/verify?code=XXXXX"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/verify", methods=["GET"])
def verify():
    """Verify Telegram login code and get SESSION_STRING"""
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
    """Auto-start broadcaster if enabled"""
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
            logger.info("Broadcaster already running")
            return
        
        broadcaster_running = True
        asyncio.run_coroutine_threadsafe(broadcast_loop(), loop)
    
    logger.info("Broadcaster auto-started on boot")


threading.Thread(target=start_broadcaster_on_boot, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
