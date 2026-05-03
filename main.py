"""
NexusAI Trading Platform - Python Backend
FastAPI + WebSockets + SQLite + Binance API + AI Signals + Telegram
"""

import asyncio
from enum import Enum
import json
import sqlite3
import hashlib
import hmac
import os
import time
import random
import math
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from pathlib import Path

import aiohttp
import jwt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr

# ── Config ─────────────────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY", "nexusai-super-secret-key-change-in-production")
ALGORITHM = "HS256"
DB_PATH = Path(__file__).parent / "nexusai.db"
BINANCE_WS = "wss://stream.binance.com:9443/stream"
BINANCE_REST = "https://api.binance.com"
TELEGRAM_API = "https://api.telegram.org"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

PAIRS = [
    "BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT",
    "ADAUSDT","DOGEUSDT","AVAXUSDT","DOTUSDT","MATICUSDT","LTCUSDT","LINKUSDT"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nexusai")

import os
db_path = os.path.join(os.path.dirname(__file__), 'nexusai.db')

# ── Database ────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    print(f"db connected")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        telegram_token TEXT DEFAULT '',
        telegram_chat_id TEXT DEFAULT '',
        created_at INTEGER DEFAULT (strftime('%s','now'))
    );

    CREATE TABLE IF NOT EXISTS bots (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        pair TEXT NOT NULL,
        strategy TEXT NOT NULL,
        status TEXT DEFAULT 'STOPPED',
        profit REAL DEFAULT 0,
        trades INTEGER DEFAULT 0,
        win_rate REAL DEFAULT 0,
        capital REAL DEFAULT 1000,
        current_position TEXT DEFAULT NULL,
        started_at INTEGER DEFAULT (strftime('%s','now')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        bot_id TEXT DEFAULT NULL,
        pair TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL NOT NULL,
        amount REAL NOT NULL,
        total REAL NOT NULL,
        fee REAL NOT NULL,
        pnl REAL DEFAULT 0,
        status TEXT DEFAULT 'FILLED',
        created_at INTEGER DEFAULT (strftime('%s','now')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS signals (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        pair TEXT NOT NULL,
        type TEXT NOT NULL,
        strength REAL DEFAULT 0,
        price REAL NOT NULL,
        target_price REAL NOT NULL,
        stop_loss REAL NOT NULL,
        confidence REAL DEFAULT 0,
        reason TEXT DEFAULT '',
        ai_generated INTEGER DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        created_at INTEGER DEFAULT (strftime('%s','now')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS strategies (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT DEFAULT '',
        win_rate REAL DEFAULT 0,
        total_trades INTEGER DEFAULT 0,
        profit_factor REAL DEFAULT 0,
        max_drawdown REAL DEFAULT 0,
        parameters TEXT DEFAULT '{}',
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)
    conn.commit()
    conn.close()
    log.info("Database initialized at %s", db_path)

# ── Auth ────────────────────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(plain), hashed)

def create_token(user_id: str) -> str:
    exp = datetime.utcnow() + timedelta(days=30)
    return jwt.encode({"sub": user_id, "exp": exp}, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except Exception:
        return None

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    user_id = decode_token(credentials.credentials)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return dict(user)

# ── Pydantic Models ─────────────────────────────────────────────────────────────
class SignupRequest(BaseModel):
    name: str
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

class BotCreate(BaseModel):
    name: str
    pair: str
    strategy: str
    capital: float = 1000.0

class BotUpdate(BaseModel):
    status: Optional[str] = None
    capital: Optional[float] = None

class TradeCreate(BaseModel):
    pair: str
    side: str
    amount: float
    order_type: str = "market"
    limit_price: Optional[float] = None

class StrategyUpdate(BaseModel):
    parameters: Dict[str, float]

class TelegramSettings(BaseModel):
    telegram_token: str
    telegram_chat_id: str


# ── Technical Analysis ──────────────────────────────────────────────────────────
def calc_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(len(closes) - period, len(closes)):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    rs = gains / (losses or 0.001)
    return 100 - 100 / (1 + rs)

def calc_ema(data: List[float], period: int) -> List[float]:
    k = 2 / (period + 1)
    ema = [data[0]]
    for v in data[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema

def calc_macd(closes: List[float]):
    if len(closes) < 26:
        return {"macd": 0, "signal": 0, "hist": 0}
    e12 = calc_ema(closes, 12)
    e26 = calc_ema(closes, 26)
    ml = [a - b for a, b in zip(e12, e26)]
    sl = calc_ema(ml, 9)
    return {"macd": ml[-1], "signal": sl[-1], "hist": ml[-1] - sl[-1]}

def calc_bb(closes: List[float], period: int = 20):
    sl = closes[-period:]
    mean = sum(sl) / len(sl)
    std = math.sqrt(sum((x - mean) ** 2 for x in sl) / len(sl))
    return {"upper": mean + 2 * std, "middle": mean, "lower": mean - 2 * std}

def generate_signal_from_ta(candles: List[dict], ticker: dict) -> dict:
    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    rsi = calc_rsi(closes)
    macd = calc_macd(closes)
    bb = calc_bb(closes)
    price = ticker["price"]
    ema50 = calc_ema(closes, 50)
    ema200 = calc_ema(closes, 200)
    score = 0
    reasons = []
 
    if rsi < 30:
        score += 2; reasons.append("RSI oversold")
    elif rsi > 70:
        score -= 2; reasons.append("RSI overbought")
    elif rsi < 45:
        score += 1; reasons.append("RSI bullish momentum")
    elif rsi > 55:
        score -= 1
        reasons.append("RSI bearish pressure")    
    
    if macd["macd"] > macd["signal"] and  macd["hist"] > 0:
        score += 2; reasons.append("MACD bullish crossover")
    elif macd["macd"] < macd["signal"]:
        score -= 2; reasons.append("MACD bearish crossover")

    if price < bb["lower"]:
        score += 2; reasons.append("Price Below lower Bollinger band")
    elif price > bb["upper"]:
        score -= 2; reasons.append("Price Above lower Bollinger band")
    
    if ema50 > ema200:
        score += 1
        reasons.append("Uptrend confirmed (EMA50 > EMA200)")
    else:
        score -= 1
        reasons.append("Downtrend (EMA50 < EMA200)")    
    
    avg_vol = sum(volumes[-20:]) / 20

    if volumes[-1] > avg_vol * 1.5:
        score += 1
        reasons.append("Volume breakout")
    
    change_pct = ticker.get("changePct", 0)

    if change_pct > 3:
        score += 1
        reasons.append("Strong bullish momentum")

    elif change_pct < -3:
        score -= 1
        reasons.append("Strong bearish momentum")
    
    if score >= 3:
        sig_type = "BUY"

    elif score <= -3:
        sig_type = "SELL"

    else:
        sig_type = "HOLD"
    
    #confidence = min(96, 50 + abs(score) * 9 + random.random() * 8)
    confidence = min(95, 50 + abs(score) * 10)

    if sig_type == "BUY":
        target = price * 1.035
        stop = price * 0.975

    elif sig_type == "SELL":
        target = price * 0.965
        stop = price * 1.025

    else:
        target = price
        stop = price
        
    return {
        "type": sig_type,
        "strength": abs(score),
        "confidence": confidence,
        "targetPrice": target,
        "stopLoss": stop,
        "reason": " | ".join(reasons),
        "rsi": rsi,
        "macd": macd,
        "bb": bb,
    }
    
# ── Market Data ─────────────────────────────────────────────────────────────────
BASE_PRICES = {
    "BTCUSDT": 67420, "ETHUSDT": 3521, "BNBUSDT": 412, "SOLUSDT": 178,
    "XRPUSDT": 0.5923, "ADAUSDT": 0.4512, "DOGEUSDT": 0.1234, "AVAXUSDT": 38.45,
    "DOTUSDT": 8.21, "MATICUSDT": 0.7834, "LTCUSDT": 84.12, "LINKUSDT": 14.87,
}
live_prices: Dict[str, float] = dict(BASE_PRICES)
live_tickers: Dict[str, dict] = {}

async def fetch_binance_tickers(session: aiohttp.ClientSession) -> List[dict]:
    """Fetch real ticker data from Binance REST API."""
    try:
        symbols = json.dumps(PAIRS)
        url = f"{BINANCE_REST}/api/v3/ticker/24hr?symbols={symbols}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200:
                data = await r.json()
                tickers = []
                for d in data:
                    p = float(d["lastPrice"])
                    live_prices[d["symbol"]] = p
                    t = {
                        "symbol": d["symbol"],
                        "price": p,
                        "change24h": float(d["priceChange"]),
                        "changePct": float(d["priceChangePercent"]),
                        "high24h": float(d["highPrice"]),
                        "low24h": float(d["lowPrice"]),
                        "volume24h": float(d["volume"]),
                        "bid": float(d["bidPrice"]),
                        "ask": float(d["askPrice"]),
                        "lastUpdate": int(time.time() * 1000),
                    }
                    tickers.append(t)
                    live_tickers[d["symbol"]] = t
                    print(f"tickers"+len(tickers))
                log.debug("Fetched %d live tickers from Binance", len(tickers))
                print(f"demo.....tickers"+len(tickers))
                return tickers
    except Exception as e:
        log.warning("Binance API error: %s — using simulation", e)
    tickers = []
    #return tickers
    return generate_sim_tickers()

def generate_sim_tickers() -> List[dict]:
    """Generate realistic simulated tickers when API is unavailable."""
    tickers = []
    for sym in PAIRS:
        base = live_prices.get(sym, BASE_PRICES.get(sym, 1.0))
        noise = (random.random() - 0.5) * 0.003
        price = base * (1 + noise)
        live_prices[sym] = price
        base_orig = BASE_PRICES.get(sym, 1.0)
        pct = ((price - base_orig) / base_orig) * 100
        t = {
            "symbol": sym, "price": price,
            "change24h": price - base_orig, "changePct": pct,
            "high24h": base_orig * 1.032, "low24h": base_orig * 0.971,
            "volume24h": base_orig * 9000 * (1 + random.random()),
            "bid": price * 0.9995, "ask": price * 1.0005,
            "lastUpdate": int(time.time() * 1000),
        }
        tickers.append(t)
        live_tickers[sym] = t
    return tickers

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str = "1h", limit: int = 100) -> List[dict]:
    """Fetch OHLCV candles from Binance."""
    try:
        url = f"{BINANCE_REST}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200:
                data = await r.json()
                return [{"time": k[0], "open": float(k[1]), "high": float(k[2]),
                         "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])} for k in data]
    except Exception:
        pass
    return gen_sim_klines(live_prices.get(symbol, BASE_PRICES.get(symbol, 100)), limit)

def gen_sim_klines(base_price: float, count: int) -> List[dict]:
    candles = []
    price = base_price * (0.95 + random.random() * 0.1)
    now = int(time.time() * 1000)
    for i in range(count, -1, -1):
        open_p = price
        change = (random.random() - 0.5) * price * 0.022
        close_p = max(price * 0.001, price + change)
        wick = random.random() * price * 0.008
        candles.append({
            "time": now - i * 3600000,
            "open": open_p, "high": max(open_p, close_p) + wick,
            "low": min(open_p, close_p) - wick * 0.5,
            "close": close_p, "volume": random.random() * 1e6,
        })
        price = close_p
    return candles

# ── AI Analysis ─────────────────────────────────────────────────────────────────
async def get_ai_signal(session: aiohttp.ClientSession, symbol: str, ticker: dict, rsi: float, macd: dict) -> Optional[dict]:
    """Call Claude API for AI-powered signal generation."""
    if not ANTHROPIC_KEY:
        return None
    try:
        prompt = f"""Analyze {symbol} for a trading signal. Data:
Price: ${ticker['price']:.4f}, RSI(14): {rsi:.1f}, MACD_hist: {macd['hist']:.6f}
24h change: {ticker['changePct']:.2f}%, High: ${ticker['high24h']:.4f}, Low: ${ticker['low24h']:.4f}

Respond ONLY as valid JSON (no markdown, no extra text):
{{"type":"BUY|SELL|HOLD","confidence":85,"targetPrice":{ticker['price']*1.03:.4f},"stopLoss":{ticker['price']*0.97:.4f},"reason":"Brief analysis here"}}"""

        async with session.post(ANTHROPIC_API,
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"},
            json={"model": "claude-sonnet-4-20250514", "max_tokens": 200, "messages": [{"role": "user", "content": prompt}]},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status == 200:
                data = await r.json()
                text = data["content"][0]["text"].strip()
                text = text.replace("```json", "").replace("```", "").strip()
                return json.loads(text)
    except Exception as e:
        log.warning("AI signal error: %s", e)
    return None

# ── Telegram ────────────────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, token: str, chat_id: str, message: str) -> bool:
    try:
        url = f"{TELEGRAM_API}/bot{token}/sendMessage"
        async with session.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            return r.status == 200
    except Exception as e:
        log.warning("Telegram error: %s", e)
        return False

# ── WebSocket Manager ───────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.connections: Dict[str, List[WebSocket]] = {}  # user_id -> [ws]

    async def connect(self, ws: WebSocket, user_id: str):
        await ws.accept()
        self.connections.setdefault(user_id, []).append(ws)
        log.info("WS connected: user=%s total=%d", user_id, len(self.connections[user_id]))

    def disconnect(self, ws: WebSocket, user_id: str):
        if user_id in self.connections:
            self.connections[user_id] = [c for c in self.connections[user_id] if c != ws]
            if not self.connections[user_id]:
                del self.connections[user_id]
        log.info("WS disconnected: user=%s", user_id)

    async def send_to_user(self, user_id: str, data: dict):
        for ws in self.connections.get(user_id, []):
            try:
                await ws.send_json(data)
            except Exception:
                pass

    async def broadcast(self, data: dict):
        for user_id in list(self.connections.keys()):
            await self.send_to_user(user_id, data)

    @property
    def connected_users(self):
        return list(self.connections.keys())

manager = ConnectionManager()

# ── Background Tasks ────────────────────────────────────────────────────────────
async def market_data_loop():
    """Continuously fetch market data and broadcast to connected clients."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                tickers = await fetch_binance_tickers(session)
                if manager.connected_users:
                    await manager.broadcast({"type": "TICKERS", "data": tickers})
            except Exception as e:
                log.error("Market loop error: %s", e)
            await asyncio.sleep(5)

async def bot_simulation_loop():
    """Simulate bot trading and update P&L."""
    while True:
        await asyncio.sleep(8)
        try:
            conn = get_db()
            bots = conn.execute("SELECT * FROM bots WHERE status='RUNNING'").fetchall()
            for bot in bots:
                bot = dict(bot)
                ticker = live_tickers.get(bot["pair"])
                if not ticker:
                    continue
               # price=ticker["price"]
                delta = (random.random() - 0.47) * bot["capital"] * 0.0015
                new_profit = bot["profit"] + delta * 0.08
                new_trades = bot["trades"] + (1 if random.random() < 0.1 else 0)
                new_wr = min(85, max(42, bot["win_rate"] + (random.random() - 0.5) * 0.08))
                conn.execute("UPDATE bots SET profit=?, trades=?, win_rate=? WHERE id=?",
                             (new_profit, new_trades, new_wr, bot["id"]))
                if random.random() < 0.12:
                    trade_id = f"t_{int(time.time()*1000)}_{bot['id'][-4:]}"
                    side = "BUY" if random.random() > 0.5 else "SELL"
                    price = ticker["price"]
                    amount = round(random.uniform(0.001, 0.01), 4)
                    total = price * amount
                    conn.execute("""INSERT INTO trades (id,user_id,bot_id,pair,side,price,amount,total,fee,pnl,status)
                        VALUES (?,?,?,?,?,?,?,?,?,?,'FILLED')""",
                        (trade_id, bot["user_id"], bot["id"], bot["pair"], side,
                         price, amount, total, total * 0.001, delta))
                    # Broadcast new trade
                    await manager.send_to_user(bot["user_id"], {
                        "type": "NEW_TRADE",
                        "data": {"id": trade_id, "pair": bot["pair"], "side": side,
                                 "price": price, "amount": amount, "total": total,
                                 "fee": total*0.001, "pnl": delta, "botId": bot["id"],
                                 "timestamp": int(time.time()*1000), "status": "FILLED"},
                    })
            conn.commit()
            # Broadcast updated bots to each user
            users_with_bots = set(b["user_id"] for b in bots)
            for uid in users_with_bots:
                updated = conn.execute("SELECT * FROM bots WHERE user_id=?", (uid,)).fetchall()
                await manager.send_to_user(uid, {
                    "type": "BOTS_UPDATE",
                    "data": [dict(b) for b in updated],
                })
            conn.close()
        except Exception as e:
            log.error("Bot loop error: %s", e)

# ── Startup / Shutdown ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    #seed_demo_data()
    t1 = asyncio.create_task(market_data_loop())
    t2 = asyncio.create_task(bot_simulation_loop())
    log.info("NexusAI backend started")
    yield
    t1.cancel(); t2.cancel()
    log.info("NexusAI backend stopped")

def seed_demo_data():
    conn = get_db()
    # Demo user
    uid = "demo_user"
    existing = conn.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone()
    if not existing:
        conn.execute("""INSERT INTO users (id,name,email,password_hash) VALUES (?,?,?,?)""",
            (uid, "Demo Trader", "demo@nexusai.trade", hash_password("demo123")))
        # Seed bots
        for b in [
            ("bot1",uid,"Scalper Alpha","BTCUSDT","RSI Scalper","RUNNING",847.23,142,68.3,5000,"LONG"),
            ("bot2",uid,"Trend Follower","ETHUSDT","EMA Cross","RUNNING",312.45,67,72.1,3000,None),
            ("bot3",uid,"Grid Bot SOL","SOLUSDT","Grid Trading","PAUSED",-45.12,23,55.2,2000,None),
        ]:
            conn.execute("""INSERT OR IGNORE INTO bots
                (id,user_id,name,pair,strategy,status,profit,trades,win_rate,capital,current_position)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""", b)
        # Seed strategies
        for s in [
            ("s1",uid,"RSI Momentum","Buy oversold, sell overbought using RSI divergence",68.5,1240,1.87,12.3,'{"rsiPeriod":14,"oversold":30,"overbought":70,"stopLoss":2.5}'),
            ("s2",uid,"EMA Crossover","Dual EMA crossover with volume confirmation",61.2,876,1.54,18.7,'{"fastEMA":12,"slowEMA":26,"volumeMult":1.5,"stopLoss":3.0}'),
            ("s3",uid,"Bollinger Squeeze","Trade breakouts from BB compression zones",73.4,445,2.12,8.9,'{"period":20,"deviation":2,"sqzThreshold":0.1,"stopLoss":2.0}'),
            ("s4",uid,"MACD Divergence","Spot MACD divergences for high-probability reversals",65.8,632,1.73,15.1,'{"fastPeriod":12,"slowPeriod":26,"signalPeriod":9,"stopLoss":2.8}'),
        ]:
            conn.execute("""INSERT OR IGNORE INTO strategies
                (id,user_id,name,description,win_rate,total_trades,profit_factor,max_drawdown,parameters)
                VALUES (?,?,?,?,?,?,?,?,?)""", s)
        conn.commit()
        log.info("Demo data seeded")
    conn.close()

# ── App ─────────────────────────────────────────────────────────────────────────
app = FastAPI(title="NexusAI Trading API", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# ─── Auth Routes ─────────────────────────────────────────────────────────────
@app.post("/auth/signup")
async def signup(req: SignupRequest):
    conn = get_db()
    if conn.execute("SELECT id FROM users WHERE email=?", (req.email,)).fetchone():
        conn.close()
        raise HTTPException(400, "Email already registered")
    uid = f"u_{int(time.time()*1000)}"
    conn.execute("INSERT INTO users (id,name,email,password_hash) VALUES (?,?,?,?)",
                 (uid, req.name, req.email, hash_password(req.password)))
    conn.commit(); conn.close()
    return {"token": create_token(uid), "user": {"id": uid, "name": req.name, "email": req.email}}

@app.post("/auth/login")
async def login(req: LoginRequest):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email,)).fetchone()
    conn.close()
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(401, "Invalid credentials")
    u = dict(user)
    return {"token": create_token(u["id"]), "user": {k: u[k] for k in ("id","name","email","telegram_token","telegram_chat_id")}}

@app.get("/auth/me")
async def me(user=Depends(get_current_user)):
    return {k: user[k] for k in ("id","name","email","telegram_token","telegram_chat_id")}

# ─── Tickers ──────────────────────────────────────────────────────────────────
@app.get("/market/tickers")
async def get_tickers():
    async with aiohttp.ClientSession() as s:
        return await fetch_binance_tickers(s)

@app.get("/market/klines/{symbol}")
async def get_klines(symbol: str, interval: str = "1h", limit: int = 1000):
    async with aiohttp.ClientSession() as s:
        return await fetch_klines(s, symbol, interval, limit)

# ─── Signals ──────────────────────────────────────────────────────────────────
@app.get("/signals")
async def list_signals(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM signals WHERE user_id=? ORDER BY created_at DESC LIMIT 100", (user["id"],)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/signals/generate")
async def generate_signals(user=Depends(get_current_user)):
    results = []
    async with aiohttp.ClientSession() as session:
        tickers = await fetch_binance_tickers(session)
        conn = get_db()
        for ticker in tickers[:8]:
            candles = await fetch_klines(session, ticker["symbol"], "1h", 60)
            ta = generate_signal_from_ta(candles, ticker)
            sid = f"sig_{int(time.time()*1000)}_{ticker['symbol']}"
            conn.execute("""INSERT INTO signals
                (id,user_id,pair,type,strength,price,target_price,stop_loss,confidence,reason,ai_generated,status)
                VALUES (?,?,?,?,?,?,?,?,?,?,0,'ACTIVE')""",
                (sid, user["id"], ticker["symbol"], ta["type"], ta["strength"],
                 ticker["price"], ta["targetPrice"], ta["stopLoss"], ta["confidence"], ta["reason"]))
            results.append({"id": sid, "pair": ticker["symbol"], **ta,
                            "price": ticker["price"], "aiGenerated": False,
                            "timestamp": int(time.time()*1000), "status": "ACTIVE"})

        # AI signals for top 2
        for ticker in tickers[:2]:
            candles = await fetch_klines(session, ticker["symbol"], "1h", 60)
            closes = [c["close"] for c in candles]
            rsi = calc_rsi(closes)
            macd = calc_macd(closes)
            ai = await get_ai_signal(session, ticker["symbol"], ticker, rsi, macd)
            if ai:
                sid = f"aisig_{int(time.time()*1000)}_{ticker['symbol']}"
                conn.execute("""INSERT INTO signals
                    (id,user_id,pair,type,strength,price,target_price,stop_loss,confidence,reason,ai_generated,status)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1,'ACTIVE')""",
                    (sid, user["id"], ticker["symbol"], ai["type"], 5, ticker["price"],
                     float(ai.get("targetPrice", ticker["price"]*1.03)),
                     float(ai.get("stopLoss", ticker["price"]*0.97)),
                     float(ai.get("confidence", 75)), ai.get("reason","AI analysis"), ))
                results.insert(0, {"id": sid, "pair": ticker["symbol"],
                                   "type": ai["type"], "confidence": float(ai.get("confidence",75)),
                                   "targetPrice": float(ai.get("targetPrice", ticker["price"]*1.03)),
                                   "stopLoss": float(ai.get("stopLoss", ticker["price"]*0.97)),
                                   "reason": ai.get("reason",""), "price": ticker["price"],
                                   "strength": 5, "aiGenerated": True,
                                   "timestamp": int(time.time()*1000), "status": "ACTIVE"})
        conn.commit(); conn.close()
        # Broadcast to user's sockets
        await manager.send_to_user(user["id"], {"type": "NEW_SIGNALS", "data": results})
    return results

@app.post("/signals/copy")
async def copy_signal(signal: dict, user=Depends(get_current_user)):
    conn = get_db()
    data = conn.execute(
        "SELECT * FROM signals WHERE id=?",
        (signal)
    ).fetchone()
    print(f"copying"+signal)
    total = data["price"] * 3
    trade_id = f"ct_{int(time.time()*1000)}"
    conn.execute("""INSERT INTO trades (id,user_id,pair,side,price,amount,total,fee,pnl,status)
        VALUES (?,?,?,?,?,?,?,?,0,'FILLED')""",
        (trade_id, user["id"], data["pair"], data["type"], data["price"], 3, total, total*0.001,0, total * 0.001))
    conn.commit(); conn.close()
    trade = {"id": trade_id, "pair": data["pair"], "side": data["type"], "price": data["price"],
             "amount": 3, "total": total, "fee": total*0.001,
             "pnl": 0, "status": "FILLED", "timestamp": int(time.time()*1000)}
    await manager.send_to_user(user["id"], {"type": "NEW_TRADE", "data": trade})
    return trade

@app.post("/signals/{signal_id}/telegram")
async def send_signal_to_telegram(signal_id: str, user=Depends(get_current_user)):
    if not user["telegram_token"] or not user["telegram_chat_id"]:
        raise HTTPException(400, "Telegram not configured. Set in Settings.")
    conn = get_db()
    sig = conn.execute("SELECT * FROM signals WHERE id=? AND user_id=?", (signal_id, user["id"])).fetchone()
    conn.close()
    if not sig:
        raise HTTPException(404, "Signal not found")
    s = dict(sig)
    emoji = "🟢" if s["type"] == "BUY" else "🔴" if s["type"] == "SELL" else "🟡"
    msg = (f"{emoji} <b>{s['type']} Signal — {s['pair']}</b>\n\n"
           f"💰 Entry: <code>${s['price']:.4f}</code>\n"
           f"🎯 Target: <code>${s['target_price']:.4f}</code>\n"
           f"🛑 Stop Loss: <code>${s['stop_loss']:.4f}</code>\n"
           f"📊 Confidence: <b>{s['confidence']:.0f}%</b>\n\n"
           f"📝 {s['reason']}\n\n"
           f"{'🤖 <i>AI-powered signal</i>' if s['ai_generated'] else ''}\n"
           f"⚡ NexusAI Trading Platform")
    async with aiohttp.ClientSession() as session:
        ok = await send_telegram(session, user["telegram_token"], user["telegram_chat_id"], msg)
    if not ok:
        raise HTTPException(502, "Failed to send Telegram message")
    return {"ok": True}

# ─── Bots ──────────────────────────────────────────────────────────────────────
@app.get("/bots")
async def list_bots(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM bots WHERE user_id=? ORDER BY started_at DESC", (user["id"],)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/bots")
async def create_bot(req: BotCreate, user=Depends(get_current_user)):
    bid = f"bot_{int(time.time()*1000)}"
    conn = get_db()
    conn.execute("""INSERT INTO bots (id,user_id,name,pair,strategy,capital,status,win_rate)
        VALUES (?,?,?,?,?,?,'RUNNING',0)""",
        (bid, user["id"], req.name, req.pair, req.strategy, req.capital))
    conn.commit()
    bot = dict(conn.execute("SELECT * FROM bots WHERE id=?", (bid,)).fetchone())
    conn.close()
    return bot

@app.patch("/bots/{bot_id}")
async def update_bot(bot_id: str, req: BotUpdate, user=Depends(get_current_user)):
    conn = get_db()
    if req.status:
        conn.execute("UPDATE bots SET status=? WHERE id=? AND user_id=?", (req.status, bot_id, user["id"]))
    conn.commit()
    bot = conn.execute("SELECT * FROM bots WHERE id=?", (bot_id,)).fetchone()
    conn.close()
    return dict(bot)

@app.delete("/bots/{bot_id}")
async def delete_bot(bot_id: str, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("DELETE FROM bots WHERE id=? AND user_id=?", (bot_id, user["id"]))
    conn.commit(); conn.close()
    return {"ok": True}

# ─── Trades ───────────────────────────────────────────────────────────────────
@app.get("/trades")
async def list_trades(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM trades WHERE user_id=? ORDER BY created_at DESC LIMIT 500", (user["id"],)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/trades")
async def place_trade(req: TradeCreate, user=Depends(get_current_user)):
    ticker = live_tickers.get(req.pair)
    price = req.limit_price if req.order_type == "limit" and req.limit_price else (ticker["price"] if ticker else 100)
    tid = f"t_{int(time.time()*1000)}"
    total = price * req.amount
    conn = get_db()
    conn.execute("""INSERT INTO trades (id,user_id,pair,side,price,amount,total,fee,pnl,status)
        VALUES (?,?,?,?,?,?,?,?,0,'FILLED')""",
        (tid, user["id"], req.pair, req.side, price, req.amount, total, total * 0.001))
    conn.commit(); conn.close()
    trade = {"id": tid, "pair": req.pair, "side": req.side, "price": price,
             "amount": req.amount, "total": total, "fee": total*0.001,
             "pnl": 0, "status": "FILLED", "timestamp": int(time.time()*1000)}
    await manager.send_to_user(user["id"], {"type": "NEW_TRADE", "data": trade})
    return trade

# ─── Strategies ───────────────────────────────────────────────────────────────
@app.get("/strategies")
async def list_strategies(user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM strategies").fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.patch("/strategies/{strategy_id}")
async def update_strategy(strategy_id: str, req: StrategyUpdate, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE strategies SET parameters=? WHERE id=? AND user_id=?",
                 (json.dumps(req.parameters), strategy_id, user["id"]))
    conn.commit()
    s = dict(conn.execute("SELECT * FROM strategies WHERE id=?", (strategy_id,)).fetchone())
    conn.close()
    return s

@app.post("/strategies/{strategy_id}/backtest")
async def run_backtest(strategy_id: str, user=Depends(get_current_user)):
    """Simulated backtest — in production connect to real backtesting engine."""
    await asyncio.sleep(1.5)  # Simulate processing
    return {
        "profit": (random.random() - 0.3) * 2200 + 400,
        "trades": random.randint(50, 250),
        "winRate": 45 + random.random() * 35,
        "sharpeRatio": 0.8 + random.random() * 1.5,
        "maxDrawdown": 5 + random.random() * 20,
    }

# ─── Settings ─────────────────────────────────────────────────────────────────
@app.patch("/settings/telegram")
async def update_telegram(req: TelegramSettings, user=Depends(get_current_user)):
    conn = get_db()
    conn.execute("UPDATE users SET telegram_token=?, telegram_chat_id=? WHERE id=?",
                 (req.telegram_token, req.telegram_chat_id, user["id"]))
    conn.commit(); conn.close()
    return {"ok": True}

@app.post("/settings/telegram/test")
async def test_telegram(user=Depends(get_current_user)):
    if not user["telegram_token"] or not user["telegram_chat_id"]:
        raise HTTPException(400, "Telegram not configured")
    async with aiohttp.ClientSession() as session:
        ok = await send_telegram(session, user["telegram_token"], user["telegram_chat_id"],
            "🤖 <b>NexusAI Connected!</b>\n\nYour trading signals will appear here.\n\n⚡ NexusAI Trading Platform")
    if not ok:
        raise HTTPException(502, "Telegram connection failed")
    return {"ok": True}

# ─── WebSocket ────────────────────────────────────────────────────────────────
@app.websocket("/ws/{token}")
async def websocket_endpoint(websocket: WebSocket, token: str):
    user_id = decode_token(token)
    if not user_id:
        await websocket.close(code=4001)
        return

    conn = get_db()
    user = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    if not user:
        await websocket.close(code=4001)
        return

    await manager.connect(websocket, user_id)
    try:
        # Send initial state
        conn = get_db()
        bots = [dict(r) for r in conn.execute("SELECT * FROM bots WHERE user_id=?", (user_id,)).fetchall()]
        trades = [dict(r) for r in conn.execute("SELECT * FROM trades WHERE user_id=? ORDER BY created_at DESC LIMIT 100", (user_id,)).fetchall()]
        signals = [dict(r) for r in conn.execute("SELECT * FROM signals WHERE user_id=? ORDER BY created_at DESC LIMIT 50", (user_id,)).fetchall()]
        conn.close()
        await websocket.send_json({"type": "INIT", "data": {"bots": bots, "trades": trades, "signals": signals}})

        while True:
            try:
                msg = await asyncio.wait_for(websocket.receive_json(), timeout=30)
                # Handle client messages
                if msg.get("type") == "PING":
                    await websocket.send_json({"type": "PONG"})
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "PING"})
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
    except Exception as e:
        log.error("WS error: %s", e)
        manager.disconnect(websocket, user_id)

@app.get("/health")
async def health():
    return {"status": "ok", "connected": len(manager.connections), "version": "2.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
