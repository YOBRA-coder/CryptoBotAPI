import sqlite3
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nexusai")
db_path = os.path.join(os.path.dirname(__file__), 'nexusai.db')

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
    print(f"sucess...")
    
get_db()
init_db()