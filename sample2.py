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
   # c = conn.cursor()
    uid = "demo_user"
    existing = conn.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone()
    if not existing:
        log.info("no exist")
        # Seed strategies
        for s in [
        ("s1",uid,"RSI Momentum","Buy oversold, sell overbought using RSI divergence",68.5,1240,1.87,12.3,'{"rsiPeriod":14,"oversold":30,"overbought":70,"stopLoss":2.5}'),
        ("s2",uid,"EMA Crossover","Dual EMA crossover with volume confirmation",61.2,876,1.54,18.7,'{"fastEMA":12,"slowEMA":26,"volumeMult":1.5,"stopLoss":3.0}'),
        ("s3",uid,"Bollinger Squeeze","Trade breakouts from BB compression zones",73.4,445,2.12,8.9,'{"period":20,"deviation":2,"sqzThreshold":0.1,"stopLoss":2.0}'),
        ("s4",uid,"MACD Divergence","Spot MACD divergences for high-probability reversals",65.8,632,1.73,15.1,'{"fastPeriod":12,"slowPeriod":26,"signalPeriod":9,"stopLoss":2.8}'),
        ]: 
            conn.execute("""INSERT OR IGNORE INTO strategies(id,user_id,name,description,win_rate,total_trades,profit_factor,max_drawdown,parameters)
                VALUES (?,?,?,?,?,?,?,?,?)""", s)
    conn.commit()
    conn.close()
    log.info("Database initialized at %s", db_path)
    print(f"sucess...")
    
get_db()
init_db()    