import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import os
import websockets
from questdb.ingress import Sender

# ================== ตั้งค่า ==================
HOST = os.getenv("QUESTDB_HOST", "metro.proxy.rlwy.net")
PORT = os.getenv("QUESTDB_PORT", "25708")
BANGKOK_TZ = timedelta(hours=7)  # Bangkok is UTC+7
# ===========================================

TICKERS = ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOT', 'AVAX', 'LINK', 'LTC', 'BCH', 'TRX', 'MATIC', 'SUI', 'APT', 'NEAR', 'TIA', 'ATOM', 'ALGO', 'STX', 'EGLD', 'KAS', 'USDT', 'USDC', 'FDUSD', 'DAI', 'FRAX', 'RLUSD', 'DOGE', 'SHIB', 'PEPE', 'FLOKI', 'BONK', 'WIF', 'MEW', 'NEIRO', 'BRETT', 'FARTCOIN', 'POPCAT', 'MOODENG', 'GOAT', 'FET', 'TAO', 'RENDER', 'HYPE', 'AKT', 'AR', 'FIL', 'GRT', 'ICP', 'THETA', 'LPT', 'JASMY', 'IO', 'NOS', 'PLANCK', 'AAVE', 'UNI', 'SUSHI', 'CRV', 'MKR', 'DYDX', 'SNX', '1INCH', 'PYTH', 'API3', 'JUP', 'ENA', 'PENDLE', 'RAY', 'BREV', 'ZENT', 'ATH', 'CGPT', 'COOKIE', 'ZKC', 'KAITO', 'MORPHO', 'SOMI', 'TURTLE', 'SENT', 'HYPER', 'ZAMA', 'GALA', 'AXS', 'SAND', 'MANA', 'ILV', 'IMX', 'BEAM']

SYMBOLS = [f"{t}USDT" for t in TICKERS if t not in ['USDT','USDC','DAI','FDUSD','FRAX','RLUSD']]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# Buffer สำหรับ trade data (agg 1s)
trade_buffer = defaultdict(list)
trade_last_flush = datetime.now(timezone.utc) + BANGKOK_TZ

# Buffer สำหรับ L1 (snapshot ล่าสุด)
l1_buffer = defaultdict(dict)
l1_last_flush = datetime.now(timezone.utc) + BANGKOK_TZ

# ================== ดึง Trade Data (เดิม) ==================
async def handle_trade(data):
    global trade_last_flush
    symbol = data['s']
    price = float(data['p'])
    qty = float(data['q'])
    is_buy = not data['m']

    now = datetime.now(timezone.utc) + BANGKOK_TZ
    ts_floor = now.replace(microsecond=0)

    trade_buffer[symbol].append((price, qty, is_buy))

    if (now - trade_last_flush).total_seconds() >= 1:
        await flush_trade(ts_floor)
        trade_last_flush = now
        trade_buffer.clear()

async def flush_trade(ts):
    try:
        with Sender.from_conf(f"tcp::addr={HOST}:{PORT};") as sender:
            for symbol, trades in trade_buffer.items():
                if not trades: continue
                buy_vol = sum(q for _, q, b in trades if b)
                sell_vol = sum(q for _, q, b in trades if not b)
                total_vol = buy_vol + sell_vol
                delta = buy_vol - sell_vol
                vwap = sum(p * q for p, q, _ in trades) / total_vol if total_vol > 0 else 0.0
                trade_count = len(trades)

                sender.row(
                    'trades_agg_1s',
                    symbols={'symbol': symbol},
                    columns={'buy_vol': buy_vol, 'sell_vol': sell_vol, 'delta': delta, 'vwap': vwap, 'trade_count': trade_count},
                    at=ts
                )
            sender.flush()
            print(f"✅ Trade Flushed {len(trade_buffer)} symbols @ {ts}")
    except Exception as e:
        print(f"❌ Trade Flush error: {e}")

# ================== ดึง L1 (Book Ticker) ==================
async def handle_l1(data):
    global l1_last_flush
    symbol = data['s']
    bid_price = float(data['b'])
    bid_qty = float(data['B'])
    ask_price = float(data['a'])
    ask_qty = float(data['A'])

    now = datetime.now(timezone.utc) + BANGKOK_TZ
    ts_floor = now.replace(microsecond=0)

    l1_buffer[symbol] = {
        'bid_price': bid_price,
        'bid_qty': bid_qty,
        'ask_price': ask_price,
        'ask_qty': ask_qty
    }

    if (now - l1_last_flush).total_seconds() >= 1:
        await flush_l1(ts_floor)
        l1_last_flush = now
        l1_buffer.clear()

async def flush_l1(ts):
    try:
        with Sender.from_conf(f"tcp::addr={HOST}:{PORT};") as sender:
            for symbol, l1 in l1_buffer.items():
                if not l1: continue

                sender.row(
                    'book_l1',
                    symbols={'symbol': symbol},
                    columns={
                        'bid_price': l1['bid_price'],
                        'bid_qty': l1['bid_qty'],
                        'ask_price': l1['ask_price'],
                        'ask_qty': l1['ask_qty']
                    },
                    at=ts
                )
            sender.flush()
            print(f"✅ L1 Flushed {len(l1_buffer)} symbols @ {ts}")
    except Exception as e:
        print(f"❌ L1 Flush error: {e}")

# ================== Main: รันทั้ง 2 พร้อมกัน ==================
async def main():
    trade_stream_names = [f"{s.lower()}@trade" for s in SYMBOLS]
    l1_stream_names = [f"{s.lower()}@bookTicker" for s in SYMBOLS]

    trade_url = f"wss://fstream.binance.com/stream?streams={'/'.join(trade_stream_names)}"
    l1_url = f"wss://fstream.binance.com/stream?streams={'/'.join(l1_stream_names)}"

    print(f"🚀 เริ่มดึง Trade + L1 สำหรับ {len(SYMBOLS)} symbols")
    print(f"QuestDB: {HOST}:{PORT}")

    # รันทั้ง 2 task พร้อมกัน
    await asyncio.gather(
        ingest_stream(trade_url, handle_trade, "Trade"),
        ingest_stream(l1_url, handle_l1, "L1")
    )

async def ingest_stream(url, handler, name):
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print(f"{name} WebSocket connected")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' in data:
                        await handler(data['data'])
        except Exception as e:
            print(f"{name} WebSocket error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
