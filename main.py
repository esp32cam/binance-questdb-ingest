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

buffer = defaultdict(list)
last_flush = datetime.now(timezone.utc) + BANGKOK_TZ

async def handle_trade(data):
    global last_flush
    symbol = data['s']
    price = float(data['p'])
    qty = float(data['q'])
    is_buy = not data['m']

    now = datetime.now(timezone.utc) + BANGKOK_TZ
    ts_floor = now.replace(microsecond=0)

    buffer[symbol].append((price, qty, is_buy))

    if (now - last_flush).total_seconds() >= 1:
        await flush_to_questdb(ts_floor)
        last_flush = now
        buffer.clear()

async def flush_to_questdb(ts):
    try:
        with Sender.from_conf(f"tcp::addr={HOST}:{PORT};") as sender:
            for symbol, trades in buffer.items():
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
            print(f"✅ Flushed {len(buffer)} symbols @ {ts}")
    except Exception as e:
        print(f"❌ Error: {e}")

async def main():
    stream_names = [f"{s.lower()}@trade" for s in SYMBOLS]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"
    print(f"🚀 เชื่อมต่อ Binance {len(SYMBOLS)} symbols... Host={HOST}:{PORT}")
    async with websockets.connect(url) as ws:
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if 'data' in data:
                await handle_trade(data['data'])

if __name__ == "__main__":
    asyncio.run(main())
