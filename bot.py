import requests
import pandas as pd
import time
import os
from datetime import datetime

BASE_URL = "https://api.india.delta.exchange"

TIMEFRAME = "5m"
MAX_SYMBOLS = 60

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

session = requests.Session()

# =========================
# GET SYMBOLS
# =========================
def get_symbols():
    res = session.get(f"{BASE_URL}/v2/products").json()

    symbols = [
        p["symbol"] for p in res["result"]
        if p["contract_type"] == "perpetual_futures"
    ]

    print(f"✅ Total perpetual symbols: {len(symbols)}")
    return symbols[:MAX_SYMBOLS]


# =========================
# GET CANDLES
# =========================
def get_close_prices(symbol):
    try:
        end = int(time.time())
        start = end - (24 * 60 * 60)

        res = session.get(
            f"{BASE_URL}/v2/history/candles",
            params={
                "resolution": TIMEFRAME,
                "symbol": symbol,
                "start": start,
                "end": end
            }
        ).json()

        candles = res.get("result", [])
        closes = []

        for c in candles:
            if isinstance(c, list):
                closes.append(float(c[4]))
            elif isinstance(c, dict):
                closes.append(float(c["close"]))

        return closes if len(closes) > 200 else None

    except:
        return None


# =========================
# FUNDING DATA
# =========================
def get_funding_data():
    res = session.get(
        f"{BASE_URL}/v2/tickers",
        params={"contract_types": "perpetual_futures"}
    ).json()

    funding = {}

    for t in res["result"]:
        try:
            funding[t["symbol"]] = float(t["funding_rate"])
        except:
            pass

    return funding


def get_funding_interval(symbol):
    try:
        r = session.get(f"{BASE_URL}/v2/products/{symbol}").json()
        sec = r["result"]["product_specs"].get("rate_exchange_interval")
        return int(sec / 3600) if sec else None
    except:
        return None


# =========================
# BUILD DATASET
# =========================
def build_dataset(symbols):
    data = {}

    for i, sym in enumerate(symbols):
        print(f"⏳ {sym} ({i+1}/{len(symbols)})")

        closes = get_close_prices(sym)

        if closes:
            data[sym] = pd.Series(closes)
            print(f"✅ Added {sym}")
        else:
            print(f"❌ Skipped {sym}")

        time.sleep(0.2)

    df = pd.DataFrame(data).dropna()
    print(f"\n📊 Data Shape: {df.shape}")
    return df


# =========================
# TOP CORRELATED PAIRS
# =========================
def get_top_pairs(df, funding):
    corr = df.corr()

    pairs = []

    for i in corr.columns:
        for j in corr.columns:
            if i != j:
                pairs.append((i, j, corr.loc[i, j]))

    pairs = sorted(pairs, key=lambda x: x[2], reverse=True)

    seen = set()
    top = []

    for a, b, c in pairs:
        if (a, b) in seen or (b, a) in seen:
            continue

        seen.add((a, b))

        f_score = abs(funding.get(a, 0)) + abs(funding.get(b, 0))
        top.append((a, b, c, f_score))

    # 🔥 sort by funding strength
    top = sorted(top, key=lambda x: x[3], reverse=True)

    return top[:10]


# =========================
# TELEGRAM
# =========================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    requests.post(url, data={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    })


# =========================
# BUILD MESSAGE (IMPORTANT)
# =========================
def build_message(pairs, df, funding):
    msg = "📊 PAIR TRADING SIGNALS\n\n"

    for a, b, corr, _ in pairs:

        spread = df[a] - df[b]
        z = (spread.iloc[-1] - spread.mean()) / spread.std()

        f1 = funding.get(a, 0)
        f2 = funding.get(b, 0)

        i1 = get_funding_interval(a)
        i2 = get_funding_interval(b)

        msg += f"{a} ↔ {b}\n"
        msg += f"Corr: {corr:.2f}\n"
        msg += f"Z: {z:.2f}\n"
        msg += f"F1: {f1:+.4f}% ({i1}h)\n"
        msg += f"F2: {f2:+.4f}% ({i2}h)\n\n"

    return msg


# =========================
# MAIN
# =========================
def main():
    print("🚀 Fetching symbols...")
    symbols = get_symbols()

    print("📊 Fetching data...")
    df = build_dataset(symbols)

    if df.empty:
        print("❌ No data")
        return

    funding = get_funding_data()

    print("🔥 Finding best pairs...")
    pairs = get_top_pairs(df, funding)

    # 🔥 IMPORTANT: single message build
    message = build_message(pairs, df, funding)

    print("\n📨 Sending Telegram...")
    send_telegram(message)


# =========================
# RUN
# =========================
if __name__ == "__main__":
    main()
