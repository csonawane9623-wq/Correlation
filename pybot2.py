import requests
import pandas as pd
import time

BASE_URL = "https://api.india.delta.exchange"

TIMEFRAME = "5m"
CANDLE_LIMIT = 288
MAX_SYMBOLS = 60

Z_THRESHOLD = 2.0

TELEGRAM_TOKEN = "8798956944:AAGNlEQgOneX5mgOlosVupux_Lunz01NiJo"
TELEGRAM_CHAT_ID = "1184234885"


# =========================
# GET SYMBOLS
# =========================
def get_symbols():
    url = f"{BASE_URL}/v2/products"
    res = requests.get(url).json()

    symbols = [
        p["symbol"] for p in res["result"]
        if p["contract_type"] == "perpetual_futures"
    ]

    print(f"✅ Total perpetual symbols: {len(symbols)}")
    return symbols[:MAX_SYMBOLS]


# =========================
# GET FUNDING DATA
# =========================
def get_funding_data():
    url = f"{BASE_URL}/v2/tickers"
    res = requests.get(url, params={"contract_types": "perpetual_futures"}).json()

    funding = {}
    interval = {}

    for t in res["result"]:
        try:
            funding[t["symbol"]] = float(t["funding_rate"])
        except:
            funding[t["symbol"]] = 0

    return funding


def get_funding_interval(symbol):
    try:
        url = f"{BASE_URL}/v2/products/{symbol}"
        res = requests.get(url).json()

        sec = res["result"]["product_specs"].get("rate_exchange_interval", 28800)
        return f"{int(sec/3600)}h"
    except:
        return "8h"


# =========================
# CLOSE PRICE EXTRACTION
# =========================
def extract_close(c):
    try:
        if isinstance(c, list):
            return float(c[4])
        elif isinstance(c, dict):
            return float(c.get("close"))
    except:
        return None


# =========================
# GET CLOSE PRICES
# =========================
def get_close_prices(symbol):
    try:
        end = int(time.time())
        start = end - (24 * 60 * 60)

        url = f"{BASE_URL}/v2/history/candles"
        params = {
            "resolution": TIMEFRAME,
            "symbol": symbol,
            "start": start,
            "end": end
        }

        res = requests.get(url, params=params).json()

        candles = res.get("result", [])

        if not candles:
            return None

        closes = []

        for c in candles:
            price = extract_close(c)
            if price:
                closes.append(price)

        return closes

    except Exception as e:
        print(f"❌ Error {symbol}: {e}")
        return None


# =========================
# BUILD DATASET
# =========================
def build_dataset(symbols):
    data = {}

    for i, sym in enumerate(symbols):
        print(f"⏳ {sym} ({i+1}/{len(symbols)})")

        closes = get_close_prices(sym)

        if not closes or len(closes) < 200:
            print(f"❌ Skipped {sym}")
            continue

        data[sym] = closes
        print(f"✅ Added {sym}")

        time.sleep(0.2)

    df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in data.items()]))

    print(f"\n📊 Data Shape: {df.shape}")
    return df.dropna()


# =========================
# Z-SCORE
# =========================
def calculate_zscore(series1, series2):
    spread = series1 - series2
    return (spread.iloc[-1] - spread.mean()) / spread.std()


# =========================
# FIND SIGNALS (UPDATED 🔥)
# =========================
def find_signals(df, funding):
    corr = df.corr()

    signals = []

    for i in corr.columns:
        for j in corr.columns:
            if i >= j:
                continue

            c = corr.loc[i, j]

            if c < 0.9:
                continue

            z = calculate_zscore(df[i], df[j])

            if abs(z) < Z_THRESHOLD:
                continue

            f1 = funding.get(i, 0)
            f2 = funding.get(j, 0)

            funding_edge = abs(f1 - f2)

            signals.append((i, j, c, z, f1, f2, funding_edge))

    # 🔥 SORT BY FUNDING EDGE
    signals = sorted(signals, key=lambda x: x[6], reverse=True)

    return signals[:10]


# =========================
# FORMAT MESSAGE
# =========================
def format_signal(s1, s2, corr, z, f1, f2, edge):
    i1 = get_funding_interval(s1)
    i2 = get_funding_interval(s2)

    if z > 0:
        signal = f"SELL {s1}\n👉 BUY {s2}"
    else:
        signal = f"SELL {s2}\n👉 BUY {s1}"

    msg = (
        f"📊 PAIR TRADING SIGNAL\n\n"
        f"Pair: {s1} ↔ {s2}\n"
        f"Corr: {corr:.2f}\n\n"
        f"Spread Z-Score: {z:.2f} ⚠️\n\n"
        f"Funding Edge: {edge:.4f}% 🔥\n\n"
        f"Signal:\n👉 {signal}\n\n"
        f"Funding:\n"
        f"F1: {f1:+.4f}% ({i1})\n"
        f"F2: {f2:+.4f}% ({i2})\n\n"
        f"Reason:\nHigh correlation + funding advantage\n"
    )

    return msg


# =========================
# TELEGRAM
# =========================
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    requests.post(url, data=data)


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

    print("\n🔥 Fetching funding...")
    funding = get_funding_data()

    print("🔥 Finding signals...")
    signals = find_signals(df, funding)

    if not signals:
        print("❌ No signals found")
        return

    for s in signals:
        msg = format_signal(*s)

        print("\n" + msg)
        send_telegram(msg)
        time.sleep(1)


# =========================
# RUN
# =========================
if __name__ == "__main__":
    main()