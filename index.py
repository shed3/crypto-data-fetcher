import ccxt
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from storage_handler import LocalStorage
from data_handler import DataHandler

load_dotenv()

KUCOIN_API_KEY = os.getenv('KUCOIN_API_KEY')
KUCOIN_API_SECRET = os.getenv('KUCOIN_API_SECRET')
KUCOIN_API_PASSWORD = os.getenv('KUCOIN_API_PASSWORD')

kucoin = ccxt.kucoin({'apiKey': KUCOIN_API_KEY, 'secret': KUCOIN_API_SECRET, 'password': KUCOIN_API_PASSWORD})
kucoin.rateLimit = 60
kucoin.load_markets()
symbols = list(filter(lambda x: x.endswith("USDT"), kucoin.symbols))
symbols = list([symbol[0:-5] for symbol in symbols])
store = LocalStorage("historical", "Crypto.Candles")


def fetch_all_candles(exchange, symbol, interval, limit, start_key, end_key, is_seconds=True, before=None):
    handler = DataHandler(store, exchange, start_key, end_key)
    minute_candles = handler.fetchAvailableHistory(exchange.fetchOHLCV, [symbol, interval, limit], interval, limit=limit, before=before)
    if len(minute_candles) > 1:
        # format minute candles as pandas dataframe
        data = pd.DataFrame(minute_candles)
        data = data.rename(columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'})
        data['timestamp'] = pd.to_datetime(data["timestamp"], unit="ms")
        data = data.set_index('timestamp')
        return data
    return []


def write_all_candles(dir, interval):
    print(store.current_items)
    for symbol in symbols:
        if not store.item_exists("" + symbol + dir):
            print("Fetching {}...".format(symbol))
            candles = fetch_all_candles(kucoin, symbol + "/USDT", interval, 1500, "startAt", "endAt")
            print(candles)
            if len(candles) > 1:
                # check for gaps to report
                # store.write(symbol + dir, candles, metadata={'source': 'kucoin', interval: interval})
                print("Saved {} records for {}".format(len(candles), symbol))
            else:
                print("Skipped {} - no available records".format(symbol))
        else:
            # TODO: Data integrity checks for existing collection
            # check if metadata._last_record is before current time
            item = store.read(symbol + dir)
            metadata = item.metadata
            mins_since_last_updated = (datetime.strptime(metadata["_updated"], "%Y-%m-%d %H:%M:%S.%f") - datetime.now()).total_seconds() / 60
            if(mins_since_last_updated > 60):
                # fill gap from current time to metadata._last_record
                print("Updating {} with most recent data...".format(symbol))

                # add any data that exists before metadata._first_record
                inital_candle_check = fetch_all_candles(kucoin, symbol, interval, 100, "startAt", "endAt", before=metadata["_last_record"])
                if(len(inital_candle_check) > 0):
                    print("Appending old entries for {}...".format(symbol))
                    store.append(symbol + dir, inital_candle_check)

                current_candle_check = fetch_all_candles(kucoin, symbol, interval, 100, "startAt", "endAt", since=metadata["_first_record"])
                if(len(current_candle_check) > 0):
                    print("Appending new entries for {}...".format(symbol))
                    store.append(symbol + dir, current_candle_check)


def read_all_candles(dir):
    for itemName in store.current_items:
        item = store.read(itemName + dir)
        metadata = item.metadata
        print(metadata)


write_all_candles("/Day", '1d')
# read_all_candles("/Day")
