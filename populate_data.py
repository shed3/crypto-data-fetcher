import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from messari import Messari
from data_handler import DataHandler

max_requests_per_min = 30
max_requests_per_day = 2000
max_data_points_per_req = 2016

load_dotenv()
MESSARI_API_KEY = os.getenv('MESSARI_API_KEY')
messari = Messari(MESSARI_API_KEY)


mining_metrics = [
    'hashrate',
    'diff.avg',
    'min.rev.usd',
    'min.rev.ntv',
    'blk.cnt',
    'blk.size.byte',
    'blk.size.bytes.avg',
]

transaction_metrics = [
    'fees',
    'fees.ntv',
    'real.vol',
    'txn.vol',
    'txn.cnt',
    'txn.fee.med',
    'txn.fee.med.ntv',
    'txn.fee.avg',
    'txn.fee.avg.ntv',
    'txn.tsfr.cnt',
    'txn.tfr.val.ntv',
    'txn.tfr.erc20.cnt',
    'txn.tfr.erc721.cnt',
    'txn.tsfr.val.avg',
    'txn.tfr.val.med',
    'txn.tsfr.val.adj',
    'txn.tfr.val.adj.ntv',
    'act.addr.cnt',
]

exchange_metrics = [
    'exch.flow.in.usd',
    'exch.flow.out.usd',
    'exch.flow.out.usd.incl',
    'exch.flow.in.usd.incl',
    'exch.flow.in.ntv',
    'exch.flow.out.ntv',
    'exch.flow.out.ntv.incl',
    'exch.flow.in.ntv.incl',
    'exch.sply',
    'exch.sply.usd',
]

market_metrics = [
    'price',
    'mcap.out',
    'mcap.circ',
    'mcap.realized',
    'mcap.dom',
    'sply.total.iss',
    'sply.total.iss.ntv',
    'sply.liquid',
    'sply.circ',
    'sply.out',
    'new.iss.ntv',
    'new.iss.usd',
    'nvt.adj',
]

sentiment_metrics = [
    'reddit.subscribers',
    'reddit.active.users',
]


def get_available_metrics():
    all_metrics = {}
    metrics = messari.list_asset_timeseries_metric_ids()['data']['metrics']
    free = list(filter(lambda x: 'role_restriction' not in x, metrics))
    for x in free:
        values = ','.join(list(x['values_schema'].keys()))
        all_metrics[x['metric_id']] = values
    return list(all_metrics.keys())


def get_available_symbols():
    all_symbols = []
    base_query = {
        'with-profiles': False,
        'with-metrics': False,
        'fields': 'id,symbol',
        'limit': 500
    }
    page = 1
    while len(all_symbols) < 1000:
        query = base_query.copy()
        query["page"] = page
        assets = messari.get_all_assets(**query)
        if 'data' in assets and len(assets['data']) > 0:
            symbols = list([asset["symbol"] for asset in assets["data"] if "symbol" in asset and asset["symbol"]])
            all_symbols += symbols
            page += 1
        else:
            break
    return all_symbols


def create_df(data, schema):
    if len(data) < 1:
        return []
    columns = {index: value for index, value in enumerate(schema)}
    df = pd.DataFrame(data)
    df = df.rename(columns=columns)
    df['timestamp'] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.set_index('timestamp')
    df.sort_index(inplace=True)
    return df


def fetch_all_candles(exchange, symbol, interval, limit, start_key, end_key, is_seconds=True, before=None):
    minute_candles = []
    if exchange == "kucoin":
        handler = DataHandler("ccxt", start_key, end_key)
        minute_candles = handler.fetchAvailableHistory(
            exchange.fetchOHLCV,
            [symbol, interval, limit],
            {},
            interval,
            limit=limit,
            before=before
        )
    elif exchange == "messari":
        handler = DataHandler("messari", start_key, end_key)
        query_params = {
            'interval': interval,
            'order': 'ascending',
            'format': 'json',
        }
        minute_candles = handler.fetchAvailableHistory(
            messari.get_asset_timeseries,
            [symbol, 'price'],
            query_params,
            interval,
            limit=limit,
            before=before
        )
    return create_df(minute_candles["data"], minute_candles["schema"])


def write_all_candles(store, source, symbols, interval, startKey, endKey, overwrite=False):
    for symbol in symbols:
        if not store.item_exists(symbol):
            print("Fetching {}...".format(symbol))
            candles = fetch_all_candles(source, symbol, interval, max_data_points_per_req, startKey, endKey)
            if len(candles) > 1:
                # check for gaps to report
                store.write(symbol, candles, metadata={'source': source, 'interval': interval})
                print("Saved {} records for {}".format(len(candles), symbol))
            else:
                print("Skipped {} - no available records".format(symbol))
        else:
            # TODO: Data integrity checks for existing collection
            # check if metadata._last_record is before current time
            item = store.read(symbol)
            metadata = item.metadata
            mins_since_last_updated = (datetime.strptime(
                metadata["_updated"], "%Y-%m-%d %H:%M:%S.%f") - datetime.now()).total_seconds() / 60
            if(mins_since_last_updated > 60):
                # fill gap from current time to metadata._last_record
                print("Updating {} with most recent data...".format(symbol))

                # add any data that exists before metadata._first_record
                inital_candle_check = fetch_all_candles(
                    source, symbol, interval, 100, startKey, endKey, before=metadata["_last_record"])
                if(len(inital_candle_check) > 0):
                    print("Appending old entries for {}...".format(symbol))
                    store.append(symbol, inital_candle_check)

                current_candle_check = fetch_all_candles(
                    source, symbol, interval, 100, startKey, endKey, since=metadata["_first_record"])
                if(len(current_candle_check) > 0):
                    print("Appending new entries for {}...".format(symbol))
                    store.append(symbol, current_candle_check)
