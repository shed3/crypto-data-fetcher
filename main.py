from storage_handler import LocalStorage, SharedStorage
from populate_data import get_available_symbols, write_all_candles, get_available_metrics
import os
import json

data_source = "messari"
all_metrics = get_available_metrics()
all_assets = {}

symbols = get_available_symbols()
# good_symbols = []
# all_files = os.listdir('/Volumes/CAPA/.storage/storage/messari_storage/')
# for file in all_files:
#     filepath = "/Volumes/CAPA/.storage/storage/messari_storage/{}".format(file)
#     print(file, filepath)
#     try:
#         with open(filepath, 'r') as json_file:
#             overview = json.load(json_file)
#             all_assets[overview['symbol']] = overview
#     except UnicodeDecodeError:
#         pass

for metric in all_metrics:
                # print(asset)
                # for metric in all_assets[asset]['metrics']:
                #     print(metric, all_assets[asset]['metrics'][metric], (metric in all_metrics))
                #     if metric in all_metrics:
                #         if all_assets[asset]['metrics'][metric] != None:

    collection = metric
    # local_store = LocalStorage(data_source, collection)
    shared_store = SharedStorage(data_source, collection)
    store = shared_store
    write_all_candles(store, data_source, ['BTC', 'ETH', 'LINK', 'ADA', 'DOT', 'LTC'], "1d", "start", "end", metric, True)


