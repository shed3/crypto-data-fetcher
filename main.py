from storage_handler import LocalStorage, SharedStorage
from populate_data import get_available_symbols, write_all_candles, get_available_metrics


data_source = "messari"
all_metrics = get_available_metrics()
symbols = get_available_symbols()
for metric in all_metrics:
    collection = metric
    # local_store = LocalStorage(data_source, collection)
    shared_store = SharedStorage(data_source, collection)
    store = shared_store
    write_all_candles(store, data_source, symbols, "1d", "start", "end", metric, True)


