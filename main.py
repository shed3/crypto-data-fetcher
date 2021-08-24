from storage_handler import LocalStorage, SharedStorage
from populate_data import get_available_symbols, write_all_candles


data_source = "messari"
collection = "price"
local_store = LocalStorage(data_source, collection)
shared_store = SharedStorage(data_source, collection)
store = shared_store

symbols = get_available_symbols()
symbols = symbols[0:1000]
write_all_candles(store, data_source, symbols, "1d", "start", "end", True)
