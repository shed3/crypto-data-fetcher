from storage_handler import LocalStorage, SharedStorage
from populate_data import get_available_symbols, write_all_candles


data_source = "messari"
collection = "price"
local_store = LocalStorage(data_source, collection)
shared_store = SharedStorage(data_source, collection)

default_symbols = ["ETH","BTC","MATIC","CRO","FIL","UNI","BAT","LINK","GRT","AAVE","COMP","SNX"]
symbols = get_available_symbols()
symbols = symbols[0:1000]
write_all_candles(shared_store, data_source, symbols, "1d", "start", "end", True)


# Write all metrics for all assets
# Write x metric for all assets
