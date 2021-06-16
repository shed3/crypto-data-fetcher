"""
Data Storage Handlers

These wrapper classes are used to provide a unified API for
interacting with different stores as they are added.

"""

import os
from datetime import datetime
import pystore
# import pandas as pd
from dotenv import load_dotenv
from intervals import INTERVAL_S

load_dotenv()
SHARED_DIRECTORY = os.getenv('SHARED_DIRECTORY')
# LOCAL_DIRECTORY = os.getenv('LOCAL_DIRECTORY')
STORAGE_DIRECTORY = os.getenv('STORAGE_DIRECTORY')
STORAGE_NAME = os.getenv('STORAGE_NAME')


class BaseTimeseriesStorage:

    def __init__(self, name, base_dir, collection='Crypto.Candles.Day'):
        self.current_dir = os.path.abspath(os.path.curdir)
        self.base_dir = os.path.abspath(base_dir)
        self.storage_dir = "{}/{}".format(self.base_dir, STORAGE_DIRECTORY)

        # create base directory is it doesnt exist
        if not os.path.isdir(self.base_dir):
            os.mkdir(self.base_dir)

        # create storage directory is it doesnt exist
        if not os.path.isdir(self.storage_dir):
            os.mkdir(self.storage_dir)

        pystore.set_path(self.storage_dir)
        self.store = pystore.store(name)
        self.collections = self.store.list_collections()
        self.set_collection(collection)

    def check_gaps(self, data, interval):
        interval_gap = (data.index.to_series().diff()).dt.seconds > interval
        num_gaps = len(list(filter(lambda x: x, interval_gap)))
        return {
            "num_gaps": num_gaps,
            "df": interval_gap
        }

    def item_exists(self, item):
        try:
            self.current_collection.item(item)
            return True
        except ValueError:
            return False

    def set_collection(self, name):
        self.current_collection = self.store.collection(name)
        self.current_items = self.current_collection.list_items()

    def read(self, item):
        return self.current_collection.item(item)

    def write(self, item, data, metadata={}, overwrite=True):
        range = data.iloc[[0, -1]].index
        gaps = self.check_gaps(data, INTERVAL_S[metadata["interval"]])
        additional_metadata = {
            "_first_record": range[0].strftime("%Y-%m-%d %H:%M:%S.%f"),
            "_last_record": range[1].strftime("%Y-%m-%d %H:%M:%S.%f"),
            '_total_records': len(data),
            "gaps": gaps["num_gaps"]
        }
        if item not in self.current_items:
            additional_metadata["_created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        metadata = {
            **metadata,
            **additional_metadata,
        }
        self.current_collection.write(item, data, metadata=metadata, overwrite=overwrite)

    def append(self, item, data):
        # data must be a list
        self.current_collection.append(item, data)
        existing_data = self.read(item)
        # parse current items date range metadata
        first_record = datetime.strptime(existing_data.metadata["_first_record"], "%Y-%m-%d %H:%M:%S.%f")
        last_record = datetime.strptime(existing_data.metadata["_last_record"], "%Y-%m-%d %H:%M:%S.%f")

        # get date range of new dataset
        new_dataset_min = data.index.min
        new_dataset_max = data.index.max

        # check for neccessary updates to item's metadata
        updated_metadata = existing_data.metadata.copy()
        metadata_was_updated = False
        if new_dataset_min < first_record:
            updated_metadata["_first_record"] = new_dataset_min.strftime("%Y-%m-%d %H:%M:%S.%f")
            metadata_was_updated = True

        if new_dataset_max < last_record:
            updated_metadata["_last_record"] = new_dataset_max.strftime("%Y-%m-%d %H:%M:%S.%f")
            metadata_was_updated = True

        if len(data) > 0:
            updated_metadata["_total_records"] = updated_metadata["_total_records"] + len(data)
            metadata_was_updated = True

        # update metadata if needed
        if metadata_was_updated:
            pystore.utils.write_metadata(pystore.utils.make_path(self.store, self.current_collection, item), updated_metadata)


class SharedStorage(BaseTimeseriesStorage):
    def __init__(self, name=STORAGE_NAME, collection='Crypto.Candles.Day'):
        super().__init__(name, SHARED_DIRECTORY, collection)


# class LocalStorage(BaseTimeseriesStorage):
#     def __init__(self, name=STORAGE_NAME, collection='Crypto.Candles.Day'):
#         super().__init__(name, LOCAL_DIRECTORY, collection)
