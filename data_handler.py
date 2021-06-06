"""
Historical Data Handler

This class allows enables devs to bridge gap between requesting large
historical datasets and storing it with any arbitrary data store (provided
the data store implements the proper interface).

Supported store options:
    * pystore local
    * pystore network drive

Supported target options:
    * ccxt exchanges
"""
import os
from dotenv import load_dotenv
from datetime import datetime
from intervals import INTERVAL_MS

load_dotenv()

STORAGE_DIRECTORY = os.getenv('STORAGE_DIRECTORY')


class DataHandler:

    def __init__(self, store, target, start_key="startAt", end_key="endAt", max_request_attempts=2):
        self.store = store          # Storage instance
        self.target = target        # ccxt exchange or other api wrapper
        self.start_key = start_key  # start key for target historical requests
        self.end_key = end_key      # end key for target historical requests
        self.max_request_attempts = max_request_attempts      # end key for target historical requests

    def fetchAvailableHistory(self, func, funcArgs, interval, **kwargs):
        """
        Fetches historical data as far back as target source allows.

        Parameters
        ----------
        func : function
            function responsible for requesting data. Supported options are ccxt.fetchTrades(), ccxt.fetchOHLCV(), ccxt.fetchOrders(), ccxt.fetchOpenOrders(), ccxt.fetchClosedOrders(), ccxt.fetchMyTrades(), ccxt.fetchTransactions(), ccxt.fetchDeposits(), ccxt.fetchWithdrawals()
        funcArgs : list
            list of args to pass to function
        interval : list
            string representation of timeseries interval. Options are 1m, 5m, 15m, 30m, 1h, 4h, 6h, 12h, 1d

        Returns
        -------
        list
            a list of all timeseries data collected
        """

        limit = kwargs.get("limit", 1000)
        is_seconds = kwargs.get("is_seconds", True)
        before = kwargs.get("before", None)
        after = kwargs.get("after", None)
        remaining_attempts = self.max_request_attempts
        all_data = []
        if before:
            end_time = int(datetime.strptime(before, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000)
        elif after:
            end_time = int(datetime.strptime(before, "%Y-%m-%d %H:%M:%S.%f").timestamp() * 1000)
        else:
            end_time = int(datetime.now().timestamp() * 1000)

        start_time = end_time - (INTERVAL_MS[interval] * limit)

        while True:
            params = {}
            params[self.start_key] = int(start_time / 1000) if is_seconds else start_time
            params[self.end_key] = int(end_time / 1000) if is_seconds else end_time
            page = func(*funcArgs, params=params)
            if len(page) > 1:
                # page contains data so sort and append to minute_candles
                page = sorted(page, key=lambda x: x[0])
                all_data = all_data + page
                end_time = page[0][0] - INTERVAL_MS[interval]    # one interval before the first timestamp received
                start_time = end_time - (INTERVAL_MS[interval] * limit)     # go back interval x limit from end date to capture next time chunk
            elif remaining_attempts > 0:
                # TODO (change retry date strategy) -> reduce start and end time by a large interval (year, month, week) then work forward to first failure date
                end_time = end_time - INTERVAL_MS[interval]
                start_time = start_time - INTERVAL_MS[interval]
                remaining_attempts -= 1
            else:
                # ran out of retry attempts -> break loop
                break
        return all_data
