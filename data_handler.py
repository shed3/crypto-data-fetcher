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
import re
from time import sleep
from datetime import datetime
from intervals import INTERVAL_MS


class DataHandler:

    def __init__(self, source, start_key="startAt", end_key="endAt", max_request_attempts=2):
        self.source = source        # name of wrapper (ccxt, messari, etc)
        self.start_key = start_key  # start key for target historical requests
        self.end_key = end_key      # end key for target historical requests
        self.max_request_attempts = max_request_attempts      # end key for target historical requests

    def get_timeframe(self, interval, size, before=None, after=None, is_seconds=True):
        if before:
            start = datetime.strptime(before, "%Y-%m-%d %H:%M:%S.%f")
        elif after:
            start = datetime.strptime(after, "%Y-%m-%d %H:%M:%S.%f")
        else:
            start = datetime.now()

        timeframe = Timeframe(interval, size, start, is_seconds)
        timeframe.prev()    # go to prev timeframe so start is the end of new timeframe
        return timeframe

    def fetchAvailableHistory(self, func, func_args, func_kwargs, interval, **kwargs):
        """
        Fetches historical data as far back as target source allows.

        Parameters
        ----------
        func : function
            function responsible for requesting data. Supported options are ccxt.fetchTrades(), ccxt.fetchOHLCV(), ccxt.fetchOrders(), ccxt.fetchOpenOrders(), ccxt.fetchClosedOrders(), ccxt.fetchMyTrades(), ccxt.fetchTransactions(), ccxt.fetchDeposits(), ccxt.fetchWithdrawals()
        func_args : list
            list of args to pass to function
        func_kwargs : dict
            list of kwargs to pass to function
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
        request_retries_left = self.max_request_attempts
        period_skips_left = self.max_request_attempts
        all_data = []
        timeframe = self.get_timeframe(interval, limit, before, after, is_seconds)
        values_schema = []
        while True:
            error_code = None
            sleep_time = 0
            func_kwargs[self.start_key] = timeframe.start_timestamp_sec if is_seconds else timeframe.start_timestamp
            func_kwargs[self.end_key] = timeframe.end_timestamp_sec if is_seconds else timeframe.end_timestamp
            if self.source == "messari":
                page = func(*func_args, **func_kwargs)
                # print(page)
                error_code = page["status"].get("error_code", None)
                if error_code == 429:
                    sleep_time = re.findall('\d+', page["status"]["error_message"])
                    sleep_time = int(sleep_time[0])
                page = page.get("data", {})
                if (len(values_schema) < 1):
                    values_schema = page.get("schema", {}).get("values_schema", {})
                    values_schema = values_schema.keys()
                page = page.get("values", [])
            elif self.source == "ccxt":
                page = func(*func_args, params=func_kwargs)

            if page and len(page) > 1:
                # page contains data so sort and append to minute_candles
                all_data = all_data + page
                request_retries_left = self.max_request_attempts
                period_skips_left = self.max_request_attempts
                timeframe.prev()
            elif period_skips_left > 0:
                # next itreation -> attempt to get data from previous period
                period_skips_left -= 1
                timeframe.prev()
            elif error_code == 429 and request_retries_left > 0:
                # next itreation -> attempt to get data from same period
                # TODO (change retry date strategy) -> reduce start and end time by a large interval (year, month, week) then work forward to first failure date
                print("rate limited...sleeping for {} seconds".format(sleep_time))
                sleep(sleep_time)
                request_retries_left -= 1
            else:
                # ran out of retry attempts -> break loop
                break
        return {
            "schema": values_schema,
            "data": all_data,
        }


class Timeframe:
    def __init__(self, interval, size, start, use_seconds=False):
        self.interval = interval    # string representation of timeseries interval. Options are 1m, 5m, 15m, 30m, 1h, 4h, 6h, 12h, 1d
        self.size = size            # number of intervals per timeframe
        self.start_timestamp = start.timestamp() * 1000
        self.end_timestamp = self.start_timestamp + (INTERVAL_MS[self.interval] * self.size)
        self.use_seconds = use_seconds

    @property
    def start_timestamp_sec(self):
        return int(self.start_timestamp / 1000)

    @property
    def end_timestamp_sec(self):
        return int(self.end_timestamp / 1000)

    def prev(self):
        self.end_timestamp = self.start_timestamp - INTERVAL_MS[self.interval]                  # one interval before the current start time
        self.start_timestamp = self.end_timestamp - (INTERVAL_MS[self.interval] * self.size)     # go back interval x size from end time to set prev time chunk

    def next(self):
        self.start_timestamp = self.end_timestamp + INTERVAL_MS[self.interval]                   # one interval after the current end time
        self.end_timestamp = self.start_timestamp + (INTERVAL_MS[self.interval] * self.size)     # go forward interval x size from start time to set next time chunk
