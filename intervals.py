INTERVAL_S = {
    "1m": 60,
    "5m": 60 * 5,
    "15m": 60 * 15,
    "30m": 60 * 30,
    "1h": 60 * 60,
    "4h": 60 * 60 * 4,
    "6h": 60 * 60 * 6,
    "12h": 60 * 60 * 12,
    "1d": 60 * 60 * 24,
}

INTERVAL_MS = {
    "1m": INTERVAL_S["1m"] * 1000,
    "5m": INTERVAL_S["5m"] * 1000,
    "15m": INTERVAL_S["15m"] * 1000,
    "30m": INTERVAL_S["30m"] * 1000,
    "1h": INTERVAL_S["1h"] * 1000,
    "4h": INTERVAL_S["4h"] * 1000,
    "6h": INTERVAL_S["6h"] * 1000,
    "12h": INTERVAL_S["12h"] * 1000,
    "1d": INTERVAL_S["1d"] * 1000,
}

INTERVAL_DATA = {
    "1m": {
        "unit": "minute",
        "frequency": 1
    },
    "5m": {
        "unit": "minute",
        "frequency": 5
    },
    "15m": {
        "unit": "minute",
        "frequency": 15
    },
    "30m": {
        "unit": "minute",
        "frequency": 30
    },
    "1h": {
        "unit": "hour",
        "frequency": 1
    },
    "4h": {
        "unit": "hour",
        "frequency": 4
    },
    "6h": {
        "unit": "hour",
        "frequency": 6
    },
    "12h": {
        "unit": "hour",
        "frequency": 12
    },
    "1d": {
        "unit": "day",
        "frequency": 1
    }
}
