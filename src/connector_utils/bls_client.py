"""BLS API client.

All BLS API access goes through this module: rate limiting, auth, request
shaping, and response interpretation. Nodes orchestrate; this client talks
to the API.
"""

import os

from ratelimit import limits, sleep_and_retry
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_result

from subsets_utils import get, post

API_BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SURVEYS_URL = "https://api.bls.gov/publicAPI/v2/surveys"
POPULAR_URL = "https://api.bls.gov/publicAPI/v2/timeseries/popular"

BATCH_SIZE = 50  # BLS limit for registered users
MAX_YEARS_PER_REQUEST = 20


class DailyQuotaExceeded(Exception):
    """Raised when the BLS API daily request limit is reached."""
    pass


def get_api_key() -> str:
    return os.environ['BLS_API_KEY']


@retry(
    retry=retry_if_result(lambda r: r.status_code == 429),
    wait=wait_exponential(min=1, max=10),
    stop=stop_after_attempt(3),
)
@sleep_and_retry
@limits(calls=10, period=10)
def rate_limited_get(url, params=None):
    return get(url, params=params)


@sleep_and_retry
@limits(calls=10, period=10)
def rate_limited_post(url, headers=None, json=None):
    response = post(url, headers=headers, json=json)
    response.raise_for_status()
    return response


def _message_str(data: dict) -> str:
    message = data.get("message", ["Unknown error"])
    if isinstance(message, list):
        return message[0] if message else "Unknown error"
    return str(message)


def _check_quota(data: dict) -> None:
    if "daily threshold" in _message_str(data).lower():
        raise DailyQuotaExceeded(_message_str(data))


def fetch_surveys() -> list[dict]:
    """Fetch the list of all BLS surveys."""
    response = rate_limited_get(SURVEYS_URL, params={"registrationkey": get_api_key()})
    data = response.json()
    _check_quota(data)
    if data.get("status") != "REQUEST_SUCCEEDED":
        raise ValueError(f"BLS surveys API error: {_message_str(data)}")
    return data.get("Results", {}).get("survey", [])


def fetch_popular_series(survey: str | None = None) -> list[dict]:
    """Fetch popular series, optionally filtered to a single survey."""
    params = {"registrationkey": get_api_key()}
    if survey:
        params["survey"] = survey
    response = rate_limited_get(POPULAR_URL, params=params)
    data = response.json()
    _check_quota(data)
    if data.get("status") != "REQUEST_SUCCEEDED":
        return []
    return data.get("Results", {}).get("series", [])


def fetch_series_batch(
    series_ids: list[str],
    start_year: int,
    end_year: int,
    *,
    latest: bool = False,
) -> list[dict]:
    """Fetch a batch of series from the BLS timeseries API.

    Args:
        series_ids: Up to BATCH_SIZE series IDs.
        start_year, end_year: Inclusive year range.
        latest: If True, BLS returns only the most recent observation per series.
            Used by refresh runs to cheaply check for new data.
    """
    headers = {"Content-type": "application/json"}
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": get_api_key(),
        "catalog": True,
    }
    if latest:
        payload["latest"] = True

    response = rate_limited_post(API_BASE_URL, headers=headers, json=payload)
    data = response.json()

    _check_quota(data)

    if data.get("status") in ("REQUEST_FAILED", "REQUEST_NOT_PROCESSED"):
        raise ValueError(f"BLS API error: {_message_str(data)}")

    return data.get("Results", {}).get("series", [])


# Human-readable titles for hardcoded JOLTS series. The BLS catalog endpoint
# does not return titles for these so we keep a static fallback alongside the
# client that fetches them.
JOLTS_SERIES_NAMES = {
    "JTS000000000000000JOL": "Total nonfarm job openings, level, seasonally adjusted",
    "JTS000000000000000HIL": "Total nonfarm hires, level, seasonally adjusted",
    "JTS000000000000000QUL": "Total nonfarm quits, level, seasonally adjusted",
    "JTS000000000000000LDL": "Total nonfarm layoffs and discharges, level, seasonally adjusted",
    "JTS000000000000000TSL": "Total nonfarm total separations, level, seasonally adjusted",
    "JTS000000000000000JOR": "Total nonfarm job openings rate, seasonally adjusted",
    "JTS000000000000000HIR": "Total nonfarm hires rate, seasonally adjusted",
    "JTS000000000000000QUR": "Total nonfarm quits rate, seasonally adjusted",
    "JTS000000000000000LDR": "Total nonfarm layoffs and discharges rate, seasonally adjusted",
    "JTS000000000000000TSR": "Total nonfarm total separations rate, seasonally adjusted",
    "JTS100000000000000JOL": "Total private job openings, level, seasonally adjusted",
    "JTS100000000000000HIL": "Total private hires, level, seasonally adjusted",
    "JTS100000000000000QUL": "Total private quits, level, seasonally adjusted",
    "JTS900000000000000JOL": "Government job openings, level, seasonally adjusted",
    "JTS510000000000000JOL": "Professional and business services job openings, level",
    "JTS540000000000000JOL": "Health care and social assistance job openings, level",
    "JTS440000000000000JOL": "Retail trade job openings, level",
    "JTS720000000000000JOL": "Accommodation and food services job openings, level",
    "JTS320000000000000JOL": "Manufacturing job openings, level",
    "JTS230000000000000JOL": "Construction job openings, level",
}
