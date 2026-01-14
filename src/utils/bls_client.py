"""BLS API client with rate limiting.

Rate-limited wrappers for BLS API calls shared across ingest modules.
"""

from ratelimit import limits, sleep_and_retry
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_result
from subsets_utils import get, post

# BLS API rate limits - shared across all modules
# Conservative limit for GET requests: 10 calls per 10 seconds
@retry(
    retry=retry_if_result(lambda r: r.status_code == 429),
    wait=wait_exponential(min=1, max=10),
    stop=stop_after_attempt(3)
)
@sleep_and_retry
@limits(calls=10, period=10)
def rate_limited_get(url, params=None):
    return get(url, params=params)


# BLS allows 50 series per POST request for registered users
# But limit calls to avoid rate limiting
@sleep_and_retry
@limits(calls=10, period=10)
def rate_limited_post(url, headers=None, json=None):
    response = post(url, headers=headers, json=json)
    response.raise_for_status()
    return response
