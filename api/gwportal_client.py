# api/gwportal_client.py
#
# Minimal GWPortal API client — trimmed to only what the GCN bot needs
# (reference-image availability lookups via ``query_tiles``). All connection
# details come exclusively from environment variables; nothing site-specific
# is baked into this file.
#
# Environment variables:
#   GWPORTAL_BASE_URL  e.g. "https://<your-portal-host>:<port>/"
#   GWPORTAL_API_KEY   API key, sent as the "X-API-Key" header

import os
import sys
import requests
from requests.exceptions import RequestException, JSONDecodeError
from urllib.parse import urljoin  # robust URL construction


def _tls_hint(exc):
    """
    Return an actionable hint when a request failed on TLS certificate
    verification, else ''. The TLS handshake completes BEFORE any HTTP header
    is sent, so these failures can never be caused by a wrong or expired API
    key — spell that out instead of leaving a cryptic SSLError.
    """
    text = str(exc)
    if 'CERTIFICATE_VERIFY_FAILED' not in text:
        return ''
    if 'certificate has expired' in text:
        return ("Hint: the portal server's TLS certificate has expired. This is a "
                "server-side problem (NOT an expired API key) — ask the portal admin "
                "to renew it. Temporary workaround, at your own risk, until the renewal "
                "lands: export GWPORTAL_INSECURE_SKIP_VERIFY=1")
    if 'IP address mismatch' in text or 'Hostname mismatch' in text:
        return ("Hint: the server certificate is issued for a host NAME, not an IP "
                "address. Set GWPORTAL_BASE_URL to the https host name, not an IP.")
    return ("Hint: TLS certificate verification failed — a server certificate "
            "problem, not an API-key problem.")


class GWPortalClient:
    """
    Minimal client for the GWPortal REST API.

    Only the pieces the GCN bot relies on are implemented: authenticated
    session setup and :meth:`query_tiles` (used to check which required tiles
    already have reference images available in the DB).

    Connection settings are read from the environment when not passed
    explicitly:
      * ``GWPORTAL_BASE_URL`` — base URL, e.g. ``https://<host>:<port>/``
      * ``GWPORTAL_API_KEY``  — API key, sent as the ``X-API-Key`` header
    """

    def __init__(self, base_url=None, api_key=None):
        self.base_url = base_url or os.getenv('GWPORTAL_BASE_URL')
        self.api_key = api_key or os.getenv('GWPORTAL_API_KEY')

        if not self.base_url:
            raise ValueError("GWPORTAL_BASE_URL is not set.")
        if not self.api_key:
            raise ValueError("GWPORTAL_API_KEY is not set.")

        # Ensure base_url ends with a slash so urljoin() behaves predictably.
        if not self.base_url.endswith('/'):
            self.base_url += '/'

        self.session = requests.Session()
        self.session.headers.update({
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

        # Emergency escape hatch while the server certificate is broken (e.g.
        # expired before its renewal lands). Verification off means the API key
        # travels on an unauthenticated channel — unset the variable as soon as
        # the server is fixed. (verify is passed per request: an explicit False
        # survives the REQUESTS_CA_BUNDLE env merge, unlike Session.verify.)
        self.verify_tls = os.getenv('GWPORTAL_INSECURE_SKIP_VERIFY', '').strip().lower() \
            not in ('1', 'true', 'yes', 'on')
        if not self.verify_tls:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            print("GWPortalClient: WARNING - TLS certificate verification is DISABLED "
                  "(GWPORTAL_INSECURE_SKIP_VERIFY). Emergency use only; unset it once "
                  "the server certificate is renewed.", file=sys.stderr)

        try:
            # Test connection by querying the /api/tiles/ endpoint.
            test_url = urljoin(self.base_url, 'api/tiles/')
            response = self.session.get(test_url, params={'page_size': 1},
                                        timeout=10, verify=self.verify_tls)
            response.raise_for_status()
            print("GWPortalClient: API connection successful.", file=sys.stderr)
        except RequestException as e:
            print(f"GWPortalClient: API connection test failed. Error: {e}", file=sys.stderr)
            hint = _tls_hint(e)
            if hint:
                print(f"GWPortalClient: {hint}", file=sys.stderr)
            # Do not raise — allow the caller to degrade gracefully.

    def _make_request(self, method, endpoint, params=None, data=None):
        """
        Make a single API request. Prepends the ``api/`` prefix to the given
        endpoint and returns the decoded JSON body (or ``{}`` for empty
        responses).
        """
        api_endpoint = f"api/{endpoint.lstrip('/')}"
        url = urljoin(self.base_url, api_endpoint)

        try:
            response = self.session.request(method, url, params=params, json=data,
                                            timeout=60, verify=self.verify_tls)
            response.raise_for_status()  # raises HTTPError for 4xx/5xx

            if not response.content:
                return {}  # 204 No Content or empty body
            return response.json()

        except JSONDecodeError:
            raise RequestException(
                f"API returned non-JSON response. Status: {response.status_code}. "
                f"Text: {response.text[:200]}... URL: {url}")

        except RequestException as e:
            if e.response is not None:
                try:
                    detail = e.response.json().get('error', 'Unknown API Error')
                    raise RequestException(
                        f"API Error ({e.response.status_code}): {detail} (URL: {url})")
                except JSONDecodeError:
                    raise RequestException(
                        f"API Error ({e.response.status_code}): "
                        f"{e.response.text[:200]}... (URL: {url})")
            hint = _tls_hint(e)
            suffix = f"\n{hint}" if hint else ""
            raise RequestException(f"Network Error: {e} (URL: {url}){suffix}")

    def query_tiles(self, **kwargs):
        """
        Query the ``tiles`` endpoint. Used to look up reference-image
        availability by tile name (e.g. ``tile_name="T001,T002"``).
        """
        return self._make_request('get', 'tiles/', params=kwargs)
