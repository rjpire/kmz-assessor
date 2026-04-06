"""
arcgis_detector.py
==================
Given any county GIS map viewer URL, attempt to find the underlying
ArcGIS REST FeatureServer or MapServer /query endpoint for parcel data.

No new dependencies — uses requests and beautifulsoup4 already in requirements.txt.

Public API:
    detect_arcgis_from_url(map_url, timeout=12, validate=True) -> DetectionResult
    DetectionResult.candidates  — ranked list of /query URLs, best first
    DetectionResult.warnings    — user-facing diagnostic messages
    DetectionResult.is_spa      — True when page appears JS-only (no HTML content)
    DetectionResult.non_arcgis  — True when no ArcGIS patterns found at all
"""

import re
import json
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse, urljoin, urlencode

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    raise ImportError("requests and beautifulsoup4 are required.")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PARCEL_KEYWORDS = frozenset([
    "parcel", "parcels", "ownership", "assessor", "property",
    "properties", "tax", "land", "apn", "pin", "realestate",
    "real_estate", "cadastral", "taxparcel",
])

_NEGATIVE_KEYWORDS = frozenset([
    "road", "street", "hydro", "flood", "zone", "boundary",
    "address", "building", "elevation", "aerial", "imagery",
    "contour", "survey", "plss", "section", "township",
])

_ARCGIS_REST_RE = re.compile(
    r'https?://[^\s\'"<>]+/(?:arcgis|ArcGIS)/rest/services/[^\s\'"<>?#]+',
    re.IGNORECASE,
)

_FEATURE_MAP_SERVER_RE = re.compile(
    r'https?://services\d*\.arcgis\.com/[^\s\'"<>?#]+/(?:FeatureServer|MapServer)(?:/\d+)?',
    re.IGNORECASE,
)

_ITEM_ID_RES = [
    re.compile(r'[?&]id=([A-Fa-f0-9]{32})'),
    re.compile(r'/items?/([A-Fa-f0-9]{32})'),
    re.compile(r'/experience/([A-Fa-f0-9]{32})'),
    re.compile(r'/apps/[^/]+/index\.html#/([A-Fa-f0-9]{32})'),
    re.compile(r'/home/item\.html\?id=([A-Fa-f0-9]{32})'),
]

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class DetectionResult:
    candidates: list   # list[str] — ranked /query URLs, best first
    warnings:   list   # list[str] — user-facing messages
    is_spa:     bool = False
    non_arcgis: bool = False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get(url: str, timeout: int) -> Optional[requests.Response]:
    """GET with browser UA; returns None on any error."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _BROWSER_UA},
            timeout=timeout,
            allow_redirects=True,
        )
        return resp
    except requests.Timeout:
        return None
    except Exception:
        return None


def _normalize_to_query_url(url: str) -> str:
    """
    Normalize any ArcGIS REST URL to end in /query.
    Strips trailing slashes, appends /0/query or /query as needed.
    """
    url = url.strip().rstrip("/")
    # Already a query endpoint
    if url.lower().endswith("/query"):
        return url
    # Ends in /FeatureServer or /MapServer with no layer number
    if re.search(r'/(FeatureServer|MapServer)$', url, re.IGNORECASE):
        return url + "/0/query"
    # Ends in /FeatureServer/N or /MapServer/N
    if re.search(r'/(FeatureServer|MapServer)/\d+$', url, re.IGNORECASE):
        return url + "/query"
    # Generic: just append /query
    return url + "/query"


def _score_url(url: str) -> int:
    """Score a URL by parcel relevance. Returns int; <1 means likely not parcel data."""
    url_lower = url.lower()
    score = 0
    for kw in _PARCEL_KEYWORDS:
        if kw in url_lower:
            score += (3 if kw in ("parcel", "parcels") else 2)
    for kw in _NEGATIVE_KEYWORDS:
        if kw in url_lower:
            score -= 1
    if "featureserver" in url_lower:
        score += 2
    return score


def _extract_rest_urls_from_text(text: str) -> list:
    """Regex-scan raw text for ArcGIS REST service URLs."""
    found = set()
    for m in _ARCGIS_REST_RE.finditer(text):
        found.add(m.group(0).rstrip(".,;\"'"))
    for m in _FEATURE_MAP_SERVER_RE.finditer(text):
        found.add(m.group(0).rstrip(".,;\"'"))
    return list(found)


def _extract_item_id(url: str) -> Optional[str]:
    """Extract an ArcGIS Online item ID from common URL patterns."""
    for pat in _ITEM_ID_RES:
        m = pat.search(url)
        if m:
            return m.group(1)
    return None


def _walk_json_for_service_urls(obj, found: set, depth: int = 0):
    """Recursively walk a JSON object for ArcGIS REST service URLs."""
    if depth > 8:
        return
    if isinstance(obj, str):
        if "/rest/services/" in obj or "FeatureServer" in obj or "MapServer" in obj:
            found.update(_extract_rest_urls_from_text(obj))
    elif isinstance(obj, dict):
        for v in obj.values():
            _walk_json_for_service_urls(v, found, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _walk_json_for_service_urls(item, found, depth + 1)


# ---------------------------------------------------------------------------
# Detection strategies
# ---------------------------------------------------------------------------

def _scrape_html(map_url: str, timeout: int) -> tuple:
    """
    Fetch the page and extract ArcGIS REST URLs from raw HTML.
    Returns (rest_urls, config_json_urls, is_spa, html_text).
    """
    resp = _get(map_url, timeout)
    if resp is None or not resp.ok:
        return [], [], False, ""

    html = resp.text
    rest_urls = _extract_rest_urls_from_text(html)

    # Find JSON config links (<script src>, <link href> with config/app/webmap in name)
    config_urls = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["script", "link"]):
            src = tag.get("src") or tag.get("href") or ""
            if any(kw in src.lower() for kw in ("config", "app.json", "webmap", "init.json")):
                config_urls.append(urljoin(map_url, src))
    except Exception:
        pass

    # Detect SPA: tiny body and no REST URLs found
    is_spa = (len(html) < 3000 and not rest_urls) or (
        "<div id=\"root\">" in html and not rest_urls
    )

    return rest_urls, config_urls, is_spa, html


def _fetch_config_json(config_url: str, timeout: int) -> list:
    """Fetch a JSON config URL and walk it for REST service URLs."""
    resp = _get(config_url, timeout)
    if resp is None or not resp.ok:
        return []
    try:
        data = resp.json()
        found = set()
        _walk_json_for_service_urls(data, found)
        return list(found)
    except Exception:
        return []


def _fetch_agol_item(item_id: str, timeout: int) -> list:
    """
    Fetch the ArcGIS Online item data JSON and extract operational layer URLs.
    Tries both the public sharing API and the common portal pattern.
    """
    urls_to_try = [
        f"https://www.arcgis.com/sharing/rest/content/items/{item_id}/data?f=json",
        f"https://www.arcgis.com/sharing/rest/content/items/{item_id}?f=json",
    ]
    found = set()
    for url in urls_to_try:
        resp = _get(url, timeout)
        if resp is None or not resp.ok:
            continue
        try:
            data = resp.json()
            _walk_json_for_service_urls(data, found)
        except Exception:
            continue
    return list(found)


def _enumerate_service_catalog(base_domain: str, timeout: int) -> list:
    """
    Hit the ArcGIS REST services catalog at base_domain/arcgis/rest/services
    and enumerate service + layer URLs. Limited to 50 services to avoid crawling
    huge state GIS portals indefinitely.
    """
    catalog_url = f"{base_domain}/arcgis/rest/services?f=json"
    resp = _get(catalog_url, timeout)
    if resp is None or not resp.ok:
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    found = []
    services = data.get("services", [])
    folders = data.get("folders", [])

    def _process_services(svc_list):
        for svc in svc_list[:50]:
            svc_type = svc.get("type", "")
            if svc_type not in ("FeatureServer", "MapServer"):
                continue
            svc_name = svc.get("name", "")
            url = f"{base_domain}/arcgis/rest/services/{svc_name}/{svc_type}"
            # Get layer list
            resp2 = _get(url + "?f=json", timeout)
            if resp2 and resp2.ok:
                try:
                    d = resp2.json()
                    for layer in d.get("layers", []):
                        lid = layer.get("id", 0)
                        found.append(f"{url}/{lid}")
                except Exception:
                    found.append(url)

    _process_services(services)

    # Also check first 3 folders
    for folder in folders[:3]:
        folder_url = f"{base_domain}/arcgis/rest/services/{folder}?f=json"
        resp_f = _get(folder_url, timeout)
        if resp_f and resp_f.ok:
            try:
                folder_data = resp_f.json()
                _process_services(folder_data.get("services", []))
            except Exception:
                pass

    return found


def _validate_candidate(query_url: str, timeout: int) -> bool:
    """Return True if the URL is a working ArcGIS REST endpoint with fields."""
    try:
        from county_lookup import fetch_arcgis_fields
        fields = fetch_arcgis_fields(query_url)
        return len(fields) > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_arcgis_from_url(
    map_url: str,
    timeout: int = 12,
    validate: bool = True,
) -> DetectionResult:
    """
    Given any county GIS map viewer URL, attempt to find the underlying
    ArcGIS REST FeatureServer or MapServer /query endpoint for parcel data.

    Parameters
    ----------
    map_url  : any URL — county map viewer, ArcGIS Online app, GIS portal, etc.
    timeout  : per-request timeout in seconds
    validate : if True, test each candidate with fetch_arcgis_fields() and
               drop any that don't respond (slower but more reliable results)

    Returns DetectionResult with candidates ranked by parcel relevance.
    """
    warnings = []
    all_raw_urls = set()

    parsed = urlparse(map_url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"

    # ── Strategy 1: Scrape HTML ───────────────────────────────────────────
    html_urls, config_urls, is_spa, html_text = _scrape_html(map_url, timeout)
    all_raw_urls.update(html_urls)

    # ── Strategy 2: Fetch JSON configs found in HTML ──────────────────────
    for cfg_url in config_urls[:5]:
        all_raw_urls.update(_fetch_config_json(cfg_url, timeout))

    # ── Strategy 3: ArcGIS Online item API ───────────────────────────────
    item_id = _extract_item_id(map_url)
    if item_id:
        all_raw_urls.update(_fetch_agol_item(item_id, timeout))

    # ── Strategy 4: Service catalog enumeration ───────────────────────────
    if not all_raw_urls:
        all_raw_urls.update(_enumerate_service_catalog(base_domain, timeout))

    # ── Score, filter, normalize ──────────────────────────────────────────
    scored = []
    for url in all_raw_urls:
        # Only keep URLs that look like ArcGIS REST layer endpoints
        if not re.search(r'/(FeatureServer|MapServer)(?:/\d+)?(?:/query)?', url, re.IGNORECASE):
            continue
        score = _score_url(url)
        query_url = _normalize_to_query_url(url)
        scored.append((query_url, score))

    # Deduplicate (case-insensitive) and sort by score descending
    seen = set()
    ranked = []
    for url, score in sorted(scored, key=lambda x: x[1], reverse=True):
        key = url.lower()
        if key not in seen:
            seen.add(key)
            ranked.append((url, score))

    # If we have scored candidates, filter to score >= 1; otherwise keep all
    positive = [(u, s) for u, s in ranked if s >= 1]
    candidates = [u for u, s in (positive if positive else ranked)]

    # ── Validate candidates ───────────────────────────────────────────────
    if validate and candidates:
        valid = [u for u in candidates if _validate_candidate(u, timeout)]
        if not valid and candidates:
            warnings.append(
                "Endpoints were found but none responded to a field query. "
                "They may require authentication or be temporarily unavailable."
            )
        candidates = valid if valid else candidates  # fall back to unvalidated if all fail

    # ── Detect non-ArcGIS ─────────────────────────────────────────────────
    non_arcgis = False
    if not candidates and not is_spa and html_text:
        # Check for hints of non-ArcGIS GIS systems
        for indicator in ("mapserver", "mapguide", "geoserver", "qgis", "terragis",
                          "openlayers", "leaflet", "maplibre"):
            if indicator in html_text.lower():
                non_arcgis = True
                break

    # ── Build user-facing warnings ────────────────────────────────────────
    if is_spa:
        warnings.append(
            "The page appears to be a JavaScript app — no ArcGIS URLs were "
            "found in the page source. Try the manual method below."
        )
    elif non_arcgis:
        warnings.append(
            "This map does not appear to use ArcGIS REST "
            "(it may use TerraGIS, GeoServer, QGIS Server, or another platform). "
            "Check if the county publishes a separate ArcGIS Open Data portal."
        )

    return DetectionResult(
        candidates=candidates,
        warnings=warnings,
        is_spa=is_spa,
        non_arcgis=non_arcgis,
    )
