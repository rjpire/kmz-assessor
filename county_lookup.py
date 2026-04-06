"""
County GIS Lookup
=================
Free parcel owner lookup using county-published ArcGIS REST Feature Services.

Flow:
  lat/lng  →  Nominatim reverse geocoder  →  county + state name
           →  CountyRegistry (counties.yaml)  →  ArcGIS REST endpoint
           →  parcel attributes  →  normalized output dict

Nominatim rate limit: 1 request/second (OSM usage policy).
A coarse spatial cache (0.5° grid ≈ ~50 km) prevents re-geocoding points
that are obviously in the same county (e.g. all points along a pipeline).
"""

import json
import time
from pathlib import Path

import requests
import yaml

# ---------------------------------------------------------------------------
# Nominatim reverse geocoding
# ---------------------------------------------------------------------------

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
_NOMINATIM_MIN_INTERVAL = 1.1  # seconds between calls (OSM policy: max 1/sec)
_last_nominatim_t: float = 0.0


def _nominatim_throttle():
    global _last_nominatim_t
    wait = _NOMINATIM_MIN_INTERVAL - (time.time() - _last_nominatim_t)
    if wait > 0:
        time.sleep(wait)
    _last_nominatim_t = time.time()


def geocode_county(lat: float, lng: float) -> tuple:
    """
    Reverse-geocode a lat/lng to (county, state) strings via Nominatim.

    Returns lowercase strings with "county" removed, e.g.:
        ("weld", "colorado")
        ("adams", "colorado")

    Raises requests.RequestException on network failure.
    """
    _nominatim_throttle()
    resp = requests.get(
        _NOMINATIM_URL,
        params={"lat": lat, "lon": lng, "format": "json", "addressdetails": 1},
        headers={"User-Agent": "KMZ-Assessor-Tool/1.0"},
        timeout=12,
    )
    resp.raise_for_status()
    addr = resp.json().get("address", {})
    county_raw = addr.get("county", "")
    state_raw = addr.get("state", "")
    # Strip " County", " Parish", " Borough" suffixes and lowercase
    county = county_raw.lower()
    for suffix in (" county", " parish", " borough", " municipality"):
        county = county.replace(suffix, "")
    county = county.strip()
    state = state_raw.lower().strip()
    return county, state


# County detection cache keyed by 0.5° grid cell (≈ 50 km).
# Pipelines and right-of-way files typically have all points in the same
# county, so this collapses ~100 queries down to 1-2 Nominatim calls.
_county_cache: dict = {}


def detect_county_cached(lat: float, lng: float) -> tuple:
    """
    Return (county, state) with a coarse spatial cache to limit Nominatim calls.
    """
    key = (round(lat * 2) / 2, round(lng * 2) / 2)
    if key not in _county_cache:
        _county_cache[key] = geocode_county(lat, lng)
    return _county_cache[key]


def clear_county_cache():
    """Clear the in-memory county detection cache."""
    _county_cache.clear()


# ---------------------------------------------------------------------------
# County Registry
# ---------------------------------------------------------------------------

def _make_key(county: str, state: str) -> str:
    """Normalize county+state into a YAML key, e.g. 'weld_colorado'."""
    c = county.lower().strip().replace(" ", "_")
    s = state.lower().strip().replace(" ", "_")
    return f"{c}_{s}"


class CountyRegistry:
    """
    Loads counties.yaml and provides lookup by (county, state).

    Usage:
        registry = CountyRegistry()
        config = registry.get("weld", "colorado")
        if config:
            result = lookup_parcel_arcgis(lat, lng, config)
    """

    def __init__(self, yaml_path: str = None):
        if yaml_path is None:
            yaml_path = Path(__file__).parent / "counties.yaml"
        self._yaml_path = Path(yaml_path)
        self._reload()

    def _reload(self):
        with open(self._yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self._counties: dict = data.get("counties", {})

    def get(self, county: str, state: str) -> dict:
        """Return county config dict, or None if not registered."""
        key = _make_key(county, state)
        return self._counties.get(key)

    def all_entries(self) -> dict:
        """Return the full {key: config} dict."""
        return dict(self._counties)

    def register(self, county: str, state: str, config: dict):
        """
        Add or update a county entry and persist to counties.yaml.

        config must include at minimum: name, type, url, fields (dict).
        """
        key = _make_key(county, state)
        self._counties[key] = config

        # Round-trip through yaml.safe_load to preserve file comments isn't
        # possible cleanly, so we write only the counties block cleanly.
        # The full file is rewritten; header comments are restored from a
        # stored template or omitted on first programmatic write.
        with open(self._yaml_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        raw.setdefault("counties", {})[key] = config

        with open(self._yaml_path, "w", encoding="utf-8") as f:
            yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        self._reload()

    def key_for(self, county: str, state: str) -> str:
        return _make_key(county, state)


# ---------------------------------------------------------------------------
# ArcGIS REST parcel query
# ---------------------------------------------------------------------------

def lookup_parcel_arcgis(lat: float, lng: float, county_config: dict) -> dict:
    """
    Query an ArcGIS REST Feature Service for the parcel containing (lat, lng).

    county_config must have:
        url    — full FeatureServer layer query URL (.../FeatureServer/0/query)
        fields — mapping of standard key → raw ArcGIS field name

    Returns a normalized dict with standard keys (parcel_id, owner, etc.),
    or {} if no parcel was found.
    """
    url = county_config.get("url", "")
    if not url:
        raise ValueError("County config missing 'url'")

    field_map: dict = county_config.get("fields", {})

    params = {
        "geometry": json.dumps({"x": lng, "y": lat}),
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "returnGeometry": "false",
        "resultRecordCount": 1,
        "f": "json",
    }

    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error {data['error'].get('code')}: {data['error'].get('message')}")

    features = data.get("features", [])
    if not features:
        return {}

    raw: dict = features[0].get("attributes", {})

    # Map raw field names → standard output keys
    result = {}
    for std_key, raw_key in field_map.items():
        val = raw.get(raw_key, "")
        # Convert None → ""
        result[std_key] = val if val is not None else ""

    # Ensure parcel_id is always set (fall back to common raw field names)
    if not result.get("parcel_id"):
        for candidate in ("PARCEL", "APN", "PARCELNUMBER", "PARCELNO", "PIN"):
            if raw.get(candidate):
                result["parcel_id"] = raw[candidate]
                break

    # Attach the county portal URL if configured
    portal = county_config.get("assessor_portal", "")
    result["assessor_portal"] = portal

    # Include the county name for transparency
    result["data_source"] = county_config.get("name", "County GIS")

    return result


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def lookup_parcel_county_gis(
    lat: float,
    lng: float,
    registry: CountyRegistry,
) -> dict:
    """
    Determine the county from (lat, lng) then query its registered GIS endpoint.

    Returns normalized parcel dict, or {} if no county is registered or
    no parcel found.  Raises on network errors.
    """
    county, state = detect_county_cached(lat, lng)
    config = registry.get(county, state)

    if config is None:
        return {
            "_unregistered_county": f"{county.title()} County, {state.title()}",
        }

    endpoint_type = config.get("type", "arcgis_rest")

    if endpoint_type == "arcgis_rest":
        return lookup_parcel_arcgis(lat, lng, config)

    raise NotImplementedError(f"County endpoint type '{endpoint_type}' not yet supported.")


# ---------------------------------------------------------------------------
# Field discovery helper (used by the Streamlit "register county" form)
# ---------------------------------------------------------------------------

def fetch_arcgis_fields(url: str) -> list:
    """
    Fetch the field list from an ArcGIS FeatureServer layer.

    url should be the layer root (without /query), e.g.:
        https://services.arcgis.com/.../FeatureServer/0

    Returns list of {name, type, alias} dicts.
    """
    # Strip trailing /query if user pasted the full query URL
    base_url = url.rstrip("/")
    if base_url.endswith("/query"):
        base_url = base_url[:-6]

    resp = requests.get(base_url, params={"f": "json"}, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error'].get('message')}")

    return [
        {"name": f["name"], "type": f["type"], "alias": f.get("alias", f["name"])}
        for f in data.get("fields", [])
    ]


def push_counties_to_github(yaml_path: str) -> bool:
    """
    Commit the updated counties.yaml back to GitHub so changes survive app restarts.

    Requires Streamlit secrets:
        app_password = "..."
        [github]
        token  = "ghp_..."   # Personal Access Token with repo write scope
        repo   = "yourname/kmz-assessor"
        branch = "main"      # optional, defaults to "main"

    Returns True on success, False if secrets not configured or on any error.
    The caller should treat False as a graceful degradation (local write already happened).
    """
    try:
        import streamlit as st
        from github import Github

        token  = st.secrets["github"]["token"]
        repo   = st.secrets["github"]["repo"]
        branch = st.secrets["github"].get("branch", "main")

        g = Github(token)
        r = g.get_repo(repo)
        with open(yaml_path, "r", encoding="utf-8") as fh:
            content = fh.read()
        existing = r.get_contents("counties.yaml", ref=branch)
        r.update_file(
            path="counties.yaml",
            message="chore: add/update county via app UI",
            content=content,
            sha=existing.sha,
            branch=branch,
        )
        return True
    except Exception:
        return False  # silently degrade — local write already happened
