"""
KMZ → County Assessor — Streamlit Web App
==========================================
Upload a KMZ file, preview points/pipeline routes on a map, pull real parcel
owner records from free county assessor GIS services, and download results as
a formatted Excel spreadsheet.

Counties are registered in counties.yaml (Weld County, CO included out of the
box). Use the County Registry panel to add additional counties.
"""

import os
import tempfile
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

from kmz_to_assessor import (
    COLUMNS,
    _flatten_meta,
    lookup_parcels_deduped,
    parse_kmz,
    write_excel,
)
from county_lookup import (
    CountyRegistry,
    fetch_arcgis_fields,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="KMZ → Assessor", page_icon="🗺️", layout="wide")

# ---------------------------------------------------------------------------
# Passphrase gate — blocks all content until the correct password is entered.
# Set app_password in Streamlit secrets (or leave unset to disable the gate).
# ---------------------------------------------------------------------------
_app_password = st.secrets.get("app_password", "")
if _app_password and not st.session_state.get("authenticated"):
    st.title("🔒 KMZ → County Assessor → Excel")
    st.caption("Enter the passphrase to access the app.")
    _pwd = st.text_input("Passphrase", type="password", label_visibility="collapsed")
    if st.button("Submit", type="primary"):
        if _pwd == _app_password:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect passphrase.")
    st.stop()

st.title("🗺️ KMZ → County Assessor → Excel")
st.caption(
    "Upload a KMZ file, preview points or pipeline routes, pull real parcel "
    "owner records from county assessor data, and download as Excel."
)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Settings")

    sample_interval = st.slider(
        "LineString sample interval (°)",
        min_value=0.0,
        max_value=0.01,
        value=0.001,
        step=0.0005,
        format="%.4f",
        help="Spacing between sampled points on pipeline/line features. "
             "0.001° ≈ 100 m. Set to 0 to use every vertex.",
    )

# ---------------------------------------------------------------------------
# County Registry Manager (expandable, in main area)
# ---------------------------------------------------------------------------
registry = CountyRegistry(Path(__file__).parent / "counties.yaml")

with st.expander("🗂️ County Registry — view & add counties", expanded=False):
        entries = registry.all_entries()
        if entries:
            rows_reg = [
                {
                    "Key": k,
                    "Name": v.get("name", ""),
                    "Type": v.get("type", ""),
                    "Assessor Portal": v.get("assessor_portal", ""),
                    "ArcGIS URL": v.get("url", ""),
                }
                for k, v in entries.items()
            ]
            st.dataframe(pd.DataFrame(rows_reg), use_container_width=True, hide_index=True)
        else:
            st.warning("No counties registered yet. Add one below.")

        st.markdown("---")
        st.subheader("➕ Register a New County")
        st.markdown(
            "**How to find a county's ArcGIS URL:**\n"
            "1. Google: `[County Name] county parcel viewer arcgis`\n"
            "2. Open the map → press **F12** → **Network** tab → filter for `FeatureServer`\n"
            "3. Copy the URL ending in `.../FeatureServer/0` (or `/0/query`)\n"
            "4. Paste it below and click **Fetch Fields** to auto-discover field names"
        )

        col1, col2 = st.columns(2)
        with col1:
            new_county = st.text_input("County name", placeholder="Adams")
            new_state  = st.text_input("State name",  placeholder="Colorado")
            new_portal = st.text_input("Assessor portal URL (optional)",
                                       placeholder="https://assessor.example.gov/")
        with col2:
            new_url = st.text_input(
                "ArcGIS FeatureServer URL",
                placeholder="https://services.arcgis.com/.../FeatureServer/0/query",
            )
            new_name = st.text_input("Display name", placeholder="Adams County, CO")

        # Field discovery
        if new_url and st.button("🔍 Fetch Fields from ArcGIS"):
            with st.spinner("Fetching field list…"):
                try:
                    fields = fetch_arcgis_fields(new_url)
                    st.session_state["_fetched_fields"] = fields
                    st.success(f"Found {len(fields)} fields")
                except Exception as e:
                    st.error(f"Could not fetch fields: {e}")

        if "_fetched_fields" in st.session_state:
            fields = st.session_state["_fetched_fields"]
            field_names = ["(not mapped)"] + [f["name"] for f in fields]

            st.markdown("**Map standard fields → ArcGIS field names**")
            STANDARD_FIELDS = [
                ("parcel_id",           "Parcel ID / APN"),
                ("account_no",          "Account Number"),
                ("owner",               "Owner Name"),
                ("mail_address",        "Mailing Address"),
                ("mail_city",           "Mail City"),
                ("mail_state",          "Mail State"),
                ("mail_zip",            "Mail ZIP"),
                ("legal_description",   "Legal Description"),
                ("situs_address",       "Situs Address"),
                ("actual_value",        "Actual Value"),
                ("assessed_value",      "Assessed Value"),
                ("acreage",             "Acreage"),
                ("tax_year",            "Tax Year"),
                ("section_township_range", "Sec/Twp/Range"),
            ]
            field_mapping = {}
            cols = st.columns(2)
            for idx, (std_key, label) in enumerate(STANDARD_FIELDS):
                with cols[idx % 2]:
                    chosen = st.selectbox(label, field_names, key=f"fm_{std_key}")
                    if chosen != "(not mapped)":
                        field_mapping[std_key] = chosen

            if st.button("💾 Save County to Registry"):
                if not new_county or not new_state or not new_url:
                    st.error("County name, state, and ArcGIS URL are required.")
                else:
                    config = {
                        "name": new_name or f"{new_county.title()} County, {new_state.title()}",
                        "type": "arcgis_rest",
                        "url": new_url.strip().rstrip("/"),
                        "assessor_portal": new_portal.strip(),
                        "fields": field_mapping,
                    }
                    if not config["url"].endswith("/query"):
                        config["url"] += "/query"
                    try:
                        registry.register(new_county, new_state, config)
                        st.success(f"Registered {config['name']}!")
                        # Push to GitHub so the change persists after restart
                        from county_lookup import push_counties_to_github
                        pushed = push_counties_to_github(
                            str(Path(__file__).parent / "counties.yaml")
                        )
                        if not pushed:
                            st.caption(
                                "💡 Configure GitHub secrets to make county changes "
                                "permanent across app restarts."
                            )
                        st.session_state.pop("_fetched_fields", None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to save: {e}")

# ---------------------------------------------------------------------------
# File upload
# ---------------------------------------------------------------------------
uploaded = st.file_uploader(
    "Upload a KMZ or KML file",
    type=["kmz", "kml"],
    help="Drag and drop or click to browse.",
)

if uploaded is None:
    st.info("Upload a KMZ file to get started.")
    st.stop()

# ---------------------------------------------------------------------------
# Parse KMZ — cache by file + sample interval
# ---------------------------------------------------------------------------
file_id = f"{uploaded.name}_{uploaded.size}_{sample_interval}"
if st.session_state.get("_file_id") != file_id:
    with tempfile.NamedTemporaryFile(
        suffix=os.path.splitext(uploaded.name)[1], delete=False, mode="wb"
    ) as tmp:
        tmp.write(uploaded.getvalue())
        tmp_path = tmp.name

    try:
        points = parse_kmz(tmp_path, sample_interval_deg=sample_interval)
    except Exception as e:
        st.error(f"Failed to parse file: {e}")
        st.stop()
    finally:
        os.unlink(tmp_path)

    st.session_state["_file_id"] = file_id
    st.session_state["points"] = points
    st.session_state.pop("results", None)
    st.session_state.pop("excel_bytes", None)

points = st.session_state["points"]

if not points:
    st.warning("No Point or LineString placemarks found in the file.")
    st.stop()

# ── Success banner ────────────────────────────────────────────────────────
ls_count = sum(1 for p in points if p.get("geometry_type") == "LineString")
pt_count = sum(1 for p in points if p.get("geometry_type") == "Point")
parts = []
if pt_count:
    parts.append(f"{pt_count} point(s)")
if ls_count:
    parts.append(f"{ls_count} LineString sample(s)")
st.success(f"**{' + '.join(parts)}** extracted from `{uploaded.name}`")

# ---------------------------------------------------------------------------
# Map preview
# ---------------------------------------------------------------------------
st.subheader("📍 Map Preview")

geometry_types = {p.get("geometry_type", "Point") for p in points}
layers = []

if "LineString" in geometry_types:
    path_groups: dict = defaultdict(list)
    for pt in points:
        if pt.get("geometry_type") == "LineString":
            path_groups[pt["name"]].append([pt["lng"], pt["lat"]])

    layers.append(
        pdk.Layer(
            "PathLayer",
            data=[{"name": k, "path": v} for k, v in path_groups.items()],
            get_path="path",
            get_width=6,
            get_color=[255, 140, 0],
            width_min_pixels=2,
            pickable=True,
        )
    )

if "Point" in geometry_types:
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=[p for p in points if p.get("geometry_type") == "Point"],
            get_position=["lng", "lat"],
            get_radius=150,
            get_fill_color=[30, 144, 255],
            pickable=True,
        )
    )

lats = [p["lat"] for p in points]
lngs = [p["lng"] for p in points]
st.pydeck_chart(
    pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=sum(lats) / len(lats),
            longitude=sum(lngs) / len(lngs),
            zoom=11,
        ),
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    ),
    use_container_width=True,
)

with st.expander("Sample point table", expanded=False):
    disp = ["name", "lat", "lng", "geometry_type"]
    if ls_count:
        disp.append("vertex_index")
    st.dataframe(pd.DataFrame(points)[disp], use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Pull records
# ---------------------------------------------------------------------------
st.subheader("🔍 Pull Parcel Records")

pull = st.button(
    "Pull Records",
    type="primary",
    use_container_width=True,
)

if pull:
    progress = st.progress(0, text="Starting…")

    def _cb(i, total, label):
        progress.progress(i / total, text=f"[{i + 1}/{total}] Looking up **{label}**…")

    with st.spinner(""):
        rows_raw = lookup_parcels_deduped(
            points,
            progress_callback=_cb,
            registry=registry,
        )

    rows = [_flatten_meta(r) for r in rows_raw]
    progress.progress(1.0, text="Done!")

    # Build Excel in memory
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False, mode="wb") as tmp:
        tmp_path = tmp.name
    write_excel(rows, tmp_path)
    with open(tmp_path, "rb") as f:
        excel_bytes = f.read()
    os.unlink(tmp_path)

    st.session_state["results"]     = rows
    st.session_state["excel_bytes"] = excel_bytes

    found          = sum(1 for r in rows if r.get("parcel_id"))
    errors         = sum(1 for r in rows if "ERROR" in str(r.get("status", "")))
    unique_queried = sum(1 for r in rows if not r.get("deduped") and r.get("parcel_id"))
    deduped_count  = sum(1 for r in rows if r.get("deduped"))
    unregistered   = sum(1 for r in rows if "No county registered" in str(r.get("status", "")))

    cols = st.columns(4)
    cols[0].metric("Samples processed",       len(rows))
    cols[1].metric("Unique parcels queried",  unique_queried)
    cols[2].metric("Records found",           found)
    cols[3].metric("Errors",                  errors)

    if deduped_count:
        st.info(
            f"**{deduped_count}** duplicate sample(s) collapsed — "
            f"those points fell on the same parcel as a nearby sample, "
            f"so no extra query was made."
        )
    if unregistered:
        st.warning(
            f"**{unregistered}** sample(s) fell in a county not yet in the registry. "
            f"Expand **County Registry** above to add it."
        )

# ---------------------------------------------------------------------------
# Results table + download
# ---------------------------------------------------------------------------
if "results" in st.session_state:
    st.subheader("📋 Results")

    results_df = pd.DataFrame(st.session_state["results"])
    display_cols = [key for _, key, _ in COLUMNS if key in results_df.columns]
    st.dataframe(results_df[display_cols], use_container_width=True, hide_index=True)

    base_name = os.path.splitext(uploaded.name)[0]
    st.download_button(
        "⬇️  Download Excel",
        data=st.session_state["excel_bytes"],
        file_name=f"{base_name}_parcels.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )
