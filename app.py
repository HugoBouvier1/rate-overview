"""
=========================================================================
RATES & MACRO DESK - Multi-source (FRED / ECB / Bundesbank / US Treasury)
=========================================================================

DATA SOURCES AND API KEYS
-------------------------
  FRED           -> API KEY REQUIRED (free, issued instantly)
                    https://fredaccount.stlouisfed.org/apikeys
  ECB Data Portal-> NO KEY
  Bundesbank     -> NO KEY
  US Treasury    -> NO KEY

=> ONLY ONE KEY TO CONFIGURE: FRED.

SETUP - nothing to edit inside this file.
  1. Create a folder named  .streamlit  next to app.py
  2. Inside it create  secrets.toml  containing:
         FRED_API_KEY = "your_key_here"

     Rate_overview\
         app.py
         .streamlit\
             secrets.toml

The app runs without any key: FRED series will be empty, but all
ECB / Bundesbank / US Treasury series still load normally.

REQUIREMENTS : pip install streamlit pandas plotly requests
RUN          : streamlit run app.py
=========================================================================
"""

import io
import time
import concurrent.futures
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

st.set_page_config(page_title="Rates Desk", layout="wide",
                   initial_sidebar_state="expanded")


# =========================================================================
# API KEY - read from .streamlit/secrets.toml
#
# Nothing to edit in this file. Put your key in:
#     .streamlit/secrets.toml   ->   FRED_API_KEY = "your_key"
#
# The try/except is required: when secrets.toml is missing, Streamlit
# raises StreamlitSecretNotFoundError instead of returning a default,
# which would crash the app on startup.
# =========================================================================

try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
except Exception:
    FRED_API_KEY = ""

HAS_FRED_KEY = bool(FRED_API_KEY.strip())

# ECB / Bundesbank / US Treasury need no key.
# =========================================================================


HISTORY_DAYS = 3700
HTTP_TIMEOUT = 60   # the ECB YC dataflow regularly takes 40s+

PERIODS = {"1W": 7, "1M": 30, "3M": 90, "6M": 180,
           "YTD": None, "1Y": 365, "3Y": 1095, "5Y": 1825, "10Y": 3650}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "rates-desk/2.1"})


def _start_date() -> str:
    return (datetime.now() - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")


def _tidy(df: pd.DataFrame) -> pd.DataFrame:
    """Single output schema for every connector: index=date, column=value."""
    if df.empty:
        return df
    df = df.dropna().sort_index()
    return df[~df.index.duplicated(keep="last")]


# =========================================================================
# CONNECTORS
# =========================================================================

def fetch_fred(series_id: str, units: str = "lin") -> pd.DataFrame:
    """FRED - requires FRED_API_KEY."""
    if not HAS_FRED_KEY:
        raise RuntimeError(
            "FRED_API_KEY not found in .streamlit/secrets.toml")
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
           f"&observation_start={_start_date()}&units={units}")
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    if not obs:
        return pd.DataFrame()
    df = pd.DataFrame(obs)[["date", "value"]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return _tidy(df.set_index("date")[["value"]])


def _ecb_raw(flow: str, key: str) -> pd.DataFrame:
    """One HTTP call to the ECB portal. Returns the raw SDMX-CSV frame."""
    url = (f"https://data-api.ecb.europa.eu/service/data/{flow}/{key}"
           f"?startPeriod={_start_date()}&format=csvdata&detail=dataonly")
    last = None
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return pd.read_csv(io.StringIO(r.text))
        except (requests.Timeout, requests.ConnectionError) as e:
            last = e
            time.sleep(1.5 * (attempt + 1))   # back off, the YC flow is slow
        except Exception:
            raise
    raise last


@st.cache_data(ttl=3600, show_spinner=False, persist="disk")
def _ecb_group(flow: str, key: str) -> dict:
    """Fetch a MULTI-series key (dimension values joined by '+') in ONE
    request, then split the response into one frame per series.

    The ECB API accepts e.g. SR_1Y+SR_2Y+SR_10Y on a single dimension.
    Eight separate calls to the YC dataflow become one, which is where
    most of the loading time was going.
    """
    df = _ecb_raw(flow, key)
    if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        return {}
    id_col = "KEY" if "KEY" in df.columns else None
    if id_col is None:
        # Single series response: rebuild its full key.
        out = df[["TIME_PERIOD", "OBS_VALUE"]].copy()
        out.columns = ["date", "value"]
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out["value"] = pd.to_numeric(out["value"], errors="coerce")
        return {f"{flow}.{key}": _tidy(out.set_index("date")[["value"]])}

    res = {}
    sub = df[[id_col, "TIME_PERIOD", "OBS_VALUE"]].copy()
    sub.columns = ["skey", "date", "value"]
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
    sub["value"] = pd.to_numeric(sub["value"], errors="coerce")
    for skey, part in sub.groupby("skey"):
        res[str(skey)] = _tidy(part.set_index("date")[["value"]])
    return res


# Series that differ on a single dimension are fetched together.
# Format: "FLOW.multi.key.with+values" -> filled at import time below.
ECB_GROUPS = {}


def _register_ecb_groups(instruments: dict) -> None:
    """Detect ECB series that differ on exactly one dimension and build
    a combined key for each family."""
    from collections import defaultdict
    buckets = defaultdict(list)
    for lst in instruments.values():
        for spec in lst:
            if spec["src"] != "ecb":
                continue
            parts = spec["id"].split(".")
            for k in range(len(parts)):
                stem = tuple(parts[:k] + ["*"] + parts[k + 1:])
                buckets[(k, stem)].append(spec["id"])
    used = set()
    for (k, stem), ids in sorted(buckets.items(), key=lambda t: -len(t[1])):
        ids = [i for i in ids if i not in used]
        if len(ids) < 2:
            continue
        vals = [i.split(".")[k] for i in ids]
        parts = list(stem)
        parts[k] = "+".join(vals)
        combined = ".".join(parts)
        flow, key = combined.split(".", 1)
        for i in ids:
            ECB_GROUPS[i] = (flow, key)
            used.add(i)


def fetch_ecb(series_key: str) -> pd.DataFrame:
    """ECB Data Portal - no key.

    If the series belongs to a registered group, the whole group is
    fetched in one shared, cached request and this call just picks its
    own slice out of it.
    """
    if series_key in ECB_GROUPS:
        flow, gkey = ECB_GROUPS[series_key]
        group = _ecb_group(flow, gkey)
        if series_key in group:
            return group[series_key]
        # Fall back to the exact key match by suffix.
        tail = series_key.split(".", 1)[1]
        for k, v in group.items():
            if k.endswith(tail):
                return v
        return pd.DataFrame()

    flow, key = series_key.split(".", 1)
    got = _ecb_group(flow, key)
    if not got:
        return pd.DataFrame()
    if series_key in got:
        return got[series_key]
    return next(iter(got.values()))


def fetch_bundesbank(series_key: str) -> pd.DataFrame:
    """Bundesbank SDMX web service - no key. Returns SDMX-CSV (comma sep).

    Expected columns include TIME_PERIOD and OBS_VALUE, same convention
    as the ECB portal. Older docs mention a ';'-separated export from the
    legacy time-series database; that one is being retired, so we parse
    the SDMX-CSV shape and fall back to positional columns if needed.
    """
    flow, key = series_key.split("/", 1)
    url = (f"https://api.statistiken.bundesbank.de/rest/data/{flow}/{key}"
           f"?startPeriod={_start_date()}&format=csv")
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    txt = r.text
    # Detect separator: SDMX-CSV uses ',', legacy export uses ';'
    head = txt.split("\n", 1)[0]
    sep = ";" if head.count(";") > head.count(",") else ","
    df = pd.read_csv(io.StringIO(txt), sep=sep, engine="python",
                     on_bad_lines="skip")
    if "TIME_PERIOD" in df.columns and "OBS_VALUE" in df.columns:
        df = df[["TIME_PERIOD", "OBS_VALUE"]]
    elif df.shape[1] >= 2:
        df = df.iloc[:, :2]
    else:
        return pd.DataFrame()
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(
        df["value"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce")
    return _tidy(df.set_index("date")[["value"]])


def fetch_boe(series_code: str) -> pd.DataFrame:
    """Bank of England IADB - no key. Public CSV endpoint.

    Not an officially documented API: stable for years and widely used,
    but the BoE makes no compatibility promise.
    """
    frm = (datetime.now() - timedelta(days=HISTORY_DAYS)).strftime("%d/%b/%Y")
    to = datetime.now().strftime("%d/%b/%Y")
    url = ("https://www.bankofengland.co.uk/boeapps/database/"
           "_iadb-fromshowcolumns.asp?csv.x=yes"
           f"&Datefrom={frm}&Dateto={to}&SeriesCodes={series_code}"
           "&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N")
    r = SESSION.get(url, timeout=HTTP_TIMEOUT,
                    headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    if df.shape[1] < 2:
        return pd.DataFrame()
    df = df.iloc[:, :2]
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return _tidy(df.set_index("date")[["value"]])


_UST_NS = {"a": "http://www.w3.org/2005/Atom",
           "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
           "d": "http://schemas.microsoft.com/ado/2007/08/dataservices"}


@st.cache_data(ttl=3600, show_spinner=False, persist="disk")
def _ust_year(year: int) -> pd.DataFrame:
    """Fetch ONE year of the full Treasury par yield curve.

    Cached per year, so the 8 UST maturities share a single download
    instead of re-fetching the same XML once per maturity.
    """
    url = ("https://home.treasury.gov/resource-center/data-chart-center/"
           "interest-rates/pages/xml?data=daily_treasury_yield_curve"
           f"&field_tdr_date_value={year}")
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    rows = []
    for entry in root.findall(".//a:entry", _UST_NS):
        props = entry.find(".//m:properties", _UST_NS)
        if props is None:
            continue
        d = props.find("d:NEW_DATE", _UST_NS)
        if d is None or not d.text:
            continue
        rec = {"date": d.text}
        for child in props:
            tag = child.tag.split("}")[-1]
            if tag.startswith("BC_"):
                rec[tag] = child.text
        rows.append(rec)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date"]).set_index("date").sort_index()


@st.cache_data(ttl=3600, show_spinner=False, persist="disk")
def _ust_all() -> pd.DataFrame:
    """All years of the curve, fetched CONCURRENTLY and concatenated."""
    this_year = datetime.now().year
    years = list(range(max(this_year - 10, 1990), this_year + 1))
    frames = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=11) as ex:
        futs = {ex.submit(_ust_year, y): y for y in years}
        for f in concurrent.futures.as_completed(futs):
            try:
                d = f.result()
                if not d.empty:
                    frames.append(d)
            except Exception:
                continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames)
    return out[~out.index.duplicated(keep="last")].sort_index()


def fetch_ust(maturity: str) -> pd.DataFrame:
    """One maturity column out of the shared, cached curve."""
    allc = _ust_all()
    if allc.empty or maturity not in allc.columns:
        return pd.DataFrame()
    df = allc[[maturity]].copy()
    df.columns = ["value"]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return _tidy(df)


FETCHERS = {
    "fred": fetch_fred,
    "ecb": fetch_ecb,
    "buba": fetch_bundesbank,
    "boe": fetch_boe,
    "ust": fetch_ust,
}

SOURCE_LABEL = {
    "fred": "FRED",
    "ecb": "ECB",
    "buba": "Bundesbank",
    "boe": "Bank of England",
    "ust": "US Treasury",
}


# =========================================================================
# INSTRUMENT UNIVERSE
#   kind : "rate"   -> level in %, change shown in bps
#          "spread" -> already a differential in %, change in bps
#          "index"  -> year-on-year rate, change in percentage points
# =========================================================================

INSTRUMENTS = {
    "US - Money Market & Policy": [
        {"name": "Fed Funds (EFFR)", "src": "fred", "id": "EFFR", "kind": "rate"},
        {"name": "SOFR", "src": "fred", "id": "SOFR", "kind": "rate"},
        {"name": "IORB", "src": "fred", "id": "IORB", "kind": "rate"},
        {"name": "OBFR", "src": "fred", "id": "OBFR", "kind": "rate"},
    ],
    "US - Sovereign Curve (official Treasury)": [
        {"name": "UST 1M", "src": "ust", "id": "BC_1MONTH", "kind": "rate"},
        {"name": "UST 3M", "src": "ust", "id": "BC_3MONTH", "kind": "rate"},
        {"name": "UST 6M", "src": "ust", "id": "BC_6MONTH", "kind": "rate"},
        {"name": "UST 1Y", "src": "ust", "id": "BC_1YEAR", "kind": "rate"},
        {"name": "UST 2Y", "src": "ust", "id": "BC_2YEAR", "kind": "rate"},
        {"name": "UST 5Y", "src": "ust", "id": "BC_5YEAR", "kind": "rate"},
        {"name": "UST 10Y", "src": "ust", "id": "BC_10YEAR", "kind": "rate"},
        {"name": "UST 30Y", "src": "ust", "id": "BC_30YEAR", "kind": "rate"},
    ],
    "US - Real Yields, Breakevens & Credit": [
        {"name": "TIPS 10Y (real)", "src": "fred", "id": "DFII10", "kind": "rate"},
        {"name": "TIPS 5Y (real)", "src": "fred", "id": "DFII5", "kind": "rate"},
        {"name": "Breakeven 10Y", "src": "fred", "id": "T10YIE", "kind": "rate"},
        {"name": "Breakeven 5Y", "src": "fred", "id": "T5YIE", "kind": "rate"},
        {"name": "IG OAS (ICE BofA)", "src": "fred", "id": "BAMLC0A0CM", "kind": "spread"},
        {"name": "HY OAS (ICE BofA)", "src": "fred", "id": "BAMLH0A0HYM2", "kind": "spread"},
    ],
    "Euro Area - Money Market": [
        {"name": "ECB Depo Facility", "src": "fred", "id": "ECBDFR", "kind": "rate"},
        {"name": "ESTR", "src": "ecb", "id": "EST.B.EU000A2X2A25.WT", "kind": "rate"},
        {"name": "Euribor 1M (monthly)", "src": "ecb", "id": "FM.M.U2.EUR.RT.MM.EURIBOR1MD_.HSTA", "kind": "rate"},
        {"name": "Euribor 3M (monthly)", "src": "ecb", "id": "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", "kind": "rate"},
        {"name": "Euribor 6M (monthly)", "src": "ecb", "id": "FM.M.U2.EUR.RT.MM.EURIBOR6MD_.HSTA", "kind": "rate"},
        {"name": "Euribor 12M (monthly)", "src": "ecb", "id": "FM.M.U2.EUR.RT.MM.EURIBOR1YD_.HSTA", "kind": "rate"},
    ],
    "Euro Area - AAA Yield Curve (ECB)": [
        {"name": "AAA 3M", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M", "kind": "rate"},
        {"name": "AAA 6M", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_6M", "kind": "rate"},
        {"name": "AAA 1Y", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y", "kind": "rate"},
        {"name": "AAA 2Y", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y", "kind": "rate"},
        {"name": "AAA 5Y", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y", "kind": "rate"},
        {"name": "AAA 10Y", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y", "kind": "rate"},
        {"name": "AAA 20Y", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_20Y", "kind": "rate"},
        {"name": "AAA 30Y", "src": "ecb", "id": "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y", "kind": "rate"},
    ],
    "Euro Area - National Sovereigns": [
        {"name": "Bund 2Y", "src": "buba", "id": "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.R02XX.R.A.A._Z._Z.A", "kind": "rate"},
        {"name": "Bund 5Y", "src": "buba", "id": "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.R05XX.R.A.A._Z._Z.A", "kind": "rate"},
        {"name": "Bund 10Y", "src": "buba", "id": "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A", "kind": "rate"},
        {"name": "Bund 30Y", "src": "buba", "id": "BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.R30XX.R.A.A._Z._Z.A", "kind": "rate"},
        # No free DAILY source for these since the Banque de France
        # connector was dropped. ECB "long-term rate for convergence
        # purposes" is monthly - the ".M." in the key marks that.
        {"name": "OAT 10Y FR (monthly)", "src": "ecb",
         "id": "IRS.M.FR.L.L40.CI.0000.EUR.N.Z", "kind": "rate"},
        {"name": "BTP 10Y IT (monthly)", "src": "ecb",
         "id": "IRS.M.IT.L.L40.CI.0000.EUR.N.Z", "kind": "rate"},
        {"name": "Bonos 10Y ES (monthly)", "src": "ecb",
         "id": "IRS.M.ES.L.L40.CI.0000.EUR.N.Z", "kind": "rate"},
        {"name": "PGB 10Y PT (monthly)", "src": "ecb",
         "id": "IRS.M.PT.L.L40.CI.0000.EUR.N.Z", "kind": "rate"},
    ],
    "United Kingdom": [
        {"name": "BoE Bank Rate", "src": "boe", "id": "IUDBEDR", "kind": "rate"},
        {"name": "SONIA", "src": "boe", "id": "IUDSOIA", "kind": "rate"},
        {"name": "Gilt 5Y", "src": "boe", "id": "IUDSNPY", "kind": "rate"},
        {"name": "Gilt 10Y", "src": "boe", "id": "IUDMNPY", "kind": "rate"},
        {"name": "Gilt 20Y", "src": "boe", "id": "IUDLNPY", "kind": "rate"},
    ],
    "Asia - Japan & China": [
        {"name": "JGB 10Y (monthly)", "src": "fred", "id": "IRLTLT01JPM156N", "kind": "rate"},
        {"name": "Japan 3M rate (monthly)", "src": "fred", "id": "IR3TIB01JPM156N", "kind": "rate"},
        {"name": "China 3M T-bill (monthly)", "src": "fred", "id": "IR3TTS01CNM156N", "kind": "rate"},
        {"name": "Japan CPI YoY", "src": "fred", "id": "JPNCPIALLMINMEI", "kind": "index", "units": "pc1"},
        {"name": "China CPI YoY", "src": "fred", "id": "CHNCPIALLMINMEI", "kind": "index", "units": "pc1"},
    ],
    "Inflation": [
        {"name": "US CPI YoY", "src": "fred", "id": "CPIAUCSL", "kind": "index", "units": "pc1"},
        {"name": "US Core CPI YoY", "src": "fred", "id": "CPILFESL", "kind": "index", "units": "pc1"},
        {"name": "US PCE YoY", "src": "fred", "id": "PCEPI", "kind": "index", "units": "pc1"},
        {"name": "US Core PCE YoY", "src": "fred", "id": "PCEPILFE", "kind": "index", "units": "pc1"},
        {"name": "EA HICP YoY", "src": "fred", "id": "CP0000EZ19M086NEST", "kind": "index", "units": "pc1"},
        {"name": "UK CPI YoY", "src": "fred", "id": "GBRCPIALLMINMEI", "kind": "index", "units": "pc1"},
        {"name": "Japan CPI YoY ", "src": "fred", "id": "JPNCPIALLMINMEI", "kind": "index", "units": "pc1"},
    ],
}

# Build the ECB multi-series groups now that INSTRUMENTS exists.
_register_ecb_groups(INSTRUMENTS)

# Spreads computed from the series above (inner join on dates).
DERIVED = [
    {"name": "UST 10Y-2Y", "section": "US - Sovereign Curve (official Treasury)",
     "long": "UST 10Y", "short": "UST 2Y"},
    {"name": "UST 10Y-3M", "section": "US - Sovereign Curve (official Treasury)",
     "long": "UST 10Y", "short": "UST 3M"},
    {"name": "UST 30Y-10Y", "section": "US - Sovereign Curve (official Treasury)",
     "long": "UST 30Y", "short": "UST 10Y"},
    {"name": "Euribor 3M - ESTR", "section": "Euro Area - Money Market",
     "long": "Euribor 3M (monthly)", "short": "ESTR"},
    {"name": "AAA 10Y-2Y", "section": "Euro Area - AAA Yield Curve (ECB)",
     "long": "AAA 10Y", "short": "AAA 2Y"},
    {"name": "Bund 10Y-2Y", "section": "Euro Area - National Sovereigns",
     "long": "Bund 10Y", "short": "Bund 2Y"},
    # Inner join: a spread inherits the LOWER frequency of its two legs,
    # so every spread against a "(monthly)" leg is MONTHLY too.
    {"name": "OAT-Bund 10Y (monthly)", "section": "Euro Area - National Sovereigns",
     "long": "OAT 10Y FR (monthly)", "short": "Bund 10Y"},
    {"name": "BTP-Bund 10Y (monthly)", "section": "Euro Area - National Sovereigns",
     "long": "BTP 10Y IT (monthly)", "short": "Bund 10Y"},
    {"name": "Bonos-Bund 10Y (monthly)", "section": "Euro Area - National Sovereigns",
     "long": "Bonos 10Y ES (monthly)", "short": "Bund 10Y"},
    {"name": "Gilt 20Y-5Y", "section": "United Kingdom",
     "long": "Gilt 20Y", "short": "Gilt 5Y"},
]


# =========================================================================
# LOADING
# =========================================================================

def _fetch_one(spec: dict):
    t0 = time.time()
    try:
        fn = FETCHERS[spec["src"]]
        if spec["src"] == "fred" and "units" in spec:
            df = fn(spec["id"], spec["units"])
        else:
            df = fn(spec["id"])
        return spec["name"], df, None, round(time.time() - t0, 2)
    except Exception as e:
        msg = str(e)
        if HAS_FRED_KEY and FRED_API_KEY in msg:
            msg = msg.replace(FRED_API_KEY, "***REDACTED***")
        return (spec["name"], pd.DataFrame(), f"{type(e).__name__}: {msg}",
                round(time.time() - t0, 2))


@st.cache_data(ttl=3600, show_spinner=False, persist="disk")
def load_section(section: str):
    specs = INSTRUMENTS[section]
    data, errors, timings = {}, {}, {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for name, df, err, dt in ex.map(_fetch_one, specs):
            data[name] = df
            timings[name] = dt
            if err:
                errors[name] = err
    return data, errors, timings


def build_derived(pool: dict, section: str) -> dict:
    out = {}
    for d in DERIVED:
        if d["section"] != section:
            continue
        a, b = pool.get(d["long"]), pool.get(d["short"])
        if a is None or b is None or a.empty or b.empty:
            continue
        j = a.join(b, lsuffix="_a", rsuffix="_b", how="inner").dropna()
        if j.empty:
            continue
        out[d["name"]] = pd.DataFrame({"value": j["value_a"] - j["value_b"]})
    return out


# =========================================================================
# USER INTERFACE
# =========================================================================

st.markdown("""
<style>
  .block-container { padding-top: 2.2rem; }
  .stDataFrame { font-size: 0.87rem; }
  .src-note { color:#7a7f87; font-size:0.78rem; margin:-0.5rem 0 0.6rem 0; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.header("Settings")
    period = st.selectbox("Period", list(PERIODS.keys()), index=3)

    st.divider()
    st.caption("**Regions shown**")
    regions = st.multiselect(
        "Regions", list(INSTRUMENTS.keys()),
        default=list(INSTRUMENTS.keys()),
        label_visibility="collapsed")

    st.divider()
    show_diag = st.checkbox("Source diagnostics", value=False)
    if st.button("Clear cache"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.caption("**API key status**")
    st.caption(("OK  " if HAS_FRED_KEY
                else "MISSING  ") + "FRED (only key required)")
    st.caption("OK   ECB / Bundesbank / BoE / US Treasury (no key)")

cutoff = (datetime(datetime.now().year, 1, 1) if period == "YTD"
          else datetime.now() - timedelta(days=PERIODS[period]))

st.title("Rates & Macro Desk")
st.markdown('<div class="src-note">Sources: FRED - ECB Data Portal - '
            'Deutsche Bundesbank - Bank of England - US Treasury. '
            'End-of-day data; each row is timestamped with its own '
            'last observation.</div>',
            unsafe_allow_html=True)

if not HAS_FRED_KEY:
    st.warning(
        "No FRED_API_KEY found, so FRED series will be empty. "
        "Create .streamlit/secrets.toml next to app.py containing "
        'FRED_API_KEY = "your_key". Free key at '
        "https://fredaccount.stlouisfed.org/apikeys")

if "sel" not in st.session_state:
    st.session_state.sel = None
if "tbl_ver" not in st.session_state:
    st.session_state.tbl_ver = {}

POOL, ERRORS, TIMINGS = {}, {}, {}

# If the selected row belongs to a section the user just hid, the detail
# panel would have nowhere to render. Drop the selection instead.
if st.session_state.sel and st.session_state.sel[0] not in regions:
    st.session_state.sel = None


def render_deep_dive(inst: str, df: pd.DataFrame) -> None:
    """Draw the detail panel for one instrument.

    Called inline, right under the table the row was picked from, so the
    chart appears next to the click instead of at the bottom of a nine
    section page.
    """
    d = df[df.index >= cutoff]
    if len(d) < 2:
        st.info("Not enough observations over the selected period.")
        return

    cur, start = d["value"].iloc[-1], d["value"].iloc[0]
    delta = cur - start
    up = delta >= 0
    color = "#26a69a" if up else "#ef5350"
    rgb = "38,166,154" if up else "239,83,80"

    with st.container(border=True):
        head, close = st.columns([6, 1])
        head.markdown(f"**{inst}**")
        if close.button("Close", key=f"close::{inst}"):
            st.session_state.sel = None
            st.rerun()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Level", f"{cur:.3f} %", f"{delta * 100:+.1f} bps")
        c2.metric("Period high", f"{d['value'].max():.3f} %")
        c3.metric("Period low", f"{d['value'].min():.3f} %")
        c4.metric("Vol (ann. sigma)",
                  f"{d['value'].diff().std() * (252 ** 0.5) * 100:.0f} bps")

        # Auto-scaled y-range so small moves are readable. A 20bps swing
        # must not look flat on a 0-4% axis. Pad by ~8% of the range, or
        # a 5bps floor when the series is nearly flat, and fill down to
        # the bottom of the plotted range rather than to zero.
        lo, hi = float(d["value"].min()), float(d["value"].max())
        span = hi - lo
        pad = max(span * 0.08, 0.05)
        y0, y1 = lo - pad, hi + pad

        fig = go.Figure()
        # Invisible baseline at the bottom of the visible range, then fill
        # the value trace down to it ('tonexty'). Cleaner than 'tozeroy'
        # once the axis no longer starts at zero.
        fig.add_trace(go.Scatter(
            x=d.index, y=[y0] * len(d), mode="lines",
            line=dict(width=0), hoverinfo="skip", showlegend=False,
        ))
        fig.add_trace(go.Scatter(
            x=d.index, y=d["value"], mode="lines",
            line=dict(color=color, width=2),
            fill="tonexty", fillcolor=f"rgba({rgb},0.12)",
            hovertemplate="%{x|%d/%m/%Y}<br><b>%{y:.3f} %</b><extra></extra>",
        ))
        fig.add_hline(y=start, line_dash="dot", line_color="#888",
                      annotation_text=f"Period start: {start:.3f} %",
                      annotation_position="top left")
        fig.update_layout(
            template="plotly_dark",
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            margin=dict(l=40, r=40, t=20, b=40), height=380,
            hovermode="x unified", showlegend=False,
            xaxis=dict(showgrid=True, gridcolor="#262730"),
            yaxis=dict(showgrid=True, gridcolor="#262730",
                       title="Level (%)", range=[y0, y1],
                       zeroline=False, tickformat=".3f"),
        )
        st.plotly_chart(fig, use_container_width=True,
                        key=f"chart::{inst}")

        st.caption(f"Last observation: {d.index[-1].strftime('%d/%m/%Y')} - "
                   f"{len(d)} points over the selected period")


def compute_row(name, df, kind, src_label):
    if df is None or df.empty:
        return None
    d = df[df.index >= cutoff]
    if len(d) < 2:
        return None
    cur, start = d["value"].iloc[-1], d["value"].iloc[0]
    delta = cur - start
    change = (f"{delta:+.2f} pp" if kind == "index"
              else f"{delta * 100:+.1f} bps")
    return {
        "Instrument": name,
        "Level (%)": round(float(cur), 3),
        "Change": change,
        "Trend": d["value"].tolist(),
        "Last point": d.index[-1].strftime("%d/%m/%Y"),
        "Source": src_label,
    }


for section in [s for s in INSTRUMENTS if s in regions]:
    st.subheader(section)

    with st.spinner(f"Loading {section}..."):
        data, errors, timings = load_section(section)
    POOL.update(data)
    ERRORS.update(errors)
    TIMINGS.update(timings)

    kinds = {s["name"]: s["kind"] for s in INSTRUMENTS[section]}
    srcs = {s["name"]: SOURCE_LABEL[s["src"]] for s in INSTRUMENTS[section]}

    for dname, ddf in build_derived(POOL, section).items():
        data[dname] = ddf
        POOL[dname] = ddf
        kinds[dname] = "spread"
        srcs[dname] = "computed"

    rows = [r for r in (compute_row(n, df, kinds.get(n, "rate"),
                                    srcs.get(n, "-"))
                        for n, df in data.items()) if r]

    if not rows:
        st.info("No data available for the selected period.")
        continue

    disp = pd.DataFrame(rows)

    # Widget key carries a version counter. Bumping the counter gives the
    # table a brand-new key, so Streamlit rebuilds it with an empty
    # selection. This is the supported way to clear a selection: writing
    # to st.session_state[key] after the widget exists raises
    # StreamlitAPIException.
    ver = st.session_state.tbl_ver.get(section, 0)
    wkey = f"tbl::{section}::v{ver}"

    ev = st.dataframe(
        disp,
        column_config={
            "Trend": st.column_config.LineChartColumn("Trend", width="medium"),
            "Level (%)": st.column_config.NumberColumn(format="%.3f"),
            "Source": st.column_config.TextColumn(width="small"),
        },
        hide_index=True,
        use_container_width=True,
        selection_mode="single-row",
        on_select="rerun",
        key=wkey,
    )

    # --- Mutually exclusive selection: the latest click wins ---
    cur_rows = tuple(ev.selection.rows)
    if cur_rows:
        picked = (section, disp.iloc[cur_rows[0]]["Instrument"])
        if st.session_state.sel != picked:
            st.session_state.sel = picked
            # Bump every OTHER table so they re-render deselected.
            for other in regions:
                if other != section:
                    st.session_state.tbl_ver[other] = (
                        st.session_state.tbl_ver.get(other, 0) + 1)
            st.rerun()

    # --- Detail panel, inline under THIS section only ---
    if st.session_state.sel and st.session_state.sel[0] == section:
        sel_inst = st.session_state.sel[1]
        sel_df = data.get(sel_inst)
        if sel_df is not None and not sel_df.empty:
            render_deep_dive(sel_inst, sel_df)

if show_diag:
    with st.expander("Diagnostics"):
        if ERRORS:
            st.markdown("**Failed series**")
            for k, v in ERRORS.items():
                st.text(f"{k} -> {v}")
        else:
            st.success("All series responded.")
        st.markdown("**Slowest responses (seconds)**")
        st.json(dict(sorted(TIMINGS.items(), key=lambda x: -x[1])[:15]))
