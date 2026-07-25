# Rates & Macro Desk

Multi-source interest-rate and macro dashboard built with Streamlit.
Pulls end-of-day data from FRED, the ECB Data Portal, Deutsche Bundesbank,
the Bank of England and the US Treasury.

## Sources and keys

| Source | Key needed | Covers |
|---|---|---|
| FRED | **yes** (free) | Fed rates, TIPS, breakevens, credit OAS, CPI/PCE, JGB/China (monthly) |
| ECB Data Portal | no | ESTR, Euribor (monthly), AAA curve, peripheral sovereigns (monthly) |
| Bundesbank | no | Bund curve (daily) |
| Bank of England | no | Bank Rate, SONIA, Gilts (daily) |
| US Treasury | no | UST par yield curve (daily) |

Only FRED needs a key: https://fredaccount.stlouisfed.org/apikeys

## Run locally

```bash
pip install -r requirements.txt
```

Create `.streamlit/secrets.toml` next to `app.py`:

```toml
FRED_API_KEY = "your_key_here"
```

Then:

```bash
streamlit run app.py
```

## Deploy on Streamlit Community Cloud

1. Push this repo to GitHub (the `.gitignore` keeps your key out).
2. Go to https://share.streamlit.io and connect the repo.
3. In **Advanced settings -> Secrets**, paste:
   ```toml
   FRED_API_KEY = "your_key_here"
   ```
4. Deploy. The app gets a public `*.streamlit.app` URL.

## Notes on data frequency

Series suffixed `(monthly)` are monthly averages, not daily fixings.
This affects Euribor, and the OAT / BTP / Bonos / PGB sovereigns, and the
Asian rates - no free daily source exists for these. Everything else is
daily. Each row shows its source and its own last observation date.
