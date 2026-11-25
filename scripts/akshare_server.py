#!/usr/bin/env python3
# Copyright 2026 Tauric Research
# SPDX-License-Identifier: Apache-2.0
"""Lightweight local HTTP bridge exposing A-share and China macro data via AkShare."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
import re
import sys
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import akshare as ak
import pandas as pd

CN_TZ = ZoneInfo("Asia/Shanghai")

INDEX_CATALOG: dict[str, dict[str, str]] = {
    "000001.SH": {"name": "上证指数", "tx": "sh000001"},
    "000300.SH": {"name": "沪深300", "tx": "sh000300"},
    "000905.SH": {"name": "中证500", "tx": "sh000905"},
    "000852.SH": {"name": "中证1000", "tx": "sh000852"},
    "000688.SH": {"name": "科创50", "tx": "sh000688"},
    "000016.SH": {"name": "上证50", "tx": "sh000016"},
    "399001.SZ": {"name": "深证成指", "tx": "sz399001"},
    "399006.SZ": {"name": "创业板指", "tx": "sz399006"},
    "899050.BJ": {"name": "北证50", "tx": "bj899050"},
}

MACRO_CATALOG: list[dict[str, Any]] = [
    {
        "key": "cn.rates.gov_10y",
        "name": "China 10-Year Government Bond Yield",
        "aliases": ["中国10年期国债收益率", "中国十年期国债收益率", "10y", "treasury", "bond", "yield", "国债"],
        "frequency": "daily",
        "unit": {"kind": "percent"},
        "kind": "bond_10y",
    },
    {
        "key": "cn.rates.gov_2y",
        "name": "China 2-Year Government Bond Yield",
        "aliases": ["中国2年期国债收益率", "2y", "短端国债"],
        "frequency": "daily",
        "unit": {"kind": "percent"},
        "kind": "bond_2y",
    },
    {
        "key": "cn.macro.cpi_yoy",
        "name": "China CPI YoY",
        "aliases": ["中国cpi", "cpi", "居民消费价格指数", "通胀", "inflation"],
        "frequency": "monthly",
        "unit": {"kind": "percent"},
        "kind": "cpi_yoy",
    },
    {
        "key": "cn.macro.pmi_manufacturing",
        "name": "China Manufacturing PMI",
        "aliases": ["中国pmi", "pmi", "制造业pmi", "采购经理人指数"],
        "frequency": "monthly",
        "unit": {"kind": "index_points"},
        "kind": "pmi_mfg",
    },
    {
        "key": "cn.macro.gdp_yoy",
        "name": "China GDP YoY",
        "aliases": ["中国gdp", "gdp", "国内生产总值", "经济增长"],
        "frequency": "quarterly",
        "unit": {"kind": "percent"},
        "kind": "gdp_yoy",
    },
]

_CACHE_LOCK = threading.Lock()
_SPOT_CACHE: dict[str, Any] = {"at": 0.0, "by_tx": {}}
_CODE_NAME_CACHE: dict[str, Any] = {"at": 0.0, "rows": []}
_DF_CACHE: dict[str, tuple[float, Any]] = {}
_LAST_ACTIVITY = time.monotonic()


def cached_call(key: str, ttl_seconds: float, loader: Any) -> Any:
    now = time.monotonic()
    with _CACHE_LOCK:
        entry = _DF_CACHE.get(key)
        if entry is not None and now - entry[0] < ttl_seconds:
            return entry[1]
    value = loader()
    with _CACHE_LOCK:
        _DF_CACHE[key] = (time.monotonic(), value)
    return value


def sanitize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (int, bool, str)):
        return value
    if hasattr(value, "item") and not isinstance(value, (dict, list)):
        return sanitize(value.item())
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.strftime("%Y-%m-%d")
    if pd.isna(value) if not isinstance(value, (list, dict, tuple)) else False:
        return None
    if isinstance(value, dict):
        return {str(k): sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(v) for v in value]
    return str(value)


def num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text or text in {"-", "--", "None", "nan", "NaN"}:
            return None
        try:
            parsed = float(text)
            return parsed if math.isfinite(parsed) else None
        except ValueError:
            return None
    try:
        if pd.isna(value):
            return None
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def parse_a_symbol(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.lower().startswith("security:"):
        text = text[9:]
    upper = text.upper()

    mic_match = re.match(r"^(XSHG|XSHE|XBSE):([0-9]{6})$", upper)
    if mic_match:
        mic, code = mic_match.group(1), mic_match.group(2)
        suffix = {"XSHG": "SH", "XSHE": "SZ", "XBSE": "BJ"}[mic]
    else:
        suffix_match = re.match(r"^([0-9]{6})\.(SH|SS|SZ|BJ)$", upper)
        if suffix_match:
            code = suffix_match.group(1)
            suffix = "SH" if suffix_match.group(2) == "SS" else suffix_match.group(2)
        else:
            prefix_match = re.match(r"^(SH|SZ|BJ)([0-9]{6})$", upper)
            if prefix_match:
                suffix, code = prefix_match.group(1), prefix_match.group(2)
            elif re.match(r"^[0-9]{6}$", upper):
                code = upper
                if code.startswith(("60", "68", "90")):
                    suffix = "SH"
                elif code.startswith(("00", "30", "20")):
                    suffix = "SZ"
                elif code.startswith(("43", "83", "87", "92")):
                    suffix = "BJ"
                else:
                    raise ValueError(f"Unsupported A-share code: {raw}")
            else:
                raise ValueError(f"Unsupported A-share symbol: {raw}")

    canonical = f"{code}.{suffix}"
    tx = f"{suffix.lower()}{code}"
    em = f"{code}.{suffix}"
    em_prefix = f"{suffix}{code}"
    mic = {"SH": "XSHG", "SZ": "XSHE", "BJ": "XBSE"}[suffix]
    venue = {"SH": "Shanghai Stock Exchange", "SZ": "Shenzhen Stock Exchange", "BJ": "Beijing Stock Exchange"}[suffix]
    is_index = canonical in INDEX_CATALOG
    return {
        "raw": raw,
        "code": code,
        "suffix": suffix,
        "canonical": canonical,
        "tx": tx,
        "em": em,
        "em_prefix": em_prefix,
        "mic": mic,
        "venue": venue,
        "is_index": is_index,
    }


def get_code_name_rows() -> list[dict[str, str]]:
    now = time.monotonic()
    with _CACHE_LOCK:
        if _CODE_NAME_CACHE["rows"] and now - _CODE_NAME_CACHE["at"] < 43200:
            return _CODE_NAME_CACHE["rows"]
    df = ak.stock_info_a_code_name()
    rows: list[dict[str, str]] = []
    for _, row in df.iterrows():
        code = str(row.get("code", "")).strip().zfill(6)
        name = str(row.get("name", "")).strip()
        if not code or not name:
            continue
        try:
            parsed = parse_a_symbol(code)
            rows.append(
                {
                    "key": parsed["canonical"],
                    "code": code,
                    "name": name,
                    "type": "stock",
                    "mic": parsed["mic"],
                }
            )
        except ValueError:
            continue
    with _CACHE_LOCK:
        _CODE_NAME_CACHE["at"] = now
        _CODE_NAME_CACHE["rows"] = rows
    return rows


def get_spot_map() -> dict[str, dict[str, Any]]:
    now = time.monotonic()
    with _CACHE_LOCK:
        if _SPOT_CACHE["by_tx"] and now - _SPOT_CACHE["at"] < 20:
            return _SPOT_CACHE["by_tx"]
    df = ak.stock_zh_a_spot_tx()
    by_tx: dict[str, dict[str, Any]] = {}
    for record in df.to_dict(orient="records"):
        tx_code = str(record.get("code", "")).strip().lower()
        if tx_code:
            by_tx[tx_code] = sanitize(record)
    with _CACHE_LOCK:
        _SPOT_CACHE["at"] = now
        _SPOT_CACHE["by_tx"] = by_tx
    return by_tx


def cn_now() -> datetime:
    return datetime.now(CN_TZ)


def cn_session_date(now_dt: datetime) -> tuple[str, str]:
    today = now_dt.strftime("%Y-%m-%d")
    hhmm = now_dt.hour * 100 + now_dt.minute
    if now_dt.weekday() >= 5:
        return today, "closed"
    if 930 <= hhmm < 1130 or 1300 <= hhmm < 1500:
        return today, "regular"
    if 915 <= hhmm < 930:
        return today, "pre"
    if 1130 <= hhmm < 1300:
        return today, "break"
    return today, "closed"


def is_bar_settled(bar_date: str, now_dt: datetime) -> bool:
    today = now_dt.strftime("%Y-%m-%d")
    if bar_date < today:
        return True
    if bar_date > today:
        return False
    return (now_dt.hour * 100 + now_dt.minute) >= 1505


def fetch_daily_df(info: dict[str, Any], from_date: str, to_date: str) -> pd.DataFrame:
    start_ymd = from_date.replace("-", "")
    end_ymd = to_date.replace("-", "")
    if info["is_index"]:
        df = ak.index_zh_daily_tx(symbol=info["tx"])
    else:
        try:
            df = ak.stock_zh_a_hist_tx(symbol=info["tx"], start_date=start_ymd, end_date=end_ymd, adjust="qfq")
        except Exception:
            df = ak.stock_zh_a_hist_sina(symbol=info["tx"], start_date=start_ymd, end_date=end_ymd, adjust="qfq")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = df["date"].astype(str).str.slice(0, 10)
    return df.sort_values("date")


def build_daily_bars(info: dict[str, Any], from_date: str, to_date: str) -> dict[str, Any]:
    # Pull extra history to confirm first trade date and whether `to_date` is settled.
    now_dt = cn_now()
    df = fetch_daily_df(info, from_date, (now_dt + timedelta(days=2)).strftime("%Y-%m-%d"))
    if df.empty:
        raise LookupError(f"AkShare has no daily bars for {info['canonical']}")

    name = INDEX_CATALOG[info["canonical"]]["name"] if info["is_index"] else lookup_stock_name(info)
    first_date = str(df.iloc[0]["date"])
    today_str = now_dt.strftime("%Y-%m-%d")
    _, mkt_state = cn_session_date(now_dt)

    bars: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        d = str(row["date"])
        if d < from_date or d > to_date:
            continue
        if d == today_str and not is_bar_settled(d, now_dt) and mkt_state in {"regular", "pre", "break"}:
            continue
        c = num(row.get("close"))
        if c is None:
            continue
        bars.append(
            {
                "date": d,
                "open": num(row.get("open")),
                "high": num(row.get("high")),
                "low": num(row.get("low")),
                "close": c,
                "volume": num(row.get("volume")),
                "revision": "final" if d < today_str or is_bar_settled(d, now_dt) else "provisional",
            }
        )

    return {
        "instrument": {
            "key": info["canonical"],
            "name": name,
            "currency": "CNY",
            "venue": info["venue"],
            "classification": {"instrument_type": "index" if info["is_index"] else "stock"},
        },
        "price_basis": "index_level" if info["is_index"] else "forward_adjusted",
        "bars": bars,
        "coverage": {
            "from": bars[0]["date"] if bars else None,
            "through": bars[-1]["date"] if bars else None,
            "complete": None,
            "history_from": first_date,
            "missing_sessions": 0,
        },
        "earlier_end": (
            (datetime.strptime(from_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            if first_date < from_date
            else None
        ),
    }


def build_intraday_bars(
    info: dict[str, Any], interval: str, start: str, end: str | None, limit: int
) -> dict[str, Any]:
    if info["is_index"]:
        raise ValueError(f"Intraday bars are only supported for stocks, not index {info['canonical']}")
    period = "15" if interval == "15m" else "60"
    df = ak.stock_zh_a_hist_min_sina(symbol=info["tx"], period=period)
    if df is None or df.empty:
        raise LookupError(f"AkShare has no intraday bars for {info['canonical']}")
    name = lookup_stock_name(info)
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        day_raw = str(row.get("day", "")).strip()
        if not day_raw:
            continue
        # Sina minute bars carry Asia/Shanghai timestamps ("YYYY-MM-DD HH:MM:SS").
        try:
            local_dt = datetime.strptime(day_raw[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)
        except ValueError:
            continue
        utc_iso = local_dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
        session_date = local_dt.strftime("%Y-%m-%d")
        if session_date < start[:10]:
            continue
        if end and utc_iso > end and session_date > end[:10]:
            continue
        rows.append(
            {
                "start": utc_iso,
                "date": session_date,
                "open": num(row.get("open")),
                "high": num(row.get("high")),
                "low": num(row.get("low")),
                "close": num(row.get("close")),
                "volume": num(row.get("volume")),
                "revision": "final" if local_dt < cn_now() else "provisional",
            }
        )
    sliced = rows[-limit:] if len(rows) > limit else rows
    earlier_end = sliced[0]["start"] if len(rows) > limit and sliced else None
    return {
        "instrument": {
            "key": info["canonical"],
            "name": name,
            "currency": "CNY",
            "venue": info["venue"],
            "classification": {"instrument_type": "stock"},
        },
        "price_basis": "unadjusted",
        "bars": sliced,
        "missing_sessions": 0,
        "incomplete_because": None,
        "history_from": rows[0]["date"] if rows else None,
        "earlier_end": earlier_end,
    }


def lookup_stock_name(info: dict[str, Any]) -> str:
    try:
        spot = get_spot_map().get(info["tx"])
        if spot and spot.get("name"):
            return str(spot["name"])
    except Exception:
        pass
    try:
        for row in get_code_name_rows():
            if row["key"] == info["canonical"]:
                return row["name"]
    except Exception:
        pass
    return info["canonical"]


def build_quote(info: dict[str, Any]) -> dict[str, Any]:
    now_dt = cn_now()
    today_str, mkt_state = cn_session_date(now_dt)
    if info["is_index"]:
        df = ak.index_zh_daily_tx(symbol=info["tx"])
        if df is None or df.empty:
            raise LookupError(f"AkShare has no quote for {info['canonical']}")
        df = df.copy()
        df["date"] = df["date"].astype(str).str.slice(0, 10)
        df = df.sort_values("date")
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else None
        price = num(latest.get("close"))
        prev_close = num(prev.get("close")) if prev is not None else None
        change = (price - prev_close) if (price is not None and prev_close is not None) else None
        change_pct = ((change / prev_close) * 100.0) if (change is not None and prev_close) else None
        sess_date = str(latest["date"])
        return {
            "instrument": {
                "key": info["canonical"],
                "name": INDEX_CATALOG[info["canonical"]]["name"],
                "currency": "CNY",
                "venue": info["venue"],
                "classification": {"instrument_type": "index"},
            },
            "quote_kind": "last_close",
            "price": price,
            "price_basis": "index_level",
            "price_time": f"{sess_date}T07:00:00Z",
            "session_date": sess_date,
            "market_state": mkt_state,
            "previous_close": prev_close,
            "change": round(change, 4) if change is not None else None,
            "change_percent": round(change_pct, 4) if change_pct is not None else None,
            "day": {
                "open": num(latest.get("open")),
                "high": num(latest.get("high")),
                "low": num(latest.get("low")),
                "volume": num(latest.get("volume")),
            },
        }

    spot = get_spot_map().get(info["tx"])
    if spot is None:
        raise LookupError(f"AkShare has no quote for {info['canonical']}")
    price = num(spot.get("zxj"))
    change = num(spot.get("zd"))
    change_pct = num(spot.get("zdf"))
    prev_close = round(price - change, 4) if price is not None and change is not None else None
    vol_lots = num(spot.get("volume"))
    zsz_yi = num(spot.get("zsz"))
    # Obtain recent OHLC / 52w range from Tencent daily bars (fast single-stock query).
    day_obj: dict[str, Any] = {"volume": vol_lots * 100.0 if vol_lots is not None else None}
    range_52w: dict[str, Any] | None = None
    session_date = today_str
    try:
        from_1y = (now_dt - timedelta(days=370)).strftime("%Y-%m-%d")
        hist = fetch_daily_df(info, from_1y, today_str)
        if not hist.empty:
            last_bar = hist.iloc[-1]
            session_date = str(last_bar["date"])
            if session_date == today_str or mkt_state == "closed":
                day_obj["open"] = num(last_bar.get("open"))
                day_obj["high"] = num(last_bar.get("high"))
                day_obj["low"] = num(last_bar.get("low"))
            lows = pd.to_numeric(hist["low"], errors="coerce").dropna()
            highs = pd.to_numeric(hist["high"], errors="coerce").dropna()
            if not lows.empty and not highs.empty:
                range_52w = {"low": float(lows.min()), "high": float(highs.max())}
    except Exception:
        pass

    return {
        "instrument": {
            "key": info["canonical"],
            "name": str(spot.get("name") or info["canonical"]),
            "currency": "CNY",
            "venue": info["venue"],
            "classification": {"instrument_type": "stock"},
        },
        "quote_kind": "last" if mkt_state == "regular" and session_date == today_str else "last_close",
        "price": price,
        "price_basis": "as traded",
        "price_time": now_dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "session_date": session_date,
        "market_state": mkt_state,
        "previous_close": prev_close,
        "change": change,
        "change_percent": change_pct,
        "day": day_obj,
        "range_52w": range_52w,
        "market_cap": zsz_yi * 1e8 if zsz_yi is not None else None,
    }


INCOME_LINE_MAP: list[tuple[str, str]] = [
    ("TOTAL_OPERATE_INCOME", "Total Operating Revenue"),
    ("OPERATE_INCOME", "Operating Revenue"),
    ("OPERATE_COST", "Cost of Goods Sold"),
    ("OPERATE_TAX_ADD", "Taxes and Surcharges"),
    ("SALE_EXPENSE", "Selling Expenses"),
    ("MANAGE_EXPENSE", "General and Administrative Expenses"),
    ("RESEARCH_EXPENSE", "Research and Development Expenses"),
    ("FINANCE_EXPENSE", "Financial Expenses"),
    ("OPERATE_PROFIT", "Operating Profit"),
    ("TOTAL_PROFIT", "Income Before Income Taxes"),
    ("INCOME_TAX", "Income Tax Expense"),
    ("NETPROFIT", "Net Income"),
    ("PARENT_NETPROFIT", "Net Income Attributable to Parent"),
    ("DEDUCT_PARENT_NETPROFIT", "Net Income Excluding Non-Recurring Items"),
    ("BASIC_EPS", "Basic EPS"),
    ("DILUTED_EPS", "Diluted EPS"),
]

BALANCE_LINE_MAP: list[tuple[str, str]] = [
    ("MONETARYFUNDS", "Cash and Cash Equivalents"),
    ("ACCOUNTS_RECE", "Accounts Receivable"),
    ("INVENTORY", "Inventory"),
    ("TOTAL_CURRENT_ASSETS", "Total Current Assets"),
    ("FIXED_ASSET", "Property, Plant & Equipment"),
    ("CIP", "Construction in Progress"),
    ("INTANGIBLE_ASSET", "Intangible Assets"),
    ("GOODWILL", "Goodwill"),
    ("TOTAL_NONCURRENT_ASSETS", "Total Non-Current Assets"),
    ("TOTAL_ASSETS", "Total Assets"),
    ("SHORT_LOAN", "Short-Term Debt"),
    ("ACCOUNTS_PAYABLE", "Accounts Payable"),
    ("CONTRACT_LIAB", "Contract Liabilities"),
    ("NONCURRENT_LIAB_1YEAR", "Current Portion of Long-Term Debt"),
    ("TOTAL_CURRENT_LIAB", "Total Current Liabilities"),
    ("LONG_LOAN", "Long-Term Debt"),
    ("BOND_PAYABLE", "Bonds Payable"),
    ("LEASE_LIAB", "Lease Liabilities"),
    ("TOTAL_NONCURRENT_LIAB", "Total Non-Current Liabilities"),
    ("TOTAL_LIABILITIES", "Total Liabilities"),
    ("SHARE_CAPITAL", "Share Capital"),
    ("TOTAL_PARENT_EQUITY", "Parent Shareholders' Equity"),
    ("MINORITY_EQUITY", "Minority Interest"),
    ("TOTAL_EQUITY", "Total Shareholders' Equity"),
]

CASHFLOW_LINE_MAP: list[tuple[str, str]] = [
    ("SALES_SERVICES", "Cash Received from Sales and Services"),
    ("NETCASH_OPERATE", "Net Cash from Operating Activities"),
    ("CONSTRUCT_LONG_ASSET", "Payments to Acquire PP&E"),
    ("NETCASH_INVEST", "Net Cash from Investing Activities"),
    ("ASSIGN_DIVIDEND_PORFIT", "Cash Paid for Dividends and Interest"),
    ("NETCASH_FINANCE", "Net Cash from Financing Activities"),
    ("CCE_ADD", "Net Change in Cash and Cash Equivalents"),
    ("END_CCE", "Ending Cash and Cash Equivalents"),
]


def quarter_start_for(end_date: str, period_type: str) -> str | None:
    if len(end_date) < 10:
        return None
    year = end_date[:4]
    if period_type == "annual":
        return f"{year}-01-01"
    mmdd = end_date[5:10]
    return {
        "03-31": f"{year}-01-01",
        "06-30": f"{year}-04-01",
        "09-30": f"{year}-07-01",
        "12-31": f"{year}-10-01",
    }.get(mmdd)


def build_statements(
    info: dict[str, Any], statement: str, period_type: str, limit: int, after: str | None
) -> dict[str, Any]:
    if info["is_index"]:
        raise ValueError(f"Financial statements are not served for index {info['canonical']}")
    sym = info["em_prefix"]
    if statement == "income_statement":
        df = cached_call(
            f"stmt:{statement}:{period_type}:{sym}",
            600,
            lambda: (
                ak.stock_profit_sheet_by_yearly_em(symbol=sym)
                if period_type == "annual"
                else ak.stock_profit_sheet_by_quarterly_em(symbol=sym)
            ),
        )
        line_map = INCOME_LINE_MAP
    elif statement == "balance_sheet":
        df = cached_call(
            f"stmt:{statement}:{period_type}:{sym}",
            600,
            lambda: (
                ak.stock_balance_sheet_by_yearly_em(symbol=sym)
                if period_type == "annual"
                else ak.stock_balance_sheet_by_report_em(symbol=sym)
            ),
        )
        line_map = BALANCE_LINE_MAP
    else:
        df = cached_call(
            f"stmt:{statement}:{period_type}:{sym}",
            600,
            lambda: (
                ak.stock_cash_flow_sheet_by_yearly_em(symbol=sym)
                if period_type == "annual"
                else ak.stock_cash_flow_sheet_by_quarterly_em(symbol=sym)
            ),
        )
        line_map = CASHFLOW_LINE_MAP

    if df is None or df.empty:
        raise LookupError(f"AkShare has no {statement} for {info['canonical']}")

    df = df.copy()
    df["_end"] = df["REPORT_DATE"].astype(str).str.slice(0, 10)
    df = df.sort_values("_end", ascending=False)
    offset = int(after) if after and after.isdigit() else 0
    window = df.iloc[offset : offset + limit]
    next_cursor = str(offset + limit) if offset + limit < len(df) else None
    stock_name = str(df.iloc[0].get("SECURITY_NAME_ABBR") or lookup_stock_name(info))

    periods: list[dict[str, Any]] = []
    for _, row in window.iterrows():
        end_date = str(row["_end"])
        notice = str(row.get("NOTICE_DATE") or "")[:10]
        report_name = str(row.get("REPORT_DATE_NAME") or row.get("REPORT_TYPE") or period_type)
        currency = str(row.get("CURRENCY") or "CNY")
        p_start = None if statement == "balance_sheet" else quarter_start_for(end_date, period_type)
        lines: list[dict[str, Any]] = []
        for col, label in line_map:
            val = num(row.get(col))
            if val is None:
                continue
            unit = "CNY/shares" if "EPS" in col else currency
            lines.append(
                {
                    "line": label,
                    "value": val,
                    "unit": unit,
                    "period_start": p_start,
                    "sources": (
                        [
                            {
                                "accession": report_name,
                                "form": report_name,
                                "accepted_at": notice or end_date,
                                "cik": info["canonical"],
                                "predecessor": False,
                            }
                        ]
                        if report_name
                        else None
                    ),
                }
            )
        periods.append({"period_end": end_date, "lines": lines})

    return {
        "instrument": {
            "key": info["canonical"],
            "name": stock_name,
            "currency": "CNY",
            "venue": info["venue"],
            "classification": {"instrument_type": "stock"},
        },
        "periods": periods,
        "succession": {"registrants": [], "stopsAt": []},
        "next_cursor": next_cursor,
    }


def build_profile(info: dict[str, Any]) -> dict[str, Any]:
    if info["is_index"]:
        raise ValueError(f"Company profiles are not served for index {info['canonical']}")

    def load_ind() -> dict[str, Any]:
        try:
            ind_df = cached_call(
                f"ind:{info['em']}",
                600,
                lambda: ak.stock_financial_analysis_indicator_em(symbol=info["em"], indicator="按报告期"),
            )
            if ind_df is not None and not ind_df.empty:
                return sanitize(ind_df.iloc[0].to_dict())
        except Exception:
            pass
        return {}

    def load_bal() -> dict[str, Any]:
        try:
            bal_df = cached_call(
                f"stmt:balance_sheet:quarterly:{info['em_prefix']}",
                600,
                lambda: ak.stock_balance_sheet_by_report_em(symbol=info["em_prefix"]),
            )
            if bal_df is not None and not bal_df.empty:
                return sanitize(bal_df.iloc[0].to_dict())
        except Exception:
            pass
        return {}

    def load_div() -> Any:
        try:
            return cached_call(f"fhps:{info['code']}", 600, lambda: ak.stock_fhps_detail_em(symbol=info["code"]))
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        spot_fut = pool.submit(get_spot_map)
        ind_fut = pool.submit(load_ind)
        bal_fut = pool.submit(load_bal)
        div_fut = pool.submit(load_div)
        spot = spot_fut.result().get(info["tx"]) or {}
        ind_row = ind_fut.result()
        bal_row = bal_fut.result()
        div_df = div_fut.result()

    name = str(spot.get("name") or lookup_stock_name(info))
    price = num(spot.get("zxj"))
    zsz_yi = num(spot.get("zsz"))
    market_cap = zsz_yi * 1e8 if zsz_yi is not None else None
    pe_ttm = num(spot.get("pe_ttm"))
    pb = num(spot.get("pn"))

    div_rate: float | None = None
    div_yield: float | None = None
    if div_df is not None and not div_df.empty:
        latest_div = div_df.iloc[-1]
        cash_per_10 = num(latest_div.get("现金分红-现金分红比例"))
        if cash_per_10 is not None:
            div_rate = cash_per_10 / 10.0
            if price and price > 0:
                div_yield = div_rate / price

    total_cash = num(bal_row.get("MONETARYFUNDS"))
    debt_parts = [
        num(bal_row.get("SHORT_LOAN")),
        num(bal_row.get("LONG_LOAN")),
        num(bal_row.get("BOND_PAYABLE")),
        num(bal_row.get("NONCURRENT_LIAB_1YEAR")),
        num(bal_row.get("LEASE_LIAB")),
    ]
    total_debt = sum(d for d in debt_parts if d is not None) if any(d is not None for d in debt_parts) else None
    total_equity = num(bal_row.get("TOTAL_EQUITY"))
    shares = num(bal_row.get("SHARE_CAPITAL"))
    if shares is None and market_cap is not None and price and price > 0:
        shares = market_cap / price

    roe_pct = num(ind_row.get("ROEJQ"))
    roa_pct = num(ind_row.get("ZZCJLL"))
    net_margin_pct = num(ind_row.get("XSJLL"))
    rev_growth_pct = num(ind_row.get("TOTALOPERATEREVETZ"))
    earn_growth_pct = num(ind_row.get("PARENTNETPROFITTZ"))
    refreshed = str(ind_row.get("UPDATE_DATE") or bal_row.get("NOTICE_DATE") or "")[:10] or None

    return {
        "instrument": {
            "key": info["canonical"],
            "name": name,
            "currency": "CNY",
            "venue": info["venue"],
            "classification": {"instrument_type": "stock"},
        },
        "dataset_status": {"refreshed_at": refreshed},
        "country": "China",
        "financial_currency": "CNY",
        "market_cap": market_cap,
        "market_cap_basis": "total_shares" if market_cap is not None else None,
        "pe_ratio": pe_ttm,
        "price_to_book": pb,
        "profit_margin": net_margin_pct / 100.0 if net_margin_pct is not None else None,
        "return_on_assets": roa_pct / 100.0 if roa_pct is not None else None,
        "return_on_equity": roe_pct / 100.0 if roe_pct is not None else None,
        "revenue_growth": rev_growth_pct / 100.0 if rev_growth_pct is not None else None,
        "earnings_growth": earn_growth_pct / 100.0 if earn_growth_pct is not None else None,
        "total_cash": total_cash,
        "total_debt": total_debt,
        "debt_to_equity": (
            (total_debt / total_equity) * 100.0 if total_debt is not None and total_equity and total_equity > 0 else None
        ),
        "current_ratio": num(ind_row.get("LD")),
        "quick_ratio": num(ind_row.get("SD")),
        "dividend_rate": div_rate,
        "dividend_yield": div_yield,
        "shares_outstanding": shares,
    }


def build_estimates(info: dict[str, Any]) -> dict[str, Any]:
    if info["is_index"]:
        raise ValueError(f"Analyst estimates are not served for index {info['canonical']}")
    df = ak.stock_profit_forecast_ths(symbol=info["code"])
    if df is None or df.empty:
        raise LookupError(f"AkShare has no consensus estimates for {info['canonical']}")
    rows = [sanitize(r) for r in df.to_dict(orient="records")]
    return {
        "source": "THS Profit Forecast via AkShare",
        "eps_forecasts": [
            {
                "year": r.get("年度"),
                "institutions": num(r.get("预测机构数")),
                "eps_low": num(r.get("最小值")),
                "eps_mean": num(r.get("均值")),
                "eps_high": num(r.get("最大值")),
                "industry_avg": num(r.get("行业平均数")),
            }
            for r in rows
        ],
    }


def build_corporate_actions(info: dict[str, Any]) -> dict[str, Any]:
    if info["is_index"]:
        raise ValueError(f"Corporate actions are not served for index {info['canonical']}")
    df = cached_call(f"fhps:{info['code']}", 600, lambda: ak.stock_fhps_detail_em(symbol=info["code"]))
    if df is None or df.empty:
        return {
            "instrument": {
                "key": info["canonical"],
                "name": lookup_stock_name(info),
                "currency": "CNY",
                "venue": info["venue"],
            },
            "actions": [],
            "through": cn_now().strftime("%Y-%m-%d"),
            "stale": False,
        }
    actions: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        ex_date = str(row.get("除权除息日") or "").strip()[:10]
        if not ex_date or ex_date in {"NaT", "None", "nan"}:
            continue
        cash_per_10 = num(row.get("现金分红-现金分红比例"))
        if cash_per_10 is not None and cash_per_10 > 0:
            actions.append(
                {
                    "ex_date": ex_date,
                    "action_type": "dividend",
                    "value": round(cash_per_10 / 10.0, 6),
                    "unit": "CNY",
                    "cash_basis": "per_share_pre_tax",
                    "revised": False,
                }
            )
        bonus_per_10 = num(row.get("送转股份-送转总比例"))
        if bonus_per_10 is not None and bonus_per_10 > 0:
            actions.append(
                {
                    "ex_date": ex_date,
                    "action_type": "split",
                    "value": round(1.0 + bonus_per_10 / 10.0, 6),
                    "unit": "ratio",
                    "cash_basis": None,
                    "revised": False,
                }
            )
    actions.sort(key=lambda a: a["ex_date"], reverse=True)
    return {
        "instrument": {
            "key": info["canonical"],
            "name": lookup_stock_name(info),
            "currency": "CNY",
            "venue": info["venue"],
            "classification": {"instrument_type": "stock"},
        },
        "actions": actions,
        "through": cn_now().strftime("%Y-%m-%d"),
        "stale": False,
    }


def build_news(symbol: str | None, limit: int, after: str | None) -> dict[str, Any]:
    if not symbol:
        raise ValueError("AkShare news requires an A-share symbol")
    info = parse_a_symbol(symbol)
    df = ak.stock_news_em(symbol=info["code"])
    if df is None or df.empty:
        return {"articles": [], "next_cursor": None}
    records = df.to_dict(orient="records")
    offset = int(after) if after and after.isdigit() else 0
    sliced = records[offset : offset + limit]
    next_cursor = str(offset + limit) if offset + limit < len(records) else None
    articles = []
    for idx, row in enumerate(sliced):
        title = str(row.get("新闻标题") or "").strip() or None
        content = str(row.get("新闻内容") or "").strip() or None
        pub_time = str(row.get("发布时间") or "").strip() or None
        publisher = str(row.get("文章来源") or "").strip() or None
        url = str(row.get("新闻链接") or "").strip() or None
        articles.append(
            {
                "id": f"ak_{info['code']}_{offset + idx}",
                "title": title,
                "publisher": publisher,
                "url": url,
                "published_at": pub_time,
                "annotations": {"summary": content, "sentiment": None, "impact": None},
            }
        )
    return {"articles": articles, "next_cursor": next_cursor}


def search_instruments(query: str, kind: str | None, limit: int) -> list[dict[str, Any]]:
    q = query.strip().lower()
    matches: list[dict[str, Any]] = []
    stock_serves = ["quotes", "priceHistory", "profiles", "statements", "corporateActions", "news"]
    index_serves = ["quotes", "priceHistory"]

    if kind in (None, "security"):
        for key, meta in INDEX_CATALOG.items():
            if q in key.lower() or q in meta["name"].lower():
                matches.append(
                    {
                        "key": key,
                        "name": meta["name"],
                        "type": "index",
                        "listing_state": "listed",
                        "serves": index_serves,
                    }
                )
        for row in get_code_name_rows():
            if q in row["code"].lower() or q in row["key"].lower() or q in row["name"].lower():
                matches.append(
                    {
                        "key": row["key"],
                        "name": row["name"],
                        "type": "stock",
                        "listing_state": "listed",
                        "serves": stock_serves,
                    }
                )
                if len(matches) >= limit:
                    break

    if kind in (None, "series") and len(matches) < limit:
        for item in MACRO_CATALOG:
            if q in item["key"].lower() or q in item["name"].lower() or any(q in a for a in item["aliases"]):
                matches.append(
                    {
                        "key": f"series:{item['key']}",
                        "name": item["name"],
                        "type": "economic",
                        "listing_state": None,
                        "serves": ["macroSeries"],
                    }
                )
    return matches[:limit]


def search_macro(query: str) -> list[dict[str, Any]]:
    q = query.strip().lower()
    hits = []
    for item in MACRO_CATALOG:
        if q in item["key"].lower() or q in item["name"].lower() or any(q in a or a in q for a in item["aliases"]):
            hits.append(
                {
                    "key": item["key"],
                    "name": item["name"],
                    "frequency": item["frequency"],
                    "unit": item["unit"],
                }
            )
    return hits


def fetch_macro_observations(key: str, limit: int, end: str | None) -> dict[str, Any]:
    meta = next((m for m in MACRO_CATALOG if m["key"] == key), None)
    if meta is None:
        raise ValueError(f"Unknown China macro series key: {key}")
    kind = meta["kind"]
    observations: list[dict[str, Any]] = []

    if kind in ("bond_10y", "bond_2y"):
        col = "中国国债收益率10年" if kind == "bond_10y" else "中国国债收益率2年"
        df = cached_call("macro:bond_zh_us", 600, lambda: ak.bond_zh_us_yield_em(start_date="20200101"))
        df = df.copy()
        df["_d"] = df["日期"].astype(str).str.slice(0, 10)
        df = df.sort_values("_d")
        for _, row in df.iterrows():
            d = str(row["_d"])
            if end and d > end:
                continue
            val = num(row.get(col))
            observations.append(
                {
                    "value": val,
                    "status": "final",
                    "known_from": d,
                    "period": {"kind": "day", "start": d, "label": d},
                }
            )
    elif kind == "cpi_yoy":
        df = ak.macro_china_cpi()
        for _, row in df.iterrows():
            raw_m = str(row.get("月份", ""))
            m = re.match(r"(\d{4})年(\d{2})月份", raw_m)
            if not m:
                continue
            start = f"{m.group(1)}-{m.group(2)}-01"
            if end and start > end:
                continue
            observations.append(
                {
                    "value": num(row.get("全国-同比增长")),
                    "status": "final",
                    "period": {"kind": "month", "start": start, "label": f"{m.group(1)}-{m.group(2)}"},
                }
            )
        observations.sort(key=lambda o: o["period"]["start"])
    elif kind == "pmi_mfg":
        df = ak.macro_china_pmi()
        for _, row in df.iterrows():
            raw_m = str(row.get("月份", ""))
            m = re.match(r"(\d{4})年(\d{2})月份", raw_m)
            if not m:
                continue
            start = f"{m.group(1)}-{m.group(2)}-01"
            if end and start > end:
                continue
            observations.append(
                {
                    "value": num(row.get("制造业-指数")),
                    "status": "final",
                    "period": {"kind": "month", "start": start, "label": f"{m.group(1)}-{m.group(2)}"},
                }
            )
        observations.sort(key=lambda o: o["period"]["start"])
    elif kind == "gdp_yoy":
        df = ak.macro_china_gdp()
        for _, row in df.iterrows():
            raw_q = str(row.get("季度", ""))
            m = re.match(r"(\d{4})年第(\d)(?:-(\d))?季度", raw_q)
            if not m:
                continue
            year = m.group(1)
            q_num = int(m.group(3) or m.group(2))
            month = {1: "01", 2: "04", 3: "07", 4: "10"}.get(q_num, "01")
            start = f"{year}-{month}-01"
            if end and start > end:
                continue
            observations.append(
                {
                    "value": num(row.get("国内生产总值-同比增长")),
                    "status": "final",
                    "period": {"kind": "quarter", "start": start, "label": f"{year}-Q{q_num}"},
                }
            )
        observations.sort(key=lambda o: o["period"]["start"])

    sliced = observations[-limit:] if len(observations) > limit else observations
    continue_end = None
    if len(observations) > limit and sliced:
        first_start = datetime.strptime(sliced[0]["period"]["start"], "%Y-%m-%d")
        continue_end = (first_start - timedelta(days=1)).strftime("%Y-%m-%d")
    return {
        "series": {"key": meta["key"], "name": meta["name"], "frequency": meta["frequency"], "unit": meta["unit"]},
        "coverage": {"continue_end": continue_end, "missing": []},
        "observations": sliced,
    }


class AkShareHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(sanitize(payload), ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        global _LAST_ACTIVITY
        _LAST_ACTIVITY = time.monotonic()
        parsed = urlparse(self.path)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items() if v}

        try:
            if parsed.path == "/health":
                self.send_json(200, {"ok": True})
                return
            if parsed.path == "/instruments":
                q = params.get("q", "")
                if not q:
                    self.send_json(400, {"error": "invalid", "message": "Missing query parameter q"})
                    return
                limit = int(params.get("limit", "10"))
                self.send_json(200, search_instruments(q, params.get("kind"), limit))
                return
            if parsed.path == "/quote":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(200, build_quote(info))
                return
            if parsed.path == "/quotes":
                raw_symbols = [s for s in params.get("symbols", "").split(",") if s.strip()]
                quotes: list[dict[str, Any]] = []
                errors: list[dict[str, Any]] = []
                for sym in raw_symbols:
                    try:
                        info = parse_a_symbol(sym)
                        quotes.append({"instrument": sym, "quote": build_quote(info)})
                    except ValueError as err:
                        errors.append({"instrument": sym, "message": str(err), "kind": "unsupported"})
                    except LookupError as err:
                        errors.append({"instrument": sym, "message": str(err), "kind": "no_data"})
                    except Exception as err:
                        errors.append({"instrument": sym, "message": str(err), "kind": "unavailable"})
                self.send_json(200, {"quotes": quotes, "errors": errors})
                return
            if parsed.path == "/history/daily":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(200, build_daily_bars(info, params["from"], params["to"]))
                return
            if parsed.path == "/history/intraday":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(
                    200,
                    build_intraday_bars(
                        info,
                        params.get("interval", "1h"),
                        params["start"],
                        params.get("end"),
                        int(params.get("limit", "100")),
                    ),
                )
                return
            if parsed.path == "/statements":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(
                    200,
                    build_statements(
                        info,
                        params["statement"],
                        params.get("period_type", "quarterly"),
                        int(params.get("limit", "3")),
                        params.get("after"),
                    ),
                )
                return
            if parsed.path == "/profile":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(200, build_profile(info))
                return
            if parsed.path == "/estimates":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(200, build_estimates(info))
                return
            if parsed.path == "/corporate_actions":
                info = parse_a_symbol(params.get("symbol", ""))
                self.send_json(200, build_corporate_actions(info))
                return
            if parsed.path == "/news":
                self.send_json(
                    200,
                    build_news(params.get("symbol"), int(params.get("limit", "8")), params.get("after")),
                )
                return
            if parsed.path == "/macro/search":
                self.send_json(200, search_macro(params.get("q", "")))
                return
            if parsed.path == "/macro/observations":
                self.send_json(
                    200,
                    fetch_macro_observations(
                        params["key"],
                        int(params.get("limit", "13")),
                        params.get("end"),
                    ),
                )
                return
            self.send_json(404, {"error": "not_found", "message": f"Unknown route: {parsed.path}"})
        except ValueError as err:
            self.send_json(422, {"error": "unsupported", "message": str(err)})
        except LookupError as err:
            self.send_json(404, {"error": "no_data", "message": str(err)})
        except Exception as err:
            text = str(err)
            kind = "rate_limited" if "RateLimit" in type(err).__name__ or "限流" in text else "unavailable"
            self.send_json(502, {"error": kind, "message": f"{type(err).__name__}: {text}"})


def main() -> None:
    parser = argparse.ArgumentParser(description="Local AkShare HTTP bridge for TradingAgents")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--idle-seconds", type=int, default=0)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), AkShareHandler)
    bound_port = server.server_address[1]
    print(f"LISTENING http://{args.host}:{bound_port}", flush=True)

    if args.idle_seconds > 0:
        # Silence tqdm/akshare noise once started in auto-managed background mode.
        devnull = open(os.devnull, "w", encoding="utf-8")
        sys.stderr = devnull
        def watchdog() -> None:
            while True:
                time.sleep(min(30, max(5, args.idle_seconds // 4)))
                if time.monotonic() - _LAST_ACTIVITY >= args.idle_seconds:
                    os._exit(0)

        threading.Thread(target=watchdog, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
