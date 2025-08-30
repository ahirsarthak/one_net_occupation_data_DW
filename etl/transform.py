import re
from datetime import datetime
from typing import Any, Dict, List, Optional


def normalize_space(text: str) -> str:
    """Collapse internal whitespace and trim ends for robust string cleanup."""
    return re.sub(r"\s+", " ", (text or "").strip())


def clean_occupation_records(records: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Minimal cleaning for occupation rows: dedupe by code, trim fields, derive major_group_code."""
    seen = set()
    clean: List[Dict[str, str]] = []
    for r in records:
        code = normalize_space(r.get("onetsoc_code", ""))
        title = normalize_space(r.get("title", ""))
        desc = normalize_space(r.get("description", ""))
        if not code or not title:
            continue
        if code in seen:
            continue
        seen.add(code)
        major_group_code = code.split("-")[0][:2] if "-" in code and len(code) >= 2 else None
        # Convert nullable text fields to 'unavailable' per request
        desc_out = desc if desc else "unavailable"
        mg_out = major_group_code if major_group_code else "unavailable"
        clean.append({
            "onetsoc_code": code,
            "title": title,
            "description": desc_out,
            "major_group_code": mg_out,
        })
    return clean


 


_ALLOWED_SCALES = {"IM", "LV"}
_SOC_RE = re.compile(r"^\d{2}-\d{4}\.\d{2}$")


def _is_valid_soc(code: str) -> bool:
    return bool(_SOC_RE.match(code))


def _norm_flag(value: Optional[Any]) -> Optional[str]:
    """Normalize flag-like values to 'Y'/'N' or None.
    Accepts 'Y'/'N', 'y'/'n', 1/0, True/False, 'T'/'F', empty → None.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "Y" if value else "N"
    s = str(value).strip().upper()
    if s in ("Y", "N"):
        return s
    if s in ("T", "TRUE", "1"):
        return "Y"
    if s in ("F", "FALSE", "0"):
        return "N"
    if s == "":
        return None
    return None


def _to_float(value: Optional[Any]) -> Optional[float]:
    """Best-effort float conversion; None/''/NaN-like → None."""
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.upper() == "NAN":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _norm_date_iso(date_value: Optional[Any]) -> Optional[str]:
    """Normalize date to YYYY-MM-DD when possible; else return None.
    Accepts common formats like 'YYYY-MM-DD', 'MM/DD/YYYY'.
    """
    if date_value is None:
        return None
    s = str(date_value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    # Leave as-is if it already looks ISO-like
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    return None


def clean_ska_records(records: List[Dict[str, Any]], domain: str) -> List[Dict[str, Any]]:
    """Clean Skills/Knowledge/Abilities records without changing grain.
    - Trim whitespace in keys: onetsoc_code, element_id, scale_id, domain_source
    - Uppercase scale_id; drop rows with unexpected scales
    - Coerce numeric fields to float/int; leave missing as None (do not impute)
    - Normalize flags (recommend_suppress, not_relevant) to 'Y'/'N'/None
    - Standardize date_updated to ISO YYYY-MM-DD when possible
    - Ensure lower_ci_bound <= upper_ci_bound if both present (swap if reversed)
    """
    out: List[Dict[str, Any]] = []
    for r in records:
        code = normalize_space(str(r.get("onetsoc_code", "")))
        elem = normalize_space(str(r.get("element_id", "")))
        scale = normalize_space(str(r.get("scale_id", ""))).upper()
        if not code or not elem or scale not in _ALLOWED_SCALES:
            continue
        dv = _to_float(r.get("data_value"))
        n = _to_float(r.get("n"))
        se = _to_float(r.get("standard_error"))
        lcb = _to_float(r.get("lower_ci_bound"))
        ucb = _to_float(r.get("upper_ci_bound"))
        rs = _norm_flag(r.get("recommend_suppress"))
        nr = _norm_flag(r.get("not_relevant"))
        du = _norm_date_iso(r.get("date_updated"))
        src = normalize_space(str(r.get("domain_source", ""))) or None

        # Swap CI if reversed
        if lcb is not None and ucb is not None and lcb > ucb:
            lcb, ucb = ucb, lcb

        # Convert nullable text fields to 'unavailable' per request for staging
        rs_out = rs if rs is not None else "unavailable"
        nr_out = nr if nr is not None else "unavailable"
        du_out = du if du is not None else "unavailable"
        src_out = src if src is not None else "unavailable"

        out.append({
            "onetsoc_code": code,
            "element_id": elem,
            "scale_id": scale,
            "data_value": dv,
            "n": n,
            "standard_error": se,
            "lower_ci_bound": lcb,
            "upper_ci_bound": ucb,
            "recommend_suppress": rs_out,
            "not_relevant": nr_out,
            "date_updated": du_out,
            "domain_source": src_out,
            "_domain": domain.upper(),
        })
    return out


def split_and_clean_ska_records(records: List[Dict[str, Any]], domain: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (valid_cleaned, invalid_raw_with_reason) for SKA records.
    Invalid reasons include: missing_onetsoc_code, invalid_soc_format, missing_element_id,
    invalid_scale_id, missing_data_value.
    """
    valid: List[Dict[str, Any]] = []
    invalid: List[Dict[str, Any]] = []
    for r in records or []:
        code_raw = str(r.get("onetsoc_code", "") or "").strip()
        elem_raw = str(r.get("element_id", "") or "").strip()
        scale_raw = str(r.get("scale_id", "") or "").strip().upper()
        # Validate required keys and shapes
        if not code_raw:
            r2 = dict(r)
            r2["error_reason"] = "missing_onetsoc_code"
            r2["domain"] = domain.upper()
            invalid.append(r2)
            continue
        if not _is_valid_soc(code_raw):
            r2 = dict(r)
            r2["error_reason"] = "invalid_soc_format"
            r2["domain"] = domain.upper()
            invalid.append(r2)
            continue
        if not elem_raw:
            r2 = dict(r)
            r2["error_reason"] = "missing_element_id"
            r2["domain"] = domain.upper()
            invalid.append(r2)
            continue
        if scale_raw not in _ALLOWED_SCALES:
            r2 = dict(r)
            r2["error_reason"] = "invalid_scale_id"
            r2["domain"] = domain.upper()
            invalid.append(r2)
            continue
        # Coerce and normalize using existing cleaner path
        cleaned = clean_ska_records([r], domain)
        if not cleaned:
            r2 = dict(r)
            r2["error_reason"] = "cleaning_failed"
            r2["domain"] = domain.upper()
            invalid.append(r2)
            continue
        valid.extend(cleaned)
    return valid, invalid


__all__ = [
    "normalize_space",
    "clean_occupation_records",
    "clean_ska_records",
    "split_and_clean_ska_records",
]
