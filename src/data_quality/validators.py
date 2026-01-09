"""
Minimal validators for MVP (check_nulls, check_duplicates, check_ranges, iqr_outliers, impute_median)
"""
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def get_numeric_columns(df: DataFrame) -> List[str]:
    return [f.name for f in df.schema.fields if f.dataType.simpleString() in ("double", "float", "int", "long", "decimal")]

def check_nulls(df: DataFrame, threshold: float = 0.2) -> Dict[str, float]:
    """
    Return columns whose null fraction exceeds threshold.
    Output: dict of col -> percent_null (0..100)
    """
    total = df.count()
    if total == 0:
        return {}
    results = {}
    for c in df.columns:
        # some columns may not support isnan; use try/except
        try:
            null_count = df.filter(F.col(c).isNull() | F.isnan(F.col(c))).count()
        except Exception:
            null_count = df.filter(F.col(c).isNull()).count()
        pct = (null_count / total) * 100
        if pct >= (threshold * 100):
            results[c] = round(pct, 3)
    return results

def check_duplicates(df: DataFrame, subset: Optional[List[str]] = None) -> Dict[str, object]:
    """
    Return a small report about duplicates.
    - dup_count: number of duplicate rows (total rows - distinct rows on subset or full row)
    - sample: up to 5 example duplicate rows (as tuples)
    """
    if subset:
        distinct = df.dropDuplicates(subset)
    else:
        distinct = df.dropDuplicates()
    dup_count = df.count() - distinct.count()
    sample = []
    if dup_count > 0:
        sample_rows = df.exceptAll(distinct).limit(5).collect()
        sample = [tuple(r) for r in sample_rows]
    return {"dup_count": int(dup_count), "sample": sample}

def check_ranges(df: DataFrame, rules: Dict[str, Tuple[Optional[float], Optional[float]]] = None) -> Dict[str, Dict]:
    """
    rules: dict of column -> (min_value or None, max_value or None)
    Returns a dict for columns that violate the rules with counts.
    """
    if not rules:
        return {}
    results = {}
    total = df.count()
    for col, (minv, maxv) in rules.items():
        cond = None
        if minv is not None:
            cond = (F.col(col) < F.lit(minv))
        if maxv is not None:
            c2 = (F.col(col) > F.lit(maxv))
            cond = c2 if cond is None else (cond | c2)
        if cond is not None:
            bad = df.filter(cond).count()
            if bad > 0:
                results[col] = {"bad_count": int(bad), "percent": round(bad / total * 100, 3) if total > 0 else 0.0}
    return results

def iqr_outlier_bounds(df: DataFrame, col: str) -> Tuple[float, float]:
    """
    Compute (lower, upper) bounds using IQR (1.5 * IQR).
    Returns numeric bounds; if column non-numeric or can't compute, returns (None, None).
    """
    try:
        q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        return float(lower), float(upper)
    except Exception:
        return (None, None)

def impute_numeric_median(df: DataFrame, cols: Optional[List[str]] = None) -> DataFrame:
    """
    Impute numeric columns with median (approxQuantile).
    Returns new DataFrame with filled values.
    """
    numeric = cols or get_numeric_columns(df)
    fill_values = {}
    for c in numeric:
        try:
            med = df.approxQuantile(c, [0.5], 0.01)[0]
            if med is None:
                continue
            fill_values[c] = med
        except Exception:
            continue
    if fill_values:
        return df.na.fill(fill_values)
    else:
        return df
