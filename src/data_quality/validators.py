"""
Type-aware validators for the MVP cleaning pipeline.

Includes:
- get_numeric_columns
- check_nulls
- check_duplicates
- iqr_outlier_bounds
- impute_numeric_median
"""
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

def get_numeric_columns(df: DataFrame) -> List[str]:
    """Return list of numeric column names."""
    return [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]

def is_numeric_field_type(field_type) -> bool:
    """Return True if the schema field type is numeric."""
    return isinstance(field_type, NumericType)

def check_nulls(df: DataFrame, threshold: float = 0.2) -> Dict[str, float]:
    """
    Return columns whose null fraction exceeds threshold.
    Uses type-aware checks to avoid calling isnan on non-numeric fields.
    Output: dict of col -> percent_null (0..100)
    """
    total = df.count()
    if total == 0:
        return {}
    results = {}
    for f in df.schema.fields:
        c = f.name
        try:
            if is_numeric_field_type(f.dataType):
                # numeric types: check NULL or NaN
                null_count = df.filter(F.col(c).isNull() | F.isnan(F.col(c))).count()
            else:
                # non-numeric: only check NULLs and empty strings
                null_count = df.filter(F.col(c).isNull() | (F.col(c) == "")).count()
        except Exception:
            # fallback: conservative approach - count nulls only
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

def iqr_outlier_bounds(df: DataFrame, col: str) -> Tuple[Optional[float], Optional[float]]:
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
