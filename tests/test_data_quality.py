import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from data_quality.validators import check_nulls, check_duplicates, impute_numeric_median

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()

def test_check_nulls_simple(spark):
    df = spark.createDataFrame([Row(a=1,b=None), Row(a=None,b=2), Row(a=3,b=4)])
    res = check_nulls(df, threshold=0.3)  # 33% threshold
    assert "a" in res and res["a"] >= 33.0

def test_check_duplicates_simple(spark):
    df = spark.createDataFrame([Row(id=1), Row(id=1), Row(id=2)])
    res = check_duplicates(df, subset=["id"])
    assert res["dup_count"] == 1

def test_impute_median(spark):
    df = spark.createDataFrame([Row(v=1.0), Row(v=None), Row(v=3.0)])
    df2 = impute_numeric_median(df, ["v"])
    vals = [r.v for r in df2.select("v").collect()]
    assert all(v is not None for v in vals)
