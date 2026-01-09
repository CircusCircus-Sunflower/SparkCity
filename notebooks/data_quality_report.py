# Minimal profiling script - run in terminal or paste into notebook cell
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark():
    return SparkSession.builder.master("local[1]").appName("DataQualityReport").getOrCreate()

spark = create_spark()
sample_files = sorted(glob.glob("data/samples/*.csv"))
if not sample_files:
    print("No sample CSVs found in data/samples/. Run the cleaning pipeline to create inline sample.")
else:
    for p in sample_files:
        print("== File:", p)
        df = spark.read.option("header","true").option("inferSchema","true").csv(p)
        print("Rows:", df.count(), "Columns:", len(df.columns))
        df.printSchema()
        total = df.count()
        nulls = {c: df.filter(F.col(c).isNull() | F.isnan(F.col(c))).count() for c in df.columns}
        print("Nulls (col:count):")
        for c,n in nulls.items():
            pct = (n/total*100) if total>0 else 0.0
            print(f"  {c}: {n} ({pct:.2f}%)")
        numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("double","float","int","long","decimal")]
        if numeric_cols:
            print("Numeric summary:")
            df.select(*numeric_cols).describe().show()
        print("Sample rows:")
        df.show(5, truncate=False)

spark.stop()
