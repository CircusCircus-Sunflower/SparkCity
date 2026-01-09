# Sample data README (MVP)

This folder contains small sample datasets used for development and QA.

How to regenerate cleaned samples:
1. Activate the venv:
   source .venv/bin/activate
2. Run the cleaning pipeline:
   python src/data_quality/cleaning_pipeline.py
3. Cleaned CSVs will appear in: data/processed/
4. Each cleaned CSV has a companion JSON quality file with summary.

Notes:
- Column types are inferred; some numeric columns may be stored as floats.
- Imputation strategy: numeric columns imputed with median.
- Outliers are capped using IQR method (1.5 * IQR).
