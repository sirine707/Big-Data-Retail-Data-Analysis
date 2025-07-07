import os
import sys
import json
import pandas as pd
from pathlib import Path

def clean_column(series: pd.Series) -> pd.Series:
    """ Nettoie les chaînes : virgules, %, espaces, etc. """
    return series.astype(str).str.strip() \
                 .str.replace(",", ".", regex=False) \
                 .str.replace("%", "", regex=False) \
                 .str.replace(r"[^\d\.\-eE]", "", regex=True)

def infer_dtype(series: pd.Series) -> str:
    sample = series.dropna().astype(str).str.strip()

    if sample.empty:
        return "empty"

    # Nettoyage
    cleaned = clean_column(sample)

    # Tenter datetime
    try:
        pd.to_datetime(sample, errors="raise")
        return "datetime"
    except Exception:
        pass

    # Tenter float
    try:
        numeric_sample = pd.to_numeric(cleaned, errors="coerce")
        ratio = numeric_sample.notnull().sum() / len(sample)
        if ratio > 0.95:
            if (numeric_sample.dropna() % 1 == 0).all():
                return "int"
            return "float"
    except:
        pass

    return "string"

def extract_schema(csv_path, output_dir="/opt/airflow/version", version="v1"):
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path, nrows=500, dtype=str)

    schema = {
        "file_name": csv_path.name,
        "columns": {}
    }

    for col in df.columns:
        inferred_type = infer_dtype(df[col])
        schema["columns"][col] = inferred_type

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"DVM_{csv_path.stem}_{version}.json"

    with open(output_path, "w") as f:
        json.dump(schema, f, indent=4)

    print(f"✅ Schéma sauvegardé dans : {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python extract_schema.py <csv_path>")
        sys.exit(1)

    csv_path = sys.argv[1]
    extract_schema(csv_path)
