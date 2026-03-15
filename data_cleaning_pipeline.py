import pandas as pd
import numpy as np

"""
Project: End-to-End Financial Data Pipeline
Phase 2: Automated Data Cleaning & Risk Flagging
"""


def run_cleaning_pipeline():
    try:
        # Load data
        df = pd.read_csv('raw_sales_data.csv')

        # FIX: Force clean the column names (removes hidden spaces in headers)
        df.columns = df.columns.str.strip()

        print("--- Step 1: Raw Data Loaded Successfully ---")
        print("Detected Columns:", df.columns.tolist())  # This will show us if the name is correct
    except FileNotFoundError:
        print("Error: raw_sales_data.csv not found.")
        return

    # 1. Strip whitespaces from Customer Names
    if 'CustomerName' in df.columns:
        df['CustomerName'] = df['CustomerName'].str.strip()
    else:
        print("Error: Column 'CustomerName' not found. Please check your CSV header.")
        return

    # 2. Standardize Date format
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')

    # 3. Remove Duplicates
    df = df.drop_duplicates(subset=['OrderID'])

    # 4. Handle Missing amounts
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    df['Amount'] = df['Amount'].fillna(df['Amount'].median())

    # 5. Risk Flagging
    df['Risk_Level'] = np.where(df['Amount'] > 2000, 'High Risk', 'Normal')

    # 6. Export cleaned report
    output_file = 'cleaned_audit_report.csv'
    df.to_csv(output_file, index=False)

    print(f"--- Step 2: Cleaning Complete! ---")
    print(f"--- Final Report Saved as: {output_file} ---")
    print(df.head())


if __name__ == "__main__":
    run_cleaning_pipeline()