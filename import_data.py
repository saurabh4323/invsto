import pandas as pd

file_path = "HINDALCO_1D (1).xlsx"

df = pd.read_excel(file_path, sheet_name=0)

print("First 5 rows of the dataset:")
print(df.head())

print("\nColumn Names:", df.columns)

print("\nMissing Values:\n", df.isnull().sum())

print("\nFirst 10 rows:")
print(df.head(10))

if 'datetime' in df.columns and 'close' in df.columns:
    print("\nDateTime and Close Price Data:")
    print(df[['datetime', 'close']].head(10))

if 'datetime' in df.columns:
    df['datetime'] = pd.to_datetime(df['datetime'])

print("\nLatest Entry:")
print(df.iloc[-1])
