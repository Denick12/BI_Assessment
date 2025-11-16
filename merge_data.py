# merge_calls.py
import pandas as pd
from pathlib import Path

# Dynamically creating the file_names (using list comprehension)
file_name = "File"
paths = [
    file_name+str(i)+".xlsx" for i in range (1,8)

]
# print(paths)

dfs = []
for p in paths:
    df = pd.read_excel(p)           
    dfs.append(df)

combined = pd.concat(dfs, ignore_index=True)

# --- Cleaning---

# Convert timestamp
combined["StartDateTime"] = pd.to_datetime(combined["StartDateTime"], errors="coerce")

# Numeric conversions
numeric_cols = [
    "Total_Handle_Time",
    "Total_Hold_Time",
    "Total_Talk_Time",
    "Total_Survey",
    "Average of Average_Survey"
]

for col in numeric_cols:
    combined[col] = pd.to_numeric(combined[col], errors="coerce")

# Trim whitespace from text fields
string_cols = ["UserId", "CallType", "disconnectType"]
for col in string_cols:
    combined[col] = combined[col].astype(str).str.strip()

# --- Output ---
combined.to_excel("combined_calls.xlsx", index=False)
print("Merged file written to combined_calls.xlsx")
print("Rows:", len(combined))
print("Columns:", combined.columns.tolist())