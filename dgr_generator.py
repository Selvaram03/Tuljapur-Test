# dgr_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Import the yearly fetcher from the other file
from mongo_connector import fetch_yearly_total

# Constants
DC_CAPACITY = 3.07     # MW
HOURS_IN_DAY = 24


def map_control_panel(inverter_no):
    """Maps Inverter No. to Control Room No. (1-9 LT1, 10-18 LT2)."""
    try:
        num = int(inverter_no.split('-')[-1])
        if 1 <= num <= 9:
            return "LT Panel 1"
        elif 10 <= num <= 18:
            return "LT Panel 2"
        else:
            return "N/A"
    except:
        return "N/A"


def calculate_daily_gti(df_raw: pd.DataFrame):
    """Calculates the Average Tilted Radiation (GTI) for each unique day."""
    df_raw['timestamp_dt'] = pd.to_datetime(df_raw['timestamp'])
    df_raw['Date'] = df_raw['timestamp_dt'].dt.strftime('%d-%b-%y')

    daily_gti_df = df_raw.groupby('Date')['Irradiation'].mean().reset_index()
    daily_gti_df = daily_gti_df.rename(columns={'Irradiation': 'Avg Tilted radiation in KWH/Mtr.Sq'})

    df_raw.drop(columns=['timestamp_dt'], inplace=True)
    return daily_gti_df


def generate_dgr_report(raw_data: list):
    """
    Generate Daily Generation Report (DGR).
    - Drops NaN values
    - Picks the last available value of the day per inverter as 'Daily Generation (KW)'
    - Monthly column is kept as 0 for now
    """
    if not raw_data:
        return pd.DataFrame()

    # --- Clean raw data ---
    df_raw = pd.DataFrame(raw_data).dropna(how="any")
    df_raw["timestamp_dt"] = pd.to_datetime(df_raw["timestamp"])
    df_raw["Date"] = df_raw["timestamp_dt"].dt.strftime("%d-%b-%y")

    all_days_data = []

    # --- For Daily Report, we only need the last record of each inverter ---
    value_vars = [col for col in df_raw.columns if col.startswith("Daily_Generation_INV")]

    for value_col in value_vars:
        # filter for this inverter column
        df_inv = df_raw[["timestamp_dt", "Date", value_col]].dropna()

        if not df_inv.empty:
            # pick last value of the day
            last_row = df_inv.iloc[-1]

            inverter_no = value_col.replace("Daily_Generation_INV", "Inverter No-")
            control_room = map_control_panel(inverter_no)

            all_days_data.append({
                "Date": last_row["Date"],
                "Control Room No.": control_room,
                "Inverter No.": inverter_no,
                "Daily Generation (KW)": last_row[value_col],
                "Monthly Generation (KW)": 0.0,   # for now, just show 0
            })

    # --- Build final DataFrame ---
    df_report = pd.DataFrame(all_days_data)

    # Merge yearly summary
    yearly_summary = fetch_yearly_total()
    df_report = pd.merge(df_report, yearly_summary, on="Inverter No.", how="left")

    # Calculate Avg GTI for the day
    Avg_GTI_day = df_raw["Irradiation"].mean() if "Irradiation" in df_raw.columns else np.nan
    df_report["Avg Tilted radiation in KWH/Mtr.Sq"] = Avg_GTI_day

    # --- PLF calculations (Daily only, Monthly = 0 for now) ---
    df_report["Daily PLF (%)"] = (df_report["Daily Generation (KW)"] / (DC_CAPACITY * HOURS_IN_DAY))
    df_report["Daily PLF (%)"] = df_report["Daily PLF (%)"].round(2).astype(str) + "%"

    df_report["Monthly PLF (%)"] = "0%"

    # --- Add placeholders for missing cols ---
    final_cols = [
        "Date", "Control Room No.", "Inverter No.", "Daily Generation (KW)", "Monthly Generation (KW)",
        "Yearly Generation (KW)", "Daily PLF (%)", "Monthly PLF (%)", "Yearly PLF (%)",
        "Generation Hrs. HR:MM", "U (Unsheduled) HR:MM", "S (Scheduled) HR:MM", "F.M (Force Measures) HR:MM",
        "GF (Grid Fault) HR:MM", "Grid OK Hrs.(6 am to 7 pm) HR:MM", "Total Grid Hrs.",
        "Plant Availability (%)", "Grid Availability (%)", "Remarks of the day",
        "Avg Tilted radiation in KWH/Mtr.Sq"
    ]

    for col in final_cols:
        if col not in df_report.columns:
            df_report[col] = np.nan

    # --- Fix MongoDB ObjectId for Streamlit/Arrow ---
    if "_id" in df_report.columns:
        df_report["_id"] = df_report["_id"].astype(str)

    return df_report[final_cols]
