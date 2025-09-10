import pandas as pd
import numpy as np
import argparse

def quarterly_forecast(input_csv, output_csv):

    df = pd.read_csv(input_csv, parse_dates=["DATE"])

    df["QUARTER_START"] = df["DATE"].dt.to_period("Q").dt.start_time
    df["QUARTER_END"] = df["DATE"].dt.to_period("Q").dt.end_time
    df["YEAR"] = df["DATE"].dt.year
    df["QUARTER"] = df["DATE"].dt.quarter

    results = []

    grouped = df.groupby(["TICKER", "INDEXNAME", "YEAR", "QUARTER", "QUARTER_START", "QUARTER_END"])

    for (ticker, indexname, year, quarter, qstart, qend), group in grouped:
        total_days = (qend - qstart).days + 1
        observed_days = group["DATE"].nunique()
        value_to_date = group["DAILYVALUE"].sum()

        # Linear extrapolation
        avg_daily = value_to_date / observed_days
        pred_linear = avg_daily * total_days

        # Seasonality based on previous year same quarter
        prev_group = df[
            (df["TICKER"] == ticker) &
            (df["INDEXNAME"] == indexname) &
            (df["YEAR"] == year - 1) &
            (df["QUARTER"] == quarter)
        ]
        if not prev_group.empty:
            prev_total = prev_group["DAILYVALUE"].sum()
            prev_avg = prev_total / len(prev_group["DATE"].unique())
            growth_factor = avg_daily / prev_avg if prev_avg > 0 else np.nan
            pred_seasonal = prev_total * growth_factor
        else:
            pred_seasonal = np.nan

        results.append({
            "TICKER": ticker,
            "INDEXNAME": indexname,
            "YEAR": year,
            "QUARTER": quarter,
            "QUARTER_START": qstart,
            "QUARTER_END": qend,
            "OBSERVED_DAYS": observed_days,
            "TOTAL_DAYS": total_days,
            "VALUE_TO_DATE": value_to_date,
            "PREDICTED_TOTAL_LINEAR": pred_linear,
            "PREDICTED_TOTAL_SEASONAL": pred_seasonal
        })

    forecast_df = pd.DataFrame(results)
    forecast_df.to_csv(output_csv, index=False)
    print(f"Quarterly forecast saved to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate quarterly forecasts from daily index data.")
    parser.add_argument("--input", required=True, help="Input daily CSV file path")
    parser.add_argument("--output", required=True, help="Output forecast CSV file path")
    args = parser.parse_args()

    quarterly_forecast(args.input, args.output)
