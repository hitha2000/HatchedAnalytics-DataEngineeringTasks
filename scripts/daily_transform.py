import pandas as pd
import numpy as np
from datetime import timedelta
import argparse

def is_leap_year(year):
    return year%4 ==0 and (year % 100 !=0 or year % 400 == 0)

def daily_transform(input_csv, output_csv):
    df = pd.read_csv(input_csv, parse_dates=["PERIODEND"])

    daily_rows = []

    for row in df.itertuples:
        ticker = row.TICKER
        duration = row.DURATION
        period_end = row.PERIODEND
        index_name = row.INDEXNAME
        value = row.VALUE

        if duration.lower() == "week":
            period_length = 7
        elif duration.lower() == "month":
            start_date = period_end.replace(day=1)
            period_length = (period_end - start_date).days + 1
        elif ("custom quarter" or "quarter") in duration.lower():
            # for simplicity i have assumed ~91 days for custom/standard quarters
            period_length = 91 
        elif duration.lower() == "year":
            period_length = 366 if is_leap_year(period_end.year) else 365
        else:
            period_length = 30

        start_date = period_end - timedelta(days= period_length-1)

        daily_value = value / period_length

        for i in range(period_length):
            current_day = start_date + timedelta(days=i)
            daily_rows.append({
                "TICKER": ticker,
                "DATE": current_day,
                "INDEXNAME": indexname,
                "DAILYVALUE": daily_value,
                "DURATION": duration,
                "PERIODEND": period_end
            })

        daily_df = pd.DataFrame(daily_rows)

        daily_df.to_csv(output_csv, index=False)
        print(f"Daily data saved to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform period-based index data into daily values.")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    args = parser.parse_args()

    daily_transform(args.input, args.output)