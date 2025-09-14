import sys
import pandas as pd
from datetime import timedelta

# Assigns a numerical key for different duration granularities
# This helps in sorting/grouping periods consistently
def get_gran_key(dur):
    dur = dur.lower()
    if 'week' in dur: return 1
    if 'mid-month' in dur: return 2
    if 'month' in dur: return 3
    if 'quarter' in dur: return 4
    if 'year' in dur: return 5
    return 6  # fallback for unexpected cases


# Infers the start and end date of a period based on DURATION
def infer_period(row):
    # Convert period end to datetime and adjust (end = last day of period)
    end = pd.to_datetime(row.PERIODEND) - timedelta(days=1)
    dur = row.DURATION.lower()

    # Derive start date depending on the type of duration
    if 'mid-month' in dur or 'month' in dur:
        start = end.replace(day=1)  # first day of month
    elif 'week' in dur:
        start = end - timedelta(days=6)  # last 7-day period
    elif 'quarter' in dur:
        start = end - timedelta(days=90)  # approx. 3 months
    elif 'year' in dur:
        start = end.replace(month=1, day=1)  # start of year
    else:
        start = end  # fallback (single-day period)

    return start, end


# Main transformation: Convert aggregated data (monthly/weekly/etc.) into daily-level data
def daily_transform(input_path, output_path):
    # Load input CSV
    df = pd.read_csv(input_path)
    df['PERIODEND'] = pd.to_datetime(df['PERIODEND'], format='%d/%m/%y')

    # Assign granularity key to each row
    df['gran_key'] = df['DURATION'].apply(get_gran_key)

    # Group data by ticker + index
    grouped = df.groupby(['TICKER', 'INDEXNAME'])

    daily_rows = []  # container for daily-level records

    # Process each ticker/index combination
    for (ticker, indexname), group in grouped:
        # Sort by granularity and end date
        g = group.sort_values(['gran_key', 'PERIODEND']).reset_index(drop=True)
        daily_values = {}  # map of date → daily value

        # Iterate through periods for this group
        for _, row in g.iterrows():
            start, end = infer_period(row)
            dates = pd.date_range(start=start, end=end, freq='D')
            if len(dates) == 0:
                continue

            # Calculate how much value is already allocated to these dates
            current_sum = sum(daily_values.get(d, 0) for d in dates)

            # Remaining value to allocate for this period
            remaining_value = row['VALUE'] - current_sum

            # Identify dates not yet covered
            uncovered_dates = [d for d in dates if daily_values.get(d, 0) == 0]

            if len(uncovered_dates) == 0 or remaining_value <= 0:
                continue

            # Distribute remaining value equally across uncovered dates
            daily_add = remaining_value / len(uncovered_dates)

            for d in uncovered_dates:
                daily_values[d] = daily_add

        # Build daily rows for this ticker/index
        for d in sorted(daily_values.keys()):
            daily_rows.append({
                'TICKER': ticker,
                'DURATION': 'Daily',        # mark as daily granularity
                'PERIODEND': d,             # actual calendar date
                'INDEXNAME': indexname,
                'VALUE': daily_values[d],   # daily value
                'CUMULATIVEVALUE': 0        # will be computed later
            })

    # If no rows were generated, exit gracefully
    if not daily_rows:
        print("No data to process.")
        return

    # Convert list of dicts → DataFrame
    daily_df = pd.DataFrame(daily_rows)

    # Sort final output for readability
    daily_df = daily_df.sort_values(['TICKER', 'INDEXNAME', 'PERIODEND'])

    # Compute cumulative values per ticker/index
    daily_df['CUMULATIVEVALUE'] = daily_df.groupby(['TICKER', 'INDEXNAME'])['VALUE'].cumsum()

    # Format dates nicely for output
    daily_df['PERIODEND'] = daily_df['PERIODEND'].dt.strftime('%Y-%m-%d')

    # Format RELEASEDDATE column if it exists (optional field in some files)
    if 'RELEASEDDATE' in daily_df.columns:
        daily_df['RELEASEDDATE'] = daily_df['RELEASEDDATE'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if pd.notnull(x) else ''
        )

    # Save to output CSV
    daily_df.to_csv(output_path, index=False)
    print(f"Daily data saved to {output_path}")


# CLI entrypoint
if __name__ == "__main__":
    # Ensure correct usage: python script.py --input <input-path> --output <output-path>
    if len(sys.argv) != 5 or sys.argv[1] != "--input" or sys.argv[3] != "--output":
        print("Usage: python script.py --input <input-path> --output <output-path>")
        sys.exit(1)

    input_path = sys.argv[2]
    output_path = sys.argv[4]

    # Run transformation
    daily_transform(input_path, output_path)
