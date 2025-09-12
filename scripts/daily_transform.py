import sys
import pandas as pd
from datetime import timedelta

def get_gran_key(dur):
    dur = dur.lower()
    if 'week' in dur: return 1
    if 'mid-month' in dur: return 2
    if 'month' in dur: return 3
    if 'quarter' in dur: return 4
    if 'year' in dur: return 5
    return 6

def infer_period(row):
    end = pd.to_datetime(row.PERIODEND) - timedelta(days=1)
    dur = row.DURATION.lower()
    if 'mid-month' in dur or 'month' in dur:
        start = end.replace(day=1)
    elif 'week' in dur:
        start = end - timedelta(days=6)
    elif 'quarter' in dur:
        start = end - timedelta(days=90)
    elif 'year' in dur:
        start = end.replace(month=1, day=1)
    else:
        start = end
    return start, end

def daily_transform(input_path, output_path):
    df = pd.read_csv(input_path)
    df['PERIODEND'] = pd.to_datetime(df['PERIODEND'], format='%d/%m/%y')

    df['gran_key'] = df['DURATION'].apply(get_gran_key)
    grouped = df.groupby(['TICKER', 'INDEXNAME'])

    daily_rows = []

    for (ticker, indexname), group in grouped:
        g = group.sort_values(['gran_key', 'PERIODEND']).reset_index(drop=True)
        daily_values = {} 

        for _, row in g.iterrows():
            start, end = infer_period(row)
            dates = pd.date_range(start=start, end=end, freq='D')
            if len(dates) == 0:
                continue

            # Calculate existing sum on these dates
            current_sum = sum(daily_values.get(d, 0) for d in dates)
            remaining_value = row['VALUE'] - current_sum
            uncovered_dates = [d for d in dates if daily_values.get(d, 0) == 0]

            if len(uncovered_dates) == 0 or remaining_value <= 0:
                continue

            daily_add = remaining_value / len(uncovered_dates)

            for d in uncovered_dates:
                daily_values[d] = daily_add

        # Build daily rows
        for d in sorted(daily_values.keys()):
            daily_rows.append({
                'TICKER': ticker,
                'DURATION': 'Daily',
                'PERIODEND': d,
                'INDEXNAME': indexname,
                'VALUE': daily_values[d],
                'CUMULATIVEVALUE': 0
            })

    if not daily_rows:
        print("No data to process.")
        return

    daily_df = pd.DataFrame(daily_rows)
    daily_df = daily_df.sort_values(['TICKER', 'INDEXNAME', 'PERIODEND'])
    daily_df['CUMULATIVEVALUE'] = daily_df.groupby(['TICKER', 'INDEXNAME'])['VALUE'].cumsum()

    # Format dates
    daily_df['PERIODEND'] = daily_df['PERIODEND'].dt.strftime('%Y-%m-%d')
    if 'RELEASEDDATE' in daily_df.columns:
        daily_df['RELEASEDDATE'] = daily_df['RELEASEDDATE'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if pd.notnull(x) else ''
        )

    daily_df.to_csv(output_path, index=False)
    print(f"Daily data saved to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 5 or sys.argv[1] != "--input" or sys.argv[3] != "--output":
        print("Usage: python script.py --input <input-path> --output <output-path>")
        sys.exit(1)

    input_path = sys.argv[2]
    output_path = sys.argv[4]

    daily_transform(input_path, output_path)

