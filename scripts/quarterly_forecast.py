import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys

# Function to get days in a month given PERIODEND
def get_days_in_month(period_end):
    data_end = period_end - timedelta(days=1)
    return data_end.day

# Function to get the quarter PERIODEND for a given month PERIODEND
def get_quarter_periodend(period_end):
    actual_end = period_end - timedelta(days=1)
    month = actual_end.month
    year = actual_end.year
    q = (month - 1) // 3 + 1
    q_end_month = q * 3
    end_year = year
    if q_end_month > 12:
        q_end_month -= 12
        end_year += 1
    if q_end_month < 12:
        return datetime(end_year, q_end_month + 1, 1)
    else:
        return datetime(end_year + 1, 1, 1)

# Function to estimate quarterly value for an incomplete quarter
def estimate_quarter_value(available_months_df, total_month_periodends):
    sum_so_far = available_months_df['VALUE'].sum()
    days_so_far = sum(get_days_in_month(p) for p in available_months_df['PERIODEND'])
    total_days = sum(get_days_in_month(p) for p in total_month_periodends)
    days_left = total_days - days_so_far
    if days_so_far == 0 or days_left <= 0:
        return None
    daily_rate = sum_so_far / days_so_far
    estimated_remaining = daily_rate * days_left
    return sum_so_far + estimated_remaining

def quarterly_forecast(input_path, output_path):

    df = pd.read_csv(input_path)

    df['PERIODEND'] = pd.to_datetime(df['PERIODEND'], format="%d/%m/%y")

    CURRENT_DATE = datetime.now()

    expected_columns = ['TICKER','DURATION','PERIODEND','INDEXNAME','VALUE','CUMULATIVEVALUE','COMMENT','RELEASEDDATE']

    quarterly_df = df[df['DURATION'] == 'Quarter'].copy()

    groups = df.groupby(['TICKER', 'INDEXNAME'])
    
    for name, group in groups:
        months_df = group[group['DURATION'] == 'Month'].sort_values('PERIODEND')
        if months_df.empty:
            continue
        
        periodends = sorted(months_df['PERIODEND'].unique())
        
        min_p = periodends[0]
        max_p = periodends[-1]

        start_q = get_quarter_periodend(min_p)
        end_q = get_quarter_periodend(max_p)
 
        current_q = start_q
        q_list = []
        while current_q <= end_q:
            q_list.append(current_q)
            current_q += relativedelta(months=3)

        prev_cumulative = 0  

        for q_periodend in q_list:

            existing = quarterly_df[
                (quarterly_df['TICKER'] == name[0]) &
                (quarterly_df['INDEXNAME'] == name[1]) &
                (quarterly_df['PERIODEND'] == q_periodend)
            ]
            if not existing.empty:
                prev_cumulative = existing['CUMULATIVEVALUE'].iloc[0] or existing['VALUE'].iloc[0]
                continue

            month3 = q_periodend
            month2 = q_periodend - relativedelta(months=1)
            month1 = q_periodend - relativedelta(months=2)
            month_periodends = [month1, month2, month3]

            available_months_df = months_df[months_df['PERIODEND'].isin(month_periodends)]
            num_available = len(available_months_df)
            
            if num_available == 3:
                value = available_months_df['VALUE'].sum()
                comment = 'Computed sum of months'
                released_date = q_periodend.strftime('%H:%M:%S')
            elif num_available > 0 and num_available < 3 and q_periodend > CURRENT_DATE:
                value = estimate_quarter_value(available_months_df, month_periodends)
                if value is None:
                    continue
                comment = 'Estimated using daily rate extrapolation'
                released_date = CURRENT_DATE.strftime('%H:%M:%S')
            else:
                continue

            cumulative_value = prev_cumulative + value
            
            # Create new row as dict
            new_row = {
                'TICKER': name[0],
                'DURATION': 'Quarter',
                'PERIODEND': q_periodend,
                'INDEXNAME': name[1],
                'VALUE': value,
                'CUMULATIVEVALUE': cumulative_value,
                'COMMENT': comment,
                'RELEASEDDATE': released_date
            }
            
            new_row_df = pd.DataFrame([new_row]).reindex(columns=expected_columns)

            quarterly_df = pd.concat([quarterly_df, new_row_df], ignore_index=True)

            prev_cumulative = cumulative_value
    
    quarterly_df = quarterly_df.sort_values(['TICKER', 'INDEXNAME', 'PERIODEND'])

    quarterly_df = quarterly_df[expected_columns]
    
    quarterly_df['PERIODEND'] = quarterly_df['PERIODEND'].dt.strftime('%d/%m/%y')
    
    # Save the output CSV
    quarterly_df.to_csv(output_path, index=False)
    print(f"Output saved to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 5 or sys.argv[1] != "--input" or sys.argv[3] != "--output":
        print("Usage: python script.py --input <input-path> --output <output-path>")
        sys.exit(1)

    input_path = sys.argv[2]
    output_path = sys.argv[4]

    quarterly_forecast(input_path, output_path)