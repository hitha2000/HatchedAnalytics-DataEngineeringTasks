import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sys

def get_days_in_month(period_end):
    data_end = period_end - timedelta(days=1)
    return data_end.day

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


def estimate_quarter_value(months_df, granular_df, month_periodends, current_date):
    # Sum full completed months (PERIODEND <= current_date)
    full_months = [p for p in month_periodends if p <= current_date]
    available_months_df = months_df[months_df['PERIODEND'].isin(full_months)]
    sum_full = available_months_df['VALUE'].sum()
    days_full = sum(get_days_in_month(p) for p in full_months)
    
    # Find ongoing month
    ongoing_month_periodend = None
    for p in month_periodends:
        if p > current_date:
            ongoing_month_periodend = p
            break
    if ongoing_month_periodend is None:
        return sum_full
    
    # Ongoing month details
    ongoing_month_start = ongoing_month_periodend - relativedelta(months=1) 
    ongoing_month_end = ongoing_month_periodend - timedelta(days=1)
    
    # Granular data for ongoing month (PERIODEND <= current_date, roughly in month)
    ongoing_granular = granular_df[
        (granular_df['PERIODEND'] <= current_date) &
        (granular_df['PERIODEND'] >= ongoing_month_start - relativedelta(months=1))  
    ].sort_values('PERIODEND')
    
    sum_partial = 0
    days_partial = 0
    
    for _, row in ongoing_granular.iterrows():
        period_end = row['PERIODEND']

        period_start = period_end - timedelta(days=6)
        total_days = (period_end - period_start).days + 1 

        start_date = max(period_start, ongoing_month_start)
        end_date = min(period_end, ongoing_month_end)
        number_of_days = (end_date - start_date).days + 1 if start_date <= end_date else 0
        
        if number_of_days > 0:
            # Prorate VALUE for ongoing month portion
            prorated_value = row['VALUE'] * (number_of_days / total_days)
            sum_partial += prorated_value
            days_partial += number_of_days
    
    total_quarter_days = sum(get_days_in_month(p) for p in month_periodends)
    days_so_far = days_full + days_partial
    days_left = total_quarter_days - days_so_far
    
    if days_so_far == 0 or days_left <= 0:
        return None
    
    # Use partial rate if available, else full months rate
    if days_partial > 0:
        daily_rate = sum_partial / days_partial
    else:
        daily_rate = sum_full / days_full if days_full > 0 else 0
    
    estimated_remaining = daily_rate * days_left
    return sum_full + sum_partial + estimated_remaining

def quarterly_forecast(input_path, output_path):
    df = pd.read_csv(input_path)
    df['PERIODEND'] = pd.to_datetime(df['PERIODEND'], format="%d/%m/%y")
    CURRENT_DATE = datetime.now() 

    expected_columns = ['TICKER','DURATION','PERIODEND','INDEXNAME','VALUE','CUMULATIVEVALUE','COMMENT','RELEASEDDATE']

    quarterly_df = df[df['DURATION'] == 'Quarter'].copy()

    groups = df.groupby(['TICKER', 'INDEXNAME'])
    
    for name, group in groups:
        months_df = group[group['DURATION'] == 'Month'].sort_values('PERIODEND')
        granular_df = group[group['DURATION'].isin(['Week', 'MidMonth'])].sort_values('PERIODEND')  # Include weeks/mid-months
        
        # Skip only if no data at all; allow weeks-only for estimation
        if months_df.empty and granular_df.empty:
            continue
        
        # If no months, use empty months_df but proceed with granular for ongoing quarters
        if months_df.empty:
            periodends = []  
            min_p = granular_df['PERIODEND'].min() if not granular_df.empty else CURRENT_DATE
            max_p = granular_df['PERIODEND'].max() if not granular_df.empty else CURRENT_DATE
            start_q = get_quarter_periodend(min_p)
            end_q = get_quarter_periodend(max_p)
        else:
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

            # Check full months available
            full_months = [p for p in month_periodends if p <= CURRENT_DATE]
            num_full = len(full_months)
            
            if num_full == 3:
                value = months_df[months_df['PERIODEND'].isin(month_periodends)]['VALUE'].sum()
                comment = 'Computed sum of months'
                released_date = q_periodend.strftime('00:00.0')
            elif num_full < 3 and q_periodend > CURRENT_DATE:
                value = estimate_quarter_value(months_df, granular_df, month_periodends, CURRENT_DATE)
                if value is None:
                    continue
                comment = 'Estimated using daily rate extrapolation with partial granular data'
                released_date = CURRENT_DATE.strftime('15:00.0') 
            else:
                continue

            cumulative_value = prev_cumulative + value
            
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
    
    quarterly_df.to_csv(output_path, index=False)
    print(f"Output saved to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) != 5 or sys.argv[1] != "--input" or sys.argv[3] != "--output":
        print("Usage: python script.py --input <input-path> --output <output-path>")
        sys.exit(1)
    input_path = sys.argv[2]
    output_path = sys.argv[4]
    quarterly_forecast(input_path, output_path)