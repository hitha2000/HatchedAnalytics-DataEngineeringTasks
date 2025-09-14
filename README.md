# Hatched Analytics Data Engineering Assessment

## Project Overview
This repository contains the solution to the Hatched Analytics data engineering assessment. The goal of the assignment is to process time-series index data for publicly listed companies and provide the following outputs:
Daily Transformation – Convert weekly, monthly, or quarterly index data into daily values.
Quarterly Forecast – Estimate quarterly totals mid-quarter using available weekly/monthly data.

## Repository Structure
```
HatchedAnalytics-DataEngineeringTasks/
│
├── data/                     # Sample input file
│   └── index.csv
│
├── scripts/                  # Python scripts
│   ├── daily_transform.py
│   └── quarterly_forecast.py
│
├── outputs/                  # Generated CSVs
│   ├── daily_output.csv
│   └── quarterly_output.csv
│
├── README.md                 # Project documentation
└── .gitignore
```

## Requirements
* Python version: 3.9+
* Dependencies: pandas, python-dateutil
* Install dependencies using: 
```pip install pandas python-dateutil```

## Usage Instructions

## Task 1: Daily Transformation
* Converts all durations (weekly, monthly, quarterly) into daily values.
* Computes cumulative daily values per ticker/index.
* Command to run the script:
```python daily_transform.py --input ../data/index.csv --output ../outputs/daily_output.csv```

## Task 2: Quarterly Forecast
* Estimates quarterly totals using available weekly and monthly data.
* If a quarter is partially available (e.g., 1–3 weeks), extrapolates remaining days using daily rate.
* Command to run the script:
```python quarterly_forecast.py --input ../data/index.csv --output ../outputs/quarterly_output.csv```

## Assumptions Made
* Task 1: Quarters are approximated as ~90 days; years start from Jan 1.
* Task 2: Extrapolation is based on available Weeks + Months only.
* Dates are in %d/%m/%y format.
* If no data exists for a quarter, no estimate is made.

## Limitations & Edge Cases
* If only weekly data is available, extrapolation is used to estimate the quarter.
* Custom quarters may require adjustment to the logic.
* The estimate assumes a linear growth pattern from observed data.

## Challenges Faced

### Task 1: Daily Transformation
* Handling overlapping data sources for the same month was challenging. For example:
    * Weekly data points were available within a month.
    * Mid-month values were also provided.
    * A full monthly aggregate existed too.
* Ensuring correct precedence of weekly → mid-month → monthly values without double-counting required careful logic.
* The allocation of values across daily ranges also needed adjustments so that daily totals matched the reported higher-level aggregate.

### Task 2: Quarterly Forecast
* Incomplete months at the start or end of a quarter were straightforward to handle by prorating values.
* A bigger challenge arose when months and weeks overlapped within the same quarter (e.g., both an August total and individual weeks of August were reported).
* To avoid overestimation, the script had to detect overlaps and adjust daily/weekly contributions before extrapolating the quarterly forecast.

## Alternative Approaches and Scalability
* Python with pandas was chosen for simplicity, readability, and speed of implementation on small to medium datasets.
* For larger-scale production systems, Spark (Scala or PySpark) would be a strong candidate. Spark’s distributed processing could efficiently handle billions of rows while applying the same transformation and forecasting logic.
* Alternative approaches could also incorporate:
    * SQL-based pipelines for ETL transformations.
    * Time-series forecasting models to provide more sophisticated extrapolation beyond linear assumptions.

## Future Improvements
* Support custom quarterly definitions and fiscal calendars.
* Extend extrapolation logic to consider seasonal patterns or year-over-year trends.
