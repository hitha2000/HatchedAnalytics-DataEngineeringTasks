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

# Usage Instructions

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

# Assumptions Made
* Task 1: Quarters are approximated as ~90 days; years start from Jan 1.
* Task 2: Extrapolation is based on available Weeks + Months only.
* Dates are in %d/%m/%y format.
* If no data exists for a quarter, no estimate is made.

# Limitations & Edge Cases
* If only weekly data is available, extrapolation is used to estimate the quarter.
* Custom quarters may require adjustment to the logic.
* The estimate assumes a linear growth pattern from observed data.

# Why Python and Not Spark/Scala
* Python/pandas was chosen for simplicity, readability, and speed of implementation for small to medium datasets.
* The sample data provided in this assessment is relatively small, so pandas can handle it efficiently without the complexity of a distributed system.
* In a production setting, where input datasets could span billions of rows across multiple tickers and indices, the daily transformation and quarterly forecasting logic can be implemented using Spark (Scala or PySpark). Spark’s distributed processing allows scaling across clusters while applying the same core algorithms, including uniform daily interpolation and linear or seasonal forecasting.

# Future Improvements
* Support custom quarterly definitions and fiscal calendars.
* Extend extrapolation logic to consider seasonal patterns or year-over-year trends.
