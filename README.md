# Hatched Analytics Data Engineering Assessment

This project involves transforming period-based order index data (monthly, weekly, quarterly, etc.) into daily values and building a forecasting pipeline to estimate quarterly totals mid-quarter using linear extrapolation and seasonal patterns. The solution is implemented in Python with Pandas, with notes on scaling to Spark/Scala for production datasets.


# How to run the scripts

## Step 1: Daily forecast
python daily_transform.py ../data/index.csv ../outputs/daily_output.csv

## Step 2: Quarterly forecast
python quarterly_forecast.py ../data/index.csv ../outputs/quarterly_forecast.csv
