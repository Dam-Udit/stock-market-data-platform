import psycopg2
import pandas as pd
import requests
import datetime
from sqlalchemy import create_engine
from utils.config import api_key


class AlphaVantageAPI:
    ALLOWED_DURATIONS = ["1d", "3d", "1w", "1m", "3m", "6m", "1y", "3y", "5y"]

    def __init__(self, api_key=api_key):
        self.__api_key = api_key

    def __is_within_duration(self, duration, max_days):
        days = self.__convert_duration_to_days(duration)
        return days <= max_days

    def __convert_duration_to_days(self, duration):
        duration_map = {
            "1d": 1,
            "3d": 3,
            "1w": 7,
            "1m": 30,
            "3m": 90,
            "6m": 180,
            "1y": 365,
            "3y": 1095,
            "5y": 1825
        }
        return duration_map.get(duration, 0)

    def get_daily_data(self, ticker, duration):
        if duration not in self.ALLOWED_DURATIONS:
            raise ValueError(
                f"Invalid duration '{duration}'. Allowed values are: {', '.join(self.ALLOWED_DURATIONS)}")

        output_size = "compact" if self.__is_within_duration(
            duration, 90) else "full"

        url = (
            "https://www.alphavantage.co/query?"
            "function=TIME_SERIES_DAILY&"
            f"symbol={ticker}&"
            f"outputsize={output_size}&"
            f"datatype=json&"
            f"apikey={self.__api_key}"
        )

        response = requests.get(url=url)
        response_data = response.json()

        if "Time Series (Daily)" not in response_data:
            raise Exception(
                f"Invalid API call. Check that ticker symbol '{ticker}' is correct.")

        stock_data = response_data["Time Series (Daily)"]
        df = pd.DataFrame.from_dict(stock_data, orient="index", dtype=float)
        df.index = pd.to_datetime(df.index)
        df.index.name = "date"
        df.columns = [c.split(". ")[1] for c in df.columns]

        start_date = datetime.datetime.today(
        ) - datetime.timedelta(days=self.__convert_duration_to_days(duration))
        df = df[df.index >= start_date]

        return df


class SQLRepository:
    def __init__(self, engine):
        self.engine = engine

    def insert_stock_data(self, table_name, records, if_exists="fail"):
        n_inserted = records.to_sql(
            name=table_name, con=self.engine, if_exists=if_exists, index=True)
        return {
            "transactions_successful": True,
            "records_inserted": n_inserted
        }

    def read_table(self, table_name, limit=None):
        if limit:
            sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        else:
            sql = f"SELECT * FROM {table_name}"

        df = pd.read_sql(sql=sql, con=self.engine, parse_dates=[
                         "date"], index_col="date")
        return df