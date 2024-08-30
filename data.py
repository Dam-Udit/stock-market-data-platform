import pandas as pd
import datetime
import yfinance as yf
import sqlite3
import enum

class Duration(enum.Enum):
    ONE_DAY = "1d"
    THREE_DAYS = "3d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1m"
    THREE_MONTHS = "3m"
    SIX_MONTHS = "6m"
    ONE_YEAR = "1y"
    THREE_YEARS = "3y"
    FIVE_YEARS = "5y"

class YahooFinanceAPI:
    def __init__(self):
        pass

    def __get_dates(self, duration):
        today = datetime.date.today()
        if duration == "1d":
            start_date = today - datetime.timedelta(days=1)
        elif duration == "3d":
            start_date = today - datetime.timedelta(days=3)
        elif duration == "1w":
            start_date = today - datetime.timedelta(weeks=1)
        elif duration == "1m":
            start_date = today - datetime.timedelta(days=30)
        elif duration == "3m":
            start_date = today - datetime.timedelta(days=90)
        elif duration == "6m":
            start_date = today - datetime.timedelta(days=180)
        elif duration == "1y":
            start_date = today - datetime.timedelta(days=365)
        elif duration == "3y":
            start_date = today - datetime.timedelta(days=1095)
        elif duration == "5y":
            start_date = today - datetime.timedelta(days=1825)
        else:
            raise ValueError("Invalid duration")
        return start_date, today

    def get_stock_data(self, ticker, duration):
        start_date, end_date = self.__get_dates(duration)
        data = yf.download(ticker, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        data.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume"
        }, inplace=True)
        data['ticker'] = ticker
        return data

class SQLRepository:
    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name)

    def insert_table(self, table_name, records, if_exists='fail'):
        n_inserted = records.to_sql(
            name=table_name,
            con=self.connection,
            if_exists=if_exists,
            index=False
        )
        return {
            "transactions_successful": True,
            "records_inserted": n_inserted
        }

    def read_table(self, table_name, limit=None):
        if limit:
            sql = f"SELECT * FROM '{table_name}' LIMIT {limit}"
        else:
            sql = f"SELECT * FROM '{table_name}'"

        df = pd.read_sql(sql=sql, con=self.connection, parse_dates=["date"], index_col="date")
        return df