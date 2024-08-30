import streamlit as st
import joblib
import pandas as pd
import matplotlib.pyplot as plt
import mplcyberpunk
from sqlalchemy import create_engine
import numpy as np
from datetime import timedelta


def load_data():
    engine = create_engine(
        "postgresql+psycopg2://airflow:password@localhost:5432/IndicesDB")

    sql = "SELECT * FROM nifty50"
    nifty50 = pd.read_sql(sql=sql, con=engine, parse_dates=[
                          "date"], index_col="Date")

    sql = "SELECT * FROM sensex"
    sensex = pd.read_sql(sql=sql, con=engine, parse_dates=[
                         "date"], index_col="Date")

    sql = "SELECT * FROM midcap100"
    midcap100 = pd.read_sql(sql=sql, con=engine, parse_dates=[
                            "date"], index_col="Date")

    return {"nifty50": nifty50, "sensex": sensex, "midcap100": midcap100}


def plot_garch(df, selected_index, horizon):
    model_path = f"airflow/dags/utils/{selected_index}_garch_model.pkl"
    model = joblib.load(model_path)

    forecast = model.forecast(horizon=horizon)
    pred_volatility = np.sqrt(forecast.variance.values[-1, :])

    last_year_data = df['adj_close'][-252:]
    forecast_start_date = last_year_data.index[-1] + pd.Timedelta(days=1)
    forecast_dates = pd.date_range(
        start=forecast_start_date, periods=horizon, freq='B')

    forecast_dates = forecast_dates[:len(pred_volatility)]

    last_close = last_year_data.iloc[-1]
    forecasted_close = np.concatenate([
        last_year_data.values,
        np.linspace(last_close, last_close * 1.02, len(forecast_dates))
    ])

    combined_dates = last_year_data.index.append(forecast_dates)
    plt.style.use('cyberpunk')
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(last_year_data.index, last_year_data.values,
            label='Adj Close Price', color='blue')

    ax.plot(forecast_dates,
            forecasted_close[-horizon:], color='green', label='Forecasted Prices')

    ax.axvline(x=last_year_data.index[-1],
               color='red', linestyle='--', label='Today')

    ax.set_title('GARCH Volatility Forecast and Index Price Prediction')
    ax.set_xlabel('Date')
    ax.set_ylabel('Price/Volatility')
    ax.legend()
    mplcyberpunk.add_glow_effects()
    st.pyplot(fig)


def plot_historical_data(df, period):
    periods_dict = {'1y': 252, '3y': 252*3, '5y': 252*5}
    data_period = df['adj_close'][-periods_dict[period]:]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(data_period.index, data_period.values,
            color='green', label=f'{period} Historical Data')

    ax.set_title(f'{period} Historical Data')
    ax.set_xlabel('Year' if period == '3y' else 'Date')
    ax.set_ylabel('Price')
    ax.legend()
    plt.style.use('cyberpunk')
    mplcyberpunk.add_glow_effects()
    st.pyplot(fig)


st.title("Volatility Forecast Dashboard")

index_options = ["nifty50", "sensex", "midcap100"]
selected_index = st.selectbox("Select Index", options=index_options, index=0)

data = load_data()
df = data[selected_index]

prediction_horizon = st.slider(
    "Select Prediction Horizon (days)", min_value=1, max_value=10, value=5)

plot_garch(df, selected_index, prediction_horizon)

period = st.selectbox("Select Historical Data Period",
                      options=['1y', '3y', '5y'], index=0)

plot_historical_data(df, period)
