import requests
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine, inspect
import holidays
import mysql.connector
import yfinance as yf
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

def download_bhavcopy(bhavcopy_url, date):
    try:
        day_of_week = datetime.strptime(date, "%d%m%Y").weekday()
        if day_of_week in [5, 6]:
            print(f"{date} is a weekend. So Skipping it")
            return 
        response = requests.get(bhavcopy_url, timeout=5)
        response.raise_for_status()
        if response.status_code == 200:
            data = pd.read_csv(StringIO(response.text))
            return data
    except requests.exceptions.RequestException as e:
        print(f"Failed to download Bhavcopy for {date}: {e}")
    except requests.exceptions.Timeout:
        print(f"Timeout occurred for {date}. So Skipping it")


def insert_into_db(final_df):
    try:
        # Create SQLAlchemy engine
        engine = create_engine('mysql+mysqlconnector://root:@localhost/trading')
        inspector = inspect(engine)
        table_name = 'nse_stock_data'

        # Fill NaN values in 'delivered_percentage'
        final_df['delivered_percentage'].fillna(0, inplace=True)

        # Check if the table exists and insert data accordingly
        if inspector.has_table(table_name):
            final_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print("Data appended to MySQL database successfully.")
        else:
            final_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print("Table created and data inserted into MySQL database successfully.")

        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def rename_columns(raw_data):
    column_mapping = {"SYMBOL": "symbol",
                      " SERIES": "series",
                      " DATE1": "stock_date",
                      " PREV_CLOSE": "previous_close",
                      " OPEN_PRICE": "open_price",
                      " HIGH_PRICE": "high_price",
                      " LOW_PRICE": "low_price",
                      " LAST_PRICE": "last_traded_price",
                      " CLOSE_PRICE": "close_price",
                      " AVG_PRICE": "avg_price",
                      " TTL_TRD_QNTY": "total_trade_qty",
                      " TURNOVER_LACS": "turnover_in_lacs",
                      " NO_OF_TRADES": "no_of_trades",
                      " DELIV_QTY": "delivered_qty",
                      " DELIV_PER": "delivered_percentage"}
    final_df = raw_data.rename(columns=column_mapping)
    return final_df

def generate_working_days(start_date, end_date):
    indian_holidays = get_indian_holidays(start_date.year)
    working_days = []
    current_date = start_date
    while current_date <= end_date:
        # Check if the current day is a working day (Monday to Friday) and not a holiday
        if current_date.weekday() < 5 and current_date not in indian_holidays:
            working_days.append(current_date.strftime("%Y-%m-%d"))

        # Move to the next day
        current_date += timedelta(days=1)
    return working_days

def get_indian_holidays(year):
    indian_holidays = holidays.India(years=year)
    return indian_holidays

def strip_whitespace(value):
    return value.strip() if isinstance(value, str) else value

if __name__ == "__main__":
    df_combined_columns = pd.DataFrame()
    
    start_date = datetime.strptime("2024-08-23", "%Y-%m-%d")
    end_date = datetime.strptime("2024-08-23", "%Y-%m-%d")
    
    dates = generate_working_days(start_date, end_date)
    print(dates)
    
    for date_str in dates:
        formatted_date = datetime.strptime(date_str, '%Y-%m-%d')
        date = formatted_date.strftime('%d%m%Y')
        bhavcopy_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
        raw_data = download_bhavcopy(bhavcopy_url, date)
        if raw_data is not None:
            raw_data = raw_data.applymap(strip_whitespace)
            raw_data = raw_data[raw_data[' SERIES'] == "EQ"]
            df_combined_columns = pd.concat([df_combined_columns, raw_data], axis=0)
            try:
                df_combined_columns[' DATE1'] = df_combined_columns[' DATE1'].apply(lambda x: str(pd.to_datetime(x))[:10])
            except ValueError as e:
                print("Error converting dates:", e)
                
        final_df = rename_columns(df_combined_columns)    
        final_df['delivered_percentage'] = pd.to_numeric(final_df['delivered_percentage'], errors='coerce')
        final_df['delivered_percentage'].fillna(0, inplace=True)

    insert_into_db(final_df)
    output_file_path = f"/Users/hemantdayma/Documents/NSE_Bhavcopydata/data/output/nse_delivery_stock_{date}.csv"
    final_df.to_csv(output_file_path, index=False)
    print(f"for the data {date_str},path : {output_file_path}")