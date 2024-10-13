import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from utilss.constant import DATABASE_HOST, DATABASE_NAME, DATABASE_PORT, DATABASE_USERNAME, DATABASE_PASSWORD, \
    DATABASE_TABLE, OUTPUT_PATH


class DataExporter:
    def __init__(self, output_path=None):
        self.db_config = {
            'host': DATABASE_HOST,
            'database': DATABASE_NAME,
            'user': DATABASE_USERNAME,
            'password': DATABASE_PASSWORD,
            'port': DATABASE_PORT
        }
        self.OUTPUT_PATH = output_path if output_path else OUTPUT_PATH
        self.table_name = DATABASE_TABLE

    def create_connection(self):
        """Create a database connection and return it."""
        try:
            connection = mysql.connector.connect(**self.db_config)
            return connection
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None

    def close_connection(self, connection):
        """Close the database connection."""
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed.")

    def save_to_excel(self, df, working_days):
        try:
            file_path = f"{self.OUTPUT_PATH}nse_delivery_stock_{working_days[0]}.csv"
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            df.to_csv(file_path, index=False)
            print(f"Data successfully saved to folder for the Date {working_days[0]}")
        except Exception as e:
            print(f"Failed to save data to Excel: {e}")

    def insert_into_mysql(self, df):
        try:
            # Replace invalid date values with '0000-00-00' for date columns
            df['52_week_high_date'] = df['52_week_high_date'].replace({'': None, '-': None, '0': None, 0: None})
            df['52_week_low_date'] = df['52_week_low_date'].replace({'': None, '-': None, '0': None, 0: None})

            # Replace invalid numeric values with None
            df['52_week_high'] = pd.to_numeric(df['52_week_high'], errors='coerce')
            df['52_week_low'] = pd.to_numeric(df['52_week_low'], errors='coerce')

            # Convert DataFrame NaNs to None
            df = df.where(pd.notnull(df), None)

            # Connect to MySQL database
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            # Create a list of column names and placeholders for SQL query
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            update_columns = ', '.join([f"{col} = VALUES({col})" for col in df.columns if col not in ['id']])

            # SQL for insert or update
            sql = f"""INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})
                  ON DUPLICATE KEY UPDATE {update_columns}"""

            # Insert data row by row
            for row in df.itertuples(index=False, name=None):
                try:
                    cursor.execute(sql, row)
                except Error as e:
                    print(f"Failed to insert or update record: {e}")

            # Commit the transaction
            connection.commit()
            print(f"Data successfully inserted into {self.table_name} table.")

        except Error as e:
            print(f"Failed to insert data into MySQL: {e}")

        finally:
            # Close the database connection
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

    def insert_mvr_data(self, df):
        print("-",*100)
        try:
            weekend_days = ['Sat', 'Sun']
            
            # Filter out rows with 'Sat' or 'Sun' in the 'Days' column
            df = df[~df['Days'].isin(weekend_days)]
            
            # If there's no data left to process after filtering, exit the function
            if df.empty:
                print("No data to process after filtering out weekends.")
                
            # Proceed with processing the remaining data
            connection = self.create_connection()
            if connection is None:
                return  # Exit if connection failed
            
            cursor = connection.cursor()

            for scan_type, group_df in df.groupby('scan_type'):
                if scan_type == 'macd':
                    table_name = 'macd_data'
                elif scan_type == 'vcp':
                    table_name = 'rocket_base_data'
                elif scan_type == 'rocket':
                    table_name = 'vcp_data'
                else:
                    print("Found Extra scan_type in dataframe")
                    continue

                # Drop scan_type column before inserting
                group_df = group_df.drop(columns=['scan_type'], errors='ignore')

                columns = ', '.join(group_df.columns)
                placeholders = ', '.join(['%s'] * len(group_df.columns))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {', '.join([f'{col}=VALUES({col})' for col in group_df.columns])}"

                # Insert data row by row
                for row in group_df.itertuples(index=False, name=None):
                    cursor.execute(sql, row)
            
            # Commit the transaction
            connection.commit()
            print(f"Data successfully inserted into the {table_name} table.")

        except Error as e:
            print(f"Failed to insert data into MySQL: {e}")

        finally:
            # Close the database connection
            self.close_connection(connection)

