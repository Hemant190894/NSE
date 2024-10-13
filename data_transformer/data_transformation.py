import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

class BhavcopyTransformation:
    def __init__(self, raw_data):
        self.raw_data = raw_data
        
    def bhavcopy_transformation(self):
        # Ensure raw_data is not None
        if self.raw_data is not None and not self.raw_data.empty:
            # Trim spaces from 'series' column values
            self.raw_data['series'] = self.raw_data['series'].str.strip()
            
            # Filter the data for the 'EQ' series
            transformed_df = self.raw_data[self.raw_data['series'] == "EQ"].copy()            
            # Convert 'stock_date' to string format
            transformed_df['stock_date'] = pd.to_datetime(transformed_df['stock_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Convert 'delivered_percentage' to numeric and fill NaN values
            transformed_df['delivered_percentage'] = pd.to_numeric(transformed_df['delivered_percentage'], errors='coerce')
            transformed_df['delivered_percentage'] = transformed_df['delivered_percentage'].fillna(0)
            
            return transformed_df
        else:
            print("No data to transform.")
            return pd.DataFrame()

class YearlyhighlowTransformation:
    def __init__(self, row_final_df):
        self.row_final_df = row_final_df
    
    def Yearly_high_low_transformation(self):
        if self.row_final_df is not None and not self.row_final_df.empty:
            # Trim spaces from 'series' column values
            self.row_final_df['series'] = self.row_final_df['series'].str.strip()
            transformed_yealyHL_df = self.row_final_df[self.row_final_df['series'] == "EQ"].copy() 

            # Convert date columns
            transformed_yealyHL_df['52_week_high_date'] = pd.to_datetime(transformed_yealyHL_df['52_week_high_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            transformed_yealyHL_df['52_week_low_date'] = pd.to_datetime(transformed_yealyHL_df['52_week_low_date'], errors='coerce').dt.strftime('%Y-%m-%d')

            # Replace invalid date values with '0000-00-00' for MySQL compatibility
            transformed_yealyHL_df['52_week_high_date'] = transformed_yealyHL_df['52_week_high_date'].replace({'NaT': None})
            transformed_yealyHL_df['52_week_low_date'] = transformed_yealyHL_df['52_week_low_date'].replace({'NaT': None})

            # Handle '52_week_high' and '52_week_low'
            transformed_yealyHL_df['52_week_high'] = transformed_yealyHL_df['52_week_high'].replace({'': None, '0': None, '-': None})
            transformed_yealyHL_df['52_week_low'] = transformed_yealyHL_df['52_week_low'].replace({'': None, '0': None, '-': None})

            # Convert columns to numeric, replacing invalid values with NaN
            transformed_yealyHL_df['52_week_high'] = pd.to_numeric(transformed_yealyHL_df['52_week_high'], errors='coerce')
            transformed_yealyHL_df['52_week_low'] = pd.to_numeric(transformed_yealyHL_df['52_week_low'], errors='coerce')

            transformed_yealyHL_df.replace('-', pd.NA, inplace=True)

            return transformed_yealyHL_df
        else:
            print("No data to transform.")
            return pd.DataFrame()


class ChartlinkTransformation:
    def __init__(self, row_final_df):
        self.raw_data = row_final_df

    def chartlink_Transformation(self):
        if self.raw_data is not None and not self.raw_data.empty:
            # Add the stock_date column with the current date in "YYYY-MM-DD" format
            self.raw_data['stock_date'] = datetime.now().strftime("%Y-%m-%d")

            self.raw_data['Days'] = self.raw_data['stock_date'].strftime("%a")

            # Filter data based on scan_type and select only the required columns
            # Filter columns for insertion
            relevant_columns = ['nsecode', 'per_chg', 'stock_date', 'scan_type','Days']

            # Select only the relevant columns
            filtered_df = self.raw_data[relevant_columns]

            return filtered_df
        else:
            print("No data to transform for chartlink.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

class DataframeJoiner:
    def __init__(self, main_df, yearly_df):
        self.main_df = main_df
        self.yearly_df = yearly_df

    def join_dataframes(self):
        if not self.main_df.empty and not self.yearly_df.empty:
            # Merge the DataFrames on 'symbol', 'series', and 'stock_date'
            merged_df = pd.merge(
                self.main_df, 
                self.yearly_df[['symbol', 'series', 'stock_date', '52_week_high', '52_week_high_date', '52_week_low', '52_week_low_date']], 
                on=['symbol', 'series', 'stock_date'], 
                how='left'
            )

            # Fill any NaN values in the new columns with 0 or appropriate default values
            columns_to_fill = ['52_week_high', '52_week_high_date', '52_week_low', '52_week_low_date']
            merged_df[columns_to_fill] = merged_df[columns_to_fill].fillna({
                '52_week_high': 0,
                '52_week_high_date': '0',
                '52_week_low': 0,
                '52_week_low_date': '0'
            })

            return merged_df
        else:
            print("One or both DataFrames are empty. Cannot perform the join operation.")
            return self.main_df
        
    
    