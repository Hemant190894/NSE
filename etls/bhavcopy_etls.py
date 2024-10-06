import pandas as pd
import requests
from io import StringIO
from utilss.constant import BHAVCOPY_URL, column_mapping
from utilss.dates_utils import Dates
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

class BhavcopyExtraction:
    def __init__(self):
        self.BHAVCOPY_URL = BHAVCOPY_URL
        self.column_mapping = column_mapping

    def download_and_concat_bhavcopy(self, dates_list):
        all_data = []
        
        for date in dates_list:
            print(f"Processing data bhavcopy for the date {date}...")
            # Format the URL with the date

            url = self.BHAVCOPY_URL.format(date=date)
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                if response.status_code == 200:
                    data = pd.read_csv(StringIO(response.text))
                    all_data.append(data)  # Store the DataFrame
            except requests.exceptions.RequestException as e:
                print(f"Failed to download Bhavcopy for {date}: {e}")
            except requests.exceptions.Timeout:
                print(f"Timeout occurred for {date}. Skipping it.")

        if all_data:
            # Concatenate all DataFrames
            combined_df = pd.concat(all_data, ignore_index=True)
            return combined_df
        else:
            print("No data was downloaded.")
            return pd.DataFrame()

    def process_bhavcopy(self, dates_list):
        # Download and concatenate data for the given dates
        combined_df = self.download_and_concat_bhavcopy(dates_list)
        
        if not combined_df.empty:
            # Rename columns for the entire concatenated DataFrame
            final_df = combined_df.rename(columns=self.column_mapping)
            return final_df
        else:
            print("No data to process.")
            return pd.DataFrame()

        
    