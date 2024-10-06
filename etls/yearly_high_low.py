import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from utilss.constant import NSE_HL_URL, URL_HEADERS,column_mapping_high_low

class Yearlydata:
    def __init__(self):
        self.NSE_HL_URL = NSE_HL_URL
        self.URL_HEADERS = URL_HEADERS
        self.column_mapping_high_low = column_mapping_high_low
        
    def download_and_concat_yearly_hl(self, dates_list):
        all_data = []
        
        for date in dates_list:
            print(f"Processing data for 52 week HL for {date}...")
            
            try:
                url = self.NSE_HL_URL.format(date=date)
                response = requests.get(url, headers=self.URL_HEADERS)
                response.raise_for_status()  # Check for HTTP errors
                
                lines = response.text.splitlines()
                filtered_lines = lines[2:]  # Assuming first two lines are headers
                cleaned_text = "\n".join(filtered_lines)
                data = StringIO(cleaned_text)
                
                column_names = ["SYMBOL", "SERIES", "Adjusted 52_Week_High", "52_Week_High_Date", 
                                "Adjusted 52_Week_Low", "52_Week_Low_DT"]
                df = pd.read_csv(data, names=column_names, delimiter=',', skiprows=1, on_bad_lines='skip')
                
                # Add extraction date to the DataFrame
                df["extract_date"] = datetime.strptime(date, '%d%m%Y').strftime('%Y-%m-%d')
                
                all_data.append(df)  # Append each DataFrame to the list
                
            except requests.exceptions.Timeout:
                print(f"Timeout occurred for {date}. Skipping it.")
            except requests.exceptions.RequestException as e:
                print(f"Failed to download data for {date}: {e}")
        
        # Concatenate all DataFrames into a single DataFrame
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df

    def process_yearly_high_low(self, dates_list):
        # Download and concatenate data for the given dates
        combined_df = self.download_and_concat_yearly_hl(dates_list)
        
        if not combined_df.empty:
            # Rename columns for the entire concatenated DataFrame
            final_df = combined_df.rename(columns=self.column_mapping_high_low)
            return final_df
        else:
            print("No data to process.")
            return pd.DataFrame()