import requests
from bs4 import BeautifulSoup
import pandas as pd
from utilss.constant import SCANS

class ChartlinkExtraction:
    def __init__(self):
        self.scans = SCANS

    def get_csrf_token(self, session):
        url = "https://chartink.com/screener/macd-ema-20-50-200-crosing"  # Sample page to get CSRF token
        response = session.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        # Extract CSRF token
        csrf_token = soup.find('meta', {'name': 'csrf-token'})['content']
        return csrf_token

    def call_chartink_api(self, scan_clause, debug_clause, csrf_token, session):
        url = "https://chartink.com/screener/process"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://chartink.com",
            "Referer": "https://chartink.com/screener/200-by-stockexploder",
            "X-CSRF-TOKEN": csrf_token
        }
        payload = {
            "scan_clause": scan_clause,
            "debug_clause": debug_clause
        }
        try:
            response = session.post(url, headers=headers, data=payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return response.json()
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def run_scans(self):
        results = []
        with requests.Session() as session:
            # Get CSRF token
            csrf_token = self.get_csrf_token(session)
            # Loop through each scan type (MACD, VCP, ROCKET_BASE_SETUP)
            for scan_type, scan_data in self.scans.items():
                print(f"Processing {scan_type} Setup:")
                response = self.call_chartink_api(scan_data['scan_clause'], scan_data['debug_clause'], csrf_token, session)
                if response:
                    # Add scan type to the results and store in a DataFrame
                    df = pd.DataFrame(response['data'])  # Assuming the API returns a key 'data'
                    df['scan_type'] = scan_type
                    results.append(df)

        # Combine all DataFrames
        final_df = pd.concat(results, ignore_index=True)
        return final_df

