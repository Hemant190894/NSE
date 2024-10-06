import configparser
import os

parser = configparser.ConfigParser()

config_file_path = os.path.join(os.path.dirname(__file__), '../config/config.conf')
parser.read(config_file_path)

DATABASE_HOST = parser.get('mysql_database', 'my_database_host')
DATABASE_NAME = parser.get('mysql_database', 'my_database_name')
DATABASE_PORT = parser.getint('mysql_database', 'my_database_port')
DATABASE_USERNAME = parser.get('mysql_database', 'my_database_username')
DATABASE_PASSWORD = parser.get('mysql_database', 'my_database_password')
DATABASE_TABLE = parser.get('mysql_database', 'my_table')

BHAVCOPY_URL = parser.get('bhavcopy_url', 'url')

NSE_HL_URL = parser.get('Yearly_high_low', 'high_low_api')

INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

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


holiday_dict = {
    "22012024": "Special Holiday",
    "26012024": "Republic Day",
    "19022024": "Chhatrapati Shivaji Maharaj Jayanti",
    "08032024": "Mahashivratri",
    "25032024": "Holi",
    "29032024": "Good Friday",
    "01042024": "Annual Bank closing",
    "09042024": "Gudi Padwa",
    "11042024": "Id-Ul-Fitr (Ramadan Eid)",
    "17042024": "Ram Navami",
    "01052024": "Maharashtra Day",
    "20052024": "General Parliamentary Elections",
    "23052024": "Buddha Pournima",
    "17062024": "Bakri Eid",
    "17072024": "Moharram",
    "15082024": "Independence Day/ Parsi New Year",
    "02102024": "Mahatma Gandhi Jayanti",
    "01112024": "Diwali-Laxmi Pujan",
    "15112024": "Prakash Gurpurb Sri Guru Nanak Dev",
    "25122024": "Christmas"
}

URL_HEADERS = {
'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
'accept-encoding': 'gzip, deflate, br',
'accept-language': 'en-US,en;q=0.9',
'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'}

column_mapping_high_low = {"SYMBOL": "symbol",
         "SERIES": "series",
         "Adjusted 52_Week_High": "52_week_high",
         "52_Week_High_Date" : "52_week_high_date",
         "Adjusted 52_Week_Low" : "52_week_low",
          "52_Week_Low_DT": "52_week_low_date",
         "extract_date" : "stock_date"}

MACD = parser.get('macd', 'macd_url')
VCP = parser.get('vcp', 'vcp_link_url')
ROCKET_BASE_SETUP = parser.get('rocket_base_setup', 'rocket_base_setup_url')

SCANS = {
    "macd": {
        "scan_clause": "( {cash} ( [0] 1 hour macd line( 26 , 12 , 9 ) < [0] 1 hour macd histogram( 26 , 12 , 9 ) and [0] 1 hour macd line( 26 , 12 , 9 ) > [0] 1 hour macd signal( 26 , 12 , 9 ) and latest ema( latest close , 20 ) > latest ema( latest close , 50 ) and latest ema( latest close , 50 ) > latest ema( latest close , 200 ) ) )" ,
        "debug_clause": "groupcount( 1 where [0] 1 hour macd line( 26 , 12 , 9 ) < [0] 1 hour macd histogram( 26 , 12 , 9 )),groupcount( 1 where [0] 1 hour macd line( 26 , 12 , 9 ) > [0] 1 hour macd signal( 26 , 12 , 9 )),groupcount( 1 where daily ema( daily close , 20 ) > daily ema( daily close , 50 )),groupcount( 1 where daily ema( daily close , 50 ) > daily ema( daily close , 200 ))"
    },
    "vcp": {
        "scan_clause": "( {cash} ( latest wma( close,1 ) > monthly wma( close,2 ) + 1 and monthly wma( close,2 ) > monthly wma( close,4 ) + 2 and latest wma( close,1 ) > weekly wma( close,6 ) + 2 and weekly wma( close,6 ) > weekly wma( close,12 ) + 2 and latest wma( close,1 ) > 4 days ago wma( close,12 ) + 2 and latest wma( close,1 ) > 2 days ago wma( close,20 ) + 2 and latest close > 25 and latest close <= 500 and weekly volume > 85000 ) )",
        "debug_clause": "groupcount( 1 where daily wma( close,1 ) > monthly wma( close,2 ) + 1),groupcount( 1 where monthly wma( close,2 ) > monthly wma( close,4 ) + 2),groupcount( 1 where daily wma( close,1 ) > weekly wma( close,6 ) + 2),groupcount( 1 where weekly wma( close,6 ) > weekly wma( close,12 ) + 2),groupcount( 1 where daily wma( close,1 ) > 4 days ago wma( close,12 ) + 2),groupcount( 1 where daily wma( close,1 ) > 2 days ago wma( close,20 ) + 2),groupcount( 1 where daily close > 25),groupcount( 1 where daily close <= 500),groupcount( 1 where weekly volume > 85000)"
    },
    "rocket": {
        "scan_clause": "( {cash} ( latest avg true range( 14 ) < 10 days ago avg true range( 14 ) and latest avg true range( 14 ) / latest close < 0.08 and latest close > ( weekly max( 52 , weekly close ) * 0.75 ) and latest ema( latest close , 50 ) > latest ema( latest close , 150 ) and latest ema( latest close , 150 ) > latest ema( latest close , 200 ) and latest close > latest ema( latest close , 50 ) and latest close > 10 and latest close * latest volume > 1000000 ) )",
        "debug_clause": "groupcount( 1 where daily avg true range( 14 ) < 10 days ago avg true range( 14 )),groupcount( 1 where daily avg true range( 14 ) / daily close < 0.08),groupcount( 1 where daily close > ( weekly max( 52 , weekly close ) * 0.75 )),groupcount( 1 where daily ema( daily close , 50 ) > daily ema( daily close , 150 )),groupcount( 1 where daily ema( daily close , 150 ) > daily ema( daily close , 200 )),groupcount( 1 where daily close > daily ema( daily close , 50 )),groupcount( 1 where daily close > 10),groupcount( 1 where daily close * daily volume > 1000000)"
    }
}