
import sqlite3
import datetime as dt
# import pandas as pd
# NOTES: yfinance API is restricted to 2000 requests per hour, or about 1 every 2 seconds
from stock_functions import get_ohlc
# create a metadata table which stores the last updated time
def create_metadata_table(table_name:str='stock_metadata',
                          db_name:str='stocks.db',
                          table_column_types:list={'ticker VARCHAR UNIQUE',
                                                   'name VARCHAR',
                                                   'industry VARCHAR',
                                                    'first_date DATETIME',
                                                    'last_date DATETIME',
                                                    'last_updated DATETIME',
                                                    'rowcount INTEGER',}):
      con = sqlite3.connect(db_name)
      with con:
            con.execute(f"CREATE TABLE IF NOT EXISTS '{table_name}'({','.join(table_column_types)});")

# create a stock table which stores the last updated time

def create_ticker_table(table_name,
                           db_name:str='stocks.db',
                             table_column_types:list=["'date' DATETIME PRIMARY KEY",
                                                      "'open' REAL",
                                                      "'high' REAL",
                                                      "'low' REAL",
                                                      "'close' REAL",
                                                      "'volume' REAL",
                                                      "'dividends' REAL",
                                                      "'stock splits' REAL"]):
    con = sqlite3.connect(db_name)
    with con:
        # throw an error if the table already exists.
        con.execute(f'CREATE TABLE "{table_name}" ({",".join(table_column_types)});')



def initial_data_pull(table_name:str, 
        start_date:str='2012-01-01',
        end_date:str='2024-01-01',
        db_name='stocks.db', METADATA_TABLE:str='stock_metadata'):

    create_ticker_table(table_name)
    data = get_ohlc(ticker_name=table_name, start_date=start_date, end_date=end_date)
    
    con = sqlite3.connect(db_name)
    columns = ",".join([f"'{x}'" for x in data.columns])
    with con:
        try:
            for row in data.iterrows():
                values = ",".join([f"'{x}'" for x in row[1].values])
                #print(f'INSERT INTO {table_name} ({columns}) VALUES ({values})')
                con.execute(f'INSERT INTO {table_name} ({columns}) VALUES ({values})')
        except:
            con.rollback()
    

def create_ticker_metadata(ticker, name, industry, METADATA_TABLE='stock_metadata'):
    con = sqlite3.connect('stocks.db')
    with con:
        try:
            con.execute(f'INSERT INTO {METADATA_TABLE} (ticker, name, industry, first_date, last_date, last_updated ) VALUES ("{ticker}", "{name}", "{industry}", NULL, NULL, "{dt.datetime.now().date()}");')
            con.commit()
        except BaseException as e:
            print(e)
            con.rollback()

