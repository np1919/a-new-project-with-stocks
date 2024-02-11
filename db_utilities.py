import sqlite3
import pandas as pd
# NOTES: yfinance API is restricted to 2000 requests per hour, or about 1 every 2 seconds
from stock_functions import get_ohlc

def query(query):
    con = sqlite3.connect('stocks.db')
    with con:
        res = con.execute(" ".join([x.strip('\n').strip('\t').strip(' ') for x in query.split()]))
        colnames = [x[0] for x in res.description]
        output = pd.DataFrame(res.fetchall(), columns=colnames)
    # con.close()
    return output


def list_all_tables():
    con = sqlite3.connect('stocks.db')
    with con:
        res = con.execute("SELECT name FROM sqlite_master WHERE type='table';")
    output = list(res.fetchall())
    con.close()
    return [x[0] for x in output]


# create a metadata table which stores the last updated time
def create_metadata_table(table_name:str='stock_metadata',
                          db_name:str='stocks.db',
                          table_column_types:list={'ticker VARCHAR UNIQUE',
                                                      'first_date DATETIME',
                                                      'last_date DATETIME',}):
      con = sqlite3.connect(db_name)
      with con:
            con.execute(f"CREATE TABLE IF NOT EXISTS '{table_name}'({','.join(table_column_types)});")

# create a stock table which stores the last updated time

def create_ticker_table(table_name,
                           db_name:str='stocks.db',
                             table_column_types:list=["date DATETIME PRIMARY KEY",
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
        con.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({",".join(table_column_types)});')
    con.close()

def drop_ticker_table(table_name:str, db_name='stocks.db', METADATA_TABLE:str='stock_metadata'):
    con = sqlite3.connect(db_name)
    # drop the table
    with con:
        con.execute(f"DROP TABLE '{table_name}';")
        print(f'dropped table {table_name}')
        # update the metadata table
        # try:
        #     con.execute(f'UPDATE OR FAIL {METADATA_TABLE} SET first_date = NULL, last_date = NULL WHERE {METADATA_TABLE}.ticker = {table_name};')
        #     print(f'updated {METADATA_TABLE}')
        # except:
        #     print('metadata update failed')


# read the table
def read_table(table_name:str,
                select_clause:str = "SELECT *",
                where_clause:str='',
                group_by:str="",
                order_by:str=""):
    '''spaces between clauses already exist'''

    con = sqlite3.connect('stocks.db')
    with con:
        res = con.execute(f'{select_clause} FROM {table_name} {where_clause} {group_by} {order_by}')
    return res.fetchall()


def update_metadata(table_name:str, 
                    first_date:str,
                    last_update:str,
                    db_name='stocks.db', METADATA_TABLE:str='stock_metadata'):
    con = sqlite3.connect(db_name)

    with con:
        try:
            con.execute(f'UPDATE OR FAIL {METADATA_TABLE} SET first_date = {first_date}, last_date = {last_update} WHERE {METADATA_TABLE}.ticker = {table_name};')
        except BaseException as e:
            print(e)


def create_metadata(table_name, first_date, last_date, METADATA_TABLE='stock_metadata'):

    con = sqlite3.connect('stocks.db')
    with con:
        try:
            con.execute(f"INSERT INTO {METADATA_TABLE} (ticker, first_date, last_date) VALUES ({table_name}, {first_date}, {last_date})")
            con.commit()
        except:
            con.rollback()

def initial_data_pull(table_name:str, 
        start_date:str='2012-01-01',
        end_date:str='2024-01-01',
        db_name='stocks.db', METADATA_TABLE:str='stock_metadata'):

    create_ticker_table(table_name)
    con = sqlite3.connect(db_name)
    data = get_ohlc(ticker_name=table_name, start_date=start_date, end_date=end_date)

    columns = ",".join([f"'{x}'" for x in data.columns])
    with con:
        try:
            for row in data.iterrows():
                values = ",".join([f"'{x}'" for x in row[1].values])
                #print(f'INSERT INTO {table_name} ({columns}) VALUES ({values})')
                con.execute(f'INSERT INTO {table_name} ({columns}) VALUES ({values})')
        except:
            con.rollback()


    # new_first_date = min([data['Date'].min(), first_date])
    # new_last_date = max([data['Date'].max(), last_date])  
    # print(new_first_date, new_last_date)
    create_metadata(table_name, str(data['Date'].max())[:10], str(data['Date'].min())[:10], )

