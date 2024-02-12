import sqlite3
import pandas as pd
# NOTES: yfinance API is restricted to 2000 requests per hour, or about 1 every 2 seconds
from stock_functions import get_ohlc
import datetime as dt

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
def read_table(from_clause:str,
                select_clause:str = "SELECT *",
                where_clause:str='',
                group_by:str="",
                order_by:str=""):
    '''spaces between clauses already exist'''

    con = sqlite3.connect('stocks.db')
    with con:
        res = con.execute(f'{select_clause} FROM {from_clause} {where_clause} {group_by} {order_by}')
        colnames = [x[0] for x in res.description]
    
    return pd.DataFrame(res.fetchall(), columns=colnames)#.set_index('date')


def update_ticker_metadata(table_name, METADATA_TABLE='stock_metadata'):
    con = sqlite3.connect('stocks.db')
  
    with con:
        try:
            rowcount, first_date, last_date = list(con.execute(f'select count(*) as rowcount, min(date) as max_date, max(date) as max_date from "{table_name}"').fetchall()[0])
            con.execute(f"UPDATE {METADATA_TABLE} SET first_date = '{first_date}', last_date = '{last_date}', rowcount = {rowcount}, last_updated = '{(dt.datetime.now().date())}' WHERE {METADATA_TABLE}.ticker = '{table_name}';")
            con.commit()
        except BaseException as e:
            print(e)
            con.rollback()

