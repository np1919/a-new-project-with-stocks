import pandas as pd
from db_utilities import query, list_all_tables, drop_ticker_table, read_table, update_ticker_metadata
from setup_utilities import create_metadata_table, initial_data_pull, create_ticker_metadata
from stock_functions import get_close_price, get_ohlc
import datetime as dt 
import sqlite3 


def main_update_job():

    ###  initial setup  #### 
    ####### DO NOT DELETE BELOW  #######
    ### may have to change the data locations to execute as a script ### 
     #known issues with this 'universe
    # bad_tickers = ['ABMD','ATVI','ALL','ABC','ANTM','BLL','BRK.B','BF.B','CERN','CTXS','DISCA','DISCK','DISH','DRE','RE','FB','FRC','FISV','FBHS','INFO','KSU','NLSN','NLOK','PBCT','PKI','SIVB','TWTR','VIAC','WLTW','XLNX']
    # data = pd.read_csv('data/constituents.csv')
    # data = data[~data['Symbol'].isin(bad_tickers)]
    # all_tickers = sorted(list(x for x in data['Symbol']))
    # drop_ticker_table('stock_metadata')
    # create_metadata_table()
    # for idx, row in data.iterrows():
    #     create_ticker_metadata(row['Symbol'], row['Name'], row['Sector'])
    ###### DO NOT DELETE ABOVE ############

    # main update job #
    issues = []
    ## update metadata ##
    ref_table = read_table('stock_metadata')
    for idx, row in ref_table.iterrows():
        print(f"updating metadata for {row['ticker']}", end='\t\r', flush=True)
        update_ticker_metadata(row['ticker'])
        ## compare today's date with last_date in the metadata ##
        if "".join(row['last_date'][:10].split('-')) < (dt.datetime.now().date()-dt.timedelta(days=4)).strftime('%Y%m%d'):
            print(f"\nupdating rows for row {row['ticker']}", end='\t\r', flush=True)            
            ## if needed, get most recent data ##
            new_data = get_ohlc(row['ticker'], start_date=dt.datetime.fromisoformat(row['last_date'])+dt.timedelta(days=1), end_date=dt.datetime.now().date())
            columns = ",".join([f"'{x}'" for x in new_data.columns])
            con = sqlite3.connect('stocks.db')
            ## insert data to ticker's table in db. should probably be its own function?
            ## NOTE: 'date' has a primary key constraint
            ## NOTE: you're working with America/New_York timezone datetimes.
            with con:
                try:
                    for local_row in new_data.iterrows():
                        values = ",".join([f"'{x}'" for x in local_row[1].values])
                        con.execute(f"INSERT INTO {row['ticker']} ({columns}) VALUES ({values})")
                        con.commit()
                except BaseException as e:
                    print(e)
                    issues.append(row['ticker'])
                    con.rollback()
            ## update metadata after changes
            update_ticker_metadata(row['ticker'])
    ## identify any errors... should go to a log instead.
    print(f"\nissues with {issues}")


if __name__ == '__main__':
    
    main_update_job()
    

