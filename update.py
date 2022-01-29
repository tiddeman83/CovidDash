import pandas as pd
import pyarrow
import datetime as datetime
from datetime import timedelta
import os
import schedule 
import time

#specification of the dataset
rivm_gemeente = 'https://data.rivm.nl/covid-19/COVID-19_aantallen_gemeente_per_dag.json'
zkh = 'https://lcps.nu/wp-content/uploads/covid-19-datafeed.csv' 

def main():
    """checks on startup if data have been obtained earlier, or otherwise will create 
    the parquet file for the first time"""
    
    if os.path.isfile('updated_nl_covid_cases.parquet'):
        print('first initiation was done, redirecting the load data method')
        start_regular_update_routine()
        
    else:
        print('First initialization of parquet file now running...')
        data = import_data()
        data.to_parquet('updated_nl_covid_cases.parquet')
        print('first data load was initiated, we are good to go')
        print('standard update routine started...')
        start_regular_update_routine()


def update_mechanism():
    print('updating information...')
    data = import_data()    
    data.to_parquet('updated_nl_covid_cases.parquet')
    print('updated information...')


def import_data():
    df = pd.read_json(rivm_gemeente)
    df['download_date'] = datetime.date.today().strftime('%Y-%m-%d')
    df['download_time'] = datetime.datetime.now().time()
    
    return df

def start_regular_update_routine():
    while True:
        schedule.every().day.at("15:45").do(update_mechanism)
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()