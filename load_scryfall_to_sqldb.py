# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import requests
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
import json
import util


def load_to_sqldb(run_id, replace_table=False):
    timestamp = date.today()
    # Get info for bulk data download
    print("Fetching data from scryfall: https://api.scryfall.com/bulk-data")
    r = requests.get('https://api.scryfall.com/bulk-data')
    print("Statuscode: ", r.status_code)
    tmp_json = r.json()
    dl_url = tmp_json['data'][0]['download_uri']
    print("Downloading bulk-data: ", dl_url)
    r_dl = requests.get(dl_url)
    # load json file into pandas dataframe
    df = pd.DataFrame.from_dict(data=r_dl.json(), orient='columns')
    # fetch current run number
    print(f"runID: {run_id}")
    # add today's date as import date
    df["RUN_ID"] = run_id
    df["IMPORT_DATE"] = timestamp
    # rearrange columns so that date is in front
    cols = df.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    df = df[cols]
    # Drop columns recently added
    df.drop(columns='security_stamp', inplace=True)
    # Credentials to database connection
    credentials = util.load_credentials("./input/credentials.json")
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(host=credentials['hostname'], db=credentials['dbname'], user=credentials['uname'],
                                   pw=credentials['pwd']))
    df = df.applymap(str)
    # Load dataframe to sql table
    print("..loading bulk data to SQL database..")
    if replace_table:
        df.to_sql('bulk_data', engine, index=False, if_exists="replace")
    else:
        df.to_sql('bulk_data', engine, index=False, if_exists="append")
    print("..loading complete..")

