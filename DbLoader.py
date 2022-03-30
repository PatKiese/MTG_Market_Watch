# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import requests
import pandas as pd
from sqlalchemy import create_engine
import json
import util

class DbLoader():
    """
    Class to handle the data I/O from the scryfall and mySQL database.
    """
    def __init__(self, 
                credentials: dict, 
                run_id: int ):
        self.run_id = run_id
        self.credentials = credentials

    def fetch_data(self) -> pd.DataFrame:
        """
        Fetches the scryfall bulk-data from the API and returns the daily updated data as a pandas DataFrame.
        :return: DataFrame with bulk-data
        """
        api_path = "https://api.scryfall.com/bulk-data"
        # Get info for bulk data download
        print("Fetching data from scryfall: {api_path}".format(api_path=api_path))
        r = requests.get(api_path)
        print("Statuscode: ", r.status_code)
        tmp_json = r.json()
        dl_url = tmp_json['data'][0]['download_uri']
        print("Downloading bulk-data: ", dl_url)
        r_dl = requests.get(dl_url)
        # load json file into pandas dataframe
        df = pd.DataFrame.from_dict(data=r_dl.json(), orient='columns')
        return df

    def fetch_table_from_db(self, run_id: int=None, table_name: str='bulk_data') -> pd.DataFrame():
        """
        Fetches a table corresponding to a run_id and table_name from the mySQL database and returns it as a pandas DataFrame.
        :param run_id: run_id from the selected table.
        :param table_name: Name of the table to be fetched from the database.
        :return: DataFrame with bulk-data
        """
        credentials = self.credentials
        _run_id = self.run_id
        if run_id is not None:
            _run_id = run_id
        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                            .format(host=credentials['hostname'], db=credentials['dbname'], user=credentials['uname'],
                                    pw=credentials['pwd']))
        # Loading bulk data from sql database
        print(f"..loading {table_name} from sql database..")
        df = pd.read_sql(f"select * from {table_name} where RUN_ID = {str(_run_id)};", con=engine)
        print("..complete..")
        return df

    def load_data_to_db(self, df: pd.DataFrame, replace_table: bool=False, table_name: str='bulk_data'):
        """
        Loads a table with table_name to the mySQL database.
        :param df: Table to be loaded into database.
        :paran replace_table: Signals if an existing table will be replaced.
        :param table_name: Name of the table to be loaded to the database.
        """
        # Credentials to database connection
        credentials = self.credentials
        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=credentials['hostname'], 
                        db=credentials['dbname'], user=credentials['uname'],pw=credentials['pwd']))
        # Load dataframe to sql table
        print("..loading bulk data to SQL database..")
        if replace_table:
            df.to_sql(table_name, engine, index=False, if_exists="replace")
        else:
            df.to_sql(table_name, engine, index=False, if_exists="append")
        print("..loading complete..")

