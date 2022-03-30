from util import ConfigManager, time_it
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import time
from datetime import datetime

class PriceChecker():
    """
    Class to check the pricing tables for price spikes.
    """
    def __init__(self, 
                credentials: dict ):
        self.credentials = credentials

    @time_it
    def read_from_db(self, tmp_sql_query, tmp_engine) -> pd.DataFrame:
        df = pd.read_sql(tmp_sql_query, con=tmp_engine)
        return df

    @time_it
    def get_latest_id(self, input_engine) -> int:
        """Get the latest run id."""
        query = """select distinct(run_id) as tmp_id, import_date from pricing_table order by import_date desc;"""
        df = self.read_from_db(query, input_engine)
        return int(df.iloc[0]["tmp_id"])

    @time_it
    def get_last_week_id(self, input_engine) -> int:
        """Get the run id from last week."""
        query = """select distinct(run_id),import_date from pricing_table where import_date < NOW() - INTERVAL 1 WEEK 
            order by import_date desc;"""
        df = self.read_from_db(query, input_engine)
        return int(df.iloc[0]["run_id"])

    @time_it
    def fetch_table_from_id(self, input_engine, input_id) -> pd.DataFrame:
        """Create a DataFrame from the corresponding item id."""
        query = '''select *  from pricing_table where id = '{input_id}' 
            order by import_date asc;'''.format(input_id=input_id)
        df = self.read_from_db(query, input_engine)
        return df

    def draw_price_time_series(self, file_path, input_engine, item_id):
        """Draw pricing time series corresponding to item_id."""
        df_time_series = self.fetch_table_from_id(input_engine=input_engine, input_id=item_id)
        time_series_title = df_time_series.iloc[0]['name']
        df_time_series.set_index('import_date', inplace=True)
        df_time_series[['price_usd', 'price_eur']].plot(subplots=True, title=time_series_title)
        plt.xlabel("Date")
        plt.savefig(file_path)
        plt.close()


    def create_sql_query_search_biggest_price_increase(self, currency="usd", mtg_format="commander", asc_desc="asc",
                                                    reserved_list="False", start_id=0, end_id=1):
        """Create an SQL query to search for the biggest price increase within a given timeframe, represented by a start-
        and end-id. Usually one week or month."""
        if currency not in ["usd", "eur", "usd_foil", "eur_foil"]:
            print("Wrong currency option selected!")
            return -1
        if mtg_format not in ["standard", "commander", "pioneer", "modern", "legacy"]:
            print("Wrong mtg-format option selected!")
            return -1
        if asc_desc not in ["asc", "desc"]:
            print("Wrong order option selected!")
            return -1
        if reserved_list not in ["True", "False"]:
            print("Wrong reserved list option selected!")
            return -1
        tmp_str = """select 
                t1.name,
                t1.set_name,
                ((t1.price_{currency} - t2.price_{currency})*100/t2.price_{currency}) as rel_difference_{currency},
                (t1.price_{currency} - t2.price_{currency}) as difference_{currency},        
                t1.price_{currency} as price_{currency}_new,
                t2.price_{currency} as price_{currency}_old,
                t1.import_date as current_day,
                t2.import_date as last_day,
                t1.run_id,
                t2.run_id,
                t1.scryfall_uri,
                t1.id
            from 
                (select * from pricing_table where run_id = {end_id}) t1,
                (select * from pricing_table where run_id = {start_id}) t2
            where t1.{mtg_format}_legal = "legal"
                and t1.id = t2.id
                and t1.reserved = "{reserved_list}"
                and (t1.price_{currency} - t2.price_{currency}) > 1
                and t1.price_{currency} <> -1
                and t2.price_{currency} <> -1
            order by rel_difference_{currency} {asc_desc}
            limit 10;""".format(currency=currency, mtg_format=mtg_format, asc_desc=asc_desc, reserved_list=reserved_list,
                            start_id=start_id, end_id=end_id)
        return tmp_str

    def run(self):
        credentials = self.credentials
        # Create SQLAlchemy engine to connect to MySQL Database
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                            .format(host=credentials['hostname'], db=credentials['dbname'], user=credentials['uname'],
                                    pw=credentials['pwd']))
        # Loading bulk data from sql database
        mtg_formats = ["standard", "commander", "pioneer", "modern", "legacy"]
        latest_id = self.get_latest_id(engine)
        last_week_id = self.get_last_week_id(engine)
        for tmp_format in mtg_formats:
            # Create SQL query for each relevant MtG format
            sql_query = self.create_sql_query_search_biggest_price_increase(currency="usd", mtg_format=tmp_format,
                                                                    asc_desc="desc", reserved_list="False",
                                                                    start_id=last_week_id, end_id=latest_id)
            # Execute query and load result into Dataframe
            df = pd.read_sql(sql_query, con=engine)
            # Save Dataframe to csv file
            df.to_csv("./flask_dashboard/static/tables/{tmp_format}_pricing_table.csv".format(tmp_format=tmp_format))
            # Create Plots of item with the biggest price increase within the last week
            n_rows = df.shape[0]
            for i in range(n_rows):
                tmp_file_name = "place_"+str(i)+".png"
                self.draw_price_time_series(file_path="./flask_dashboard/static/media/{tmp_format}_{tmp_file_name}".format(
                    tmp_format=tmp_format, tmp_file_name=tmp_file_name), input_engine=engine, item_id=df.iloc[i]['id'])


if __name__ == '__main__':
    cfg_mgr = ConfigManager.from_files(credentials_file_path="./input/credentials.json" , run_id_file_path="./input/runID.json")
    credentials = cfg_mgr.credentials
    price_checker = PriceChecker(credentials=credentials)
    price_checker.run()
