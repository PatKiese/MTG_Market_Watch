from sqlalchemy import create_engine
from util import time_it
import pandas as pd
import util
from datetime import date

class DataTransformer():
    """
    This class handles the main data manipulations.
    """
    def __init__(self, 
                run_id: int ):
        self.run_id = run_id

    def prep_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepares the downloaded bulk data for future transformations. 
        Adds a run_id and import_date.
        :param df: DataFrame containing the daily bulk-data.
        """
        timestamp = date.today()
        # add today's date as import date
        df["RUN_ID"] = self.run_id
        df["IMPORT_DATE"] = timestamp

        # rearrange columns so that date is in front
        cols = df.columns.tolist()
        cols = cols[-2:] + cols[:-2]
        df = df[cols]

        # Drop columns recently added
        df.drop(columns='security_stamp', inplace=True)
        df.drop(columns='penny_rank', inplace=True)

        # Data uniformity
        df = df.applymap(str)
        
        return df

    def fetch_float_from_dict(self, x, key) -> float:
        """
        Fetches a float from a given 'dictionary' string.
        :param x: 'dictionary' in form of a single string.
        :param key: key of the dicitonary to search for.
        :return: Value corresponding to key
        """
        x.strip('{')
        x.strip('}')
        x_list = x.split(',')
        val = -1
        for i in x_list:
            if i.find(key) != -1:
                val = i.split()[1].strip("'")
                if val == 'None':
                    val = -1
                else:
                    val = float(val)
                return val
        return val

    def fetch_string_from_dict(self, x, key) -> str:
        """
        Fetches a string from a given 'dictionary' string.
        :param x: 'dictionary' in form of a single string.
        :param key: key of the dicitonary to search for.
        :return: Value corresponding to key
        """
        x.strip('{')
        x.strip('}')
        x_list = x.split(',')
        val = -1
        for i in x_list:
            if i.find(key) != -1:
                val = i.split()[1].strip("'")
                return val
        return val

    @util.time_it
    def create_pricing_table(self, df: pd.DataFrame()) -> pd.DataFrame():
        """
        Creates a formatted pricing table from given bulk-data DataFrame.
        :param df: Bulk-data DataFrame.
        :return: Cleaned pricing table.
        """
        print("..creating pricing data table..")
        df_prices = pd.DataFrame()
        df_prices[['run_id', 'import_date', 'id', 'name']] = df[['RUN_ID', 'IMPORT_DATE', 'id', 'name']]
        df_prices[['lang', 'set_name']] = df[['lang', 'set_name']]
        df_prices['price_usd'] = df.apply(lambda x: self.fetch_float_from_dict(x["prices"], 'usd'), axis=1)
        df_prices['price_usd_foil'] = df.apply(lambda x: self.fetch_float_from_dict(x["prices"], 'usd_foil'), axis=1)
        df_prices['price_usd_etched'] = df.apply(lambda x: self.fetch_float_from_dict(x["prices"], 'usd_etched'), axis=1)
        df_prices['price_eur'] = df.apply(lambda x: self.fetch_float_from_dict(x["prices"], 'eur'), axis=1)
        df_prices['price_eur_foil'] = df.apply(lambda x: self.fetch_float_from_dict(x["prices"], 'eur_foil'), axis=1)
        df_prices['commander_legal'] = df.apply(lambda x: self.fetch_string_from_dict(x["legalities"], 'commander'), axis=1)
        df_prices['standard_legal'] = df.apply(lambda x: self.fetch_string_from_dict(x["legalities"], 'standard'), axis=1)
        df_prices['pioneer_legal'] = df.apply(lambda x: self.fetch_string_from_dict(x["legalities"], 'pioneer'), axis=1)
        df_prices['modern_legal'] = df.apply(lambda x: self.fetch_string_from_dict(x["legalities"], 'modern'), axis=1)
        df_prices['legacy_legal'] = df.apply(lambda x: self.fetch_string_from_dict(x["legalities"], 'legacy'), axis=1)
        df_prices[['reserved', 'scryfall_uri']] = df[['reserved', 'scryfall_uri']]
        return df_prices
