from sqlalchemy import create_engine
import pandas as pd
import util


def fetch_float_from_dict(x, key):
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


def fetch_string_from_dict(x, key):
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
def create_pricing_table(run_id, replace_table=False):
    '''Read bulk data from sql table and create a condensed pricing table. Load it into new table.'''
    credentials = util.load_credentials("./input/credentials.json")
    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(host=credentials['hostname'], db=credentials['dbname'], user=credentials['uname'],
                                   pw=credentials['pwd']))
    # Loading bulk data from sql database
    print("..loading bulk data from sql database..")
    df = pd.read_sql(f"select * from bulk_data where RUN_ID = {str(run_id)};", con=engine)
    print("..complete..")
    print("..creating pricing data table..")
    df_prices = pd.DataFrame()
    df_prices[['run_id', 'import_date', 'id', 'name']] = df[['RUN_ID', 'IMPORT_DATE', 'id', 'name']]
    df_prices[['lang', 'set_name']] = df[['lang', 'set_name']]
    df_prices['price_usd'] = df.apply(lambda x: fetch_float_from_dict(x["prices"], 'usd'), axis=1)
    df_prices['price_usd_foil'] = df.apply(lambda x: fetch_float_from_dict(x["prices"], 'usd_foil'), axis=1)
    df_prices['price_usd_etched'] = df.apply(lambda x: fetch_float_from_dict(x["prices"], 'usd_etched'), axis=1)
    df_prices['price_eur'] = df.apply(lambda x: fetch_float_from_dict(x["prices"], 'eur'), axis=1)
    df_prices['price_eur_foil'] = df.apply(lambda x: fetch_float_from_dict(x["prices"], 'eur_foil'), axis=1)
    df_prices['commander_legal'] = df.apply(lambda x: fetch_string_from_dict(x["legalities"], 'commander'), axis=1)
    df_prices['standard_legal'] = df.apply(lambda x: fetch_string_from_dict(x["legalities"], 'standard'), axis=1)
    df_prices['pioneer_legal'] = df.apply(lambda x: fetch_string_from_dict(x["legalities"], 'pioneer'), axis=1)
    df_prices['modern_legal'] = df.apply(lambda x: fetch_string_from_dict(x["legalities"], 'modern'), axis=1)
    df_prices['legacy_legal'] = df.apply(lambda x: fetch_string_from_dict(x["legalities"], 'legacy'), axis=1)
    df_prices[['reserved', 'scryfall_uri']] = df[['reserved', 'scryfall_uri']]
    # Load dataframe to sql table
    print("..loading pricing data to SQL database..")
    if replace_table:
        df_prices.to_sql('pricing_table', engine, index=False, if_exists="replace")
    else:
        df_prices.to_sql('pricing_table', engine, index=False, if_exists="append")
    print("..loading complete..")
