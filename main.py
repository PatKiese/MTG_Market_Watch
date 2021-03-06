from DbLoader import DbLoader
from DataTransformer import DataTransformer
from util import ConfigManager


if __name__ == '__main__':
    # Create the config manager
    cfg_mgr = ConfigManager.from_files(credentials_file_path="./input/credentials.json" , run_id_file_path="./input/runID.json")
    db_loader = DbLoader(run_id=cfg_mgr.run_id ,credentials=cfg_mgr.credentials)
    d_transformer = DataTransformer(run_id=cfg_mgr.run_id)

    # Fetch data and load to DataFrame
    df = db_loader.fetch_data()
    df = d_transformer.prep_data(df)
    db_loader.load_data_to_db(df, table_name='bulk_data')

    # Create cleaned pricing table
    df = db_loader.fetch_table_from_db(run_id=cfg_mgr.run_id)
    df_prices = d_transformer.create_pricing_table(df)
    db_loader.load_data_to_db(df_prices, table_name='pricing_table')
    
    # increment run id by one and save it to file
    cfg_mgr.save_run_id(file_path="./input/runID.json" , run_id=(cfg_mgr.run_id + 1))
