# This is a sample Python script.

# Press Umschalt+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import load_scryfall_to_sqldb
import create_cleaned_pricing_table
import util

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Create the config manager
    cfg_mgr = ConfigManager(credentials_file_path="./input/credentials.json" , run_id_file_path="./input/runID.json")
    # load run id from file
    current_run_id = cfg_mgr.run_id
    # execute code in order
    load_scryfall_to_sqldb.load_to_sqldb(current_run_id)
    create_cleaned_pricing_table.create_pricing_table(current_run_id)
    # increment run id by one and save it to file
    cfg_mgr.save_run_id(file_path="./input/runID.json" , run_id=(current_run_id+1))
