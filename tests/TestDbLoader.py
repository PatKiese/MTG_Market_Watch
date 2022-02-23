import unittest
import os
import sys
import inspect
import pandas as pd

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from DbLoader import DbLoader
from util import ConfigManager

class TestDbLoader(unittest.TestCase):
    def setUp(self):
        self.cfg_mgr = ConfigManager(credentials_file_path="../input/credentials.json" , 
                        run_id_file_path="../input/runID.json")
        self.db_loader = DbLoader(run_id=self.cfg_mgr.run_id ,credentials=self.cfg_mgr.credentials)

    def test_fetch_data(self):
        df = self.db_loader.fetch_data()
        self.assertFalse(df.empty)

    def test_fetch_data_from_db(self):
        df = self.db_loader.fetch_table_from_db(run_id=(self.cfg_mgr.run_id - 1))
        self.assertFalse(df.empty)

    def test_load_data_to_db(self):
        _run_id = self.cfg_mgr.run_id
        df = pd.DataFrame({'RUN_ID': [_run_id, _run_id, _run_id, _run_id], 
                            'a': [0,1,2,3], 'b': [0,1,2,3]})
        self.db_loader.load_data_to_db(df, replace_table=True, table_name='test_data')
        df = self.db_loader.fetch_table_from_db(run_id=_run_id, table_name='test_data')
        self.assertFalse(df.empty)

if __name__ == '__main__':
    unittest.main()