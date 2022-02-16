import json
import time

def time_it(function):
    def timed(*args, **kw):
        ts_1 = time.time()
        result = function(*args, **kw)
        ts_2 = time.time()
        print('func:{name} took: {execution_time} sec'.format(name=function.__name__, execution_time=(ts_2-ts_1)))
        return result
    return timed

class ConfigManager():

    def __init__(self, 
                credentials_file_path: str,
                run_id_file_path: str):
        self.run_id = self.load_run_id(run_id_file_path)
        self.credentials = self.load_credentials(credentials_file_path)

    def load_run_id(file_path: str) -> int:
        """Load run id from a file."""
        print("..loading runID..")
        run_id = -1
        with open(file_path) as fp:
            data = json.load(fp)
            run_id = data["runID"]
        return run_id

    def save_run_id(file_path: str, run_id: int):
        """Save the run id into a file."""
        print("..saving runID..")
        tmp_run_id = {"runID": run_id}
        with open(file_path, "w") as fp:
            json.dump(tmp_run_id, fp)

    def load_credentials(file_path: str):
        """Load credentials from a file."""
        print("..loading credentials..")
        with open(file_path) as fp:
            cred = json.load(fp)
        return cred
        