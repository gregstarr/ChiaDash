import os
import pathlib
import pandas
import json


config_fn = pathlib.Path(__file__).parent / ".." / "config.json"
with open(config_fn) as f:
    config_dict = json.load(f)
if config_dict['database_dir'] in ['NULL', 'None', 0]:
    db_dir = pathlib.Path(__file__).parent
else:
    db_dir = pathlib.Path(config_dict['database_dir'])
ts_data_dir = db_dir / "ts_data"


def get_data_dir(ip_addr, time):
    directory = ts_data_dir / f"{ip_addr.replace('.', '_')}__{time.replace(':', '')}"
    os.makedirs(directory)
    return directory


def save_data(fn, time, data):
    df = pandas.json_normalize(data)
    df['time'] = time
    if fn.exists():
        with open(fn, 'a') as f:
            df.to_csv(f, header=False)
    else:
        with open(fn, 'a') as f:
            df.to_csv(f, header=True)


def save_job_process_data(directory, time, data):
    fn = directory / "job_process_data.csv"
    save_data(fn, time, data)


def save_disk_io_data(directory, time, data):
    fn = directory / "disk_io_data.csv"
    save_data(fn, time, data)


def save_temps_data(directory, time, data):
    fn = directory / "temps_data.csv"
    save_data(fn, time, data)


def save_fans_data(directory, time, data):
    fn = directory / "fans_data.csv"
    save_data(fn, time, data)


def save_chia_data(directory, time, data):
    fn = directory / "chia_data.csv"
    save_data(fn, time, data)
