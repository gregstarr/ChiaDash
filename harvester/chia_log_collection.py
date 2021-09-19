import pathlib
import json
import re


config_fn = pathlib.Path(__file__).parent / "config.json"
with open(config_fn) as f:
    config_dict = json.load(f)
chia_log_dir = pathlib.Path(config_dict['chia_log_dir'])
latest_chia_log = chia_log_dir / "debug.log"


def read_chia_log(path=None):
    if path is None:
        path = latest_chia_log
    with open(path) as f:
        chia_log = f.read()
    # total n plots
    chia_data = {}
    match = re.search(r".+ plots were eligible for farming .+ Found .+ proofs\. Time: .+ s\. Total (\d+) plots", chia_log)
    if match:
        chia_data['n_plots'] = int(match.group(1))
    return chia_data
