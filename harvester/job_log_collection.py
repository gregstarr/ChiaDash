import json
import os
import pathlib
import re
import datetime


job_log_search_dict = {
    'temp_dir1':        r"Starting plotting progress into temporary dirs: (.+) and .+\n",
    'temp_dir2':        r"Starting plotting progress into temporary dirs: .+ and (.+)\n",
    'final_dir':        r"Final Directory is: (.+)\n",
    'plot_id':          r"ID: (.+)\n",
    'process_id':       r"Process ID is: (.+)\n",
    'phase1_time':      r"Time for phase 1 = (\d+\.\d+) seconds\.",
    'phase2_time':      r"Time for phase 2 = (\d+\.\d+) seconds\.",
    'phase3_time':      r"Time for phase 3 = (\d+\.\d+) seconds\.",
    'phase4_time':      r"Time for phase 4 = (\d+\.\d+) seconds\.",
    'total_time':       r"Total time = (\d+\.\d+) seconds\.",
    'copy_time':        r"Copy time = (\d+\.\d+) seconds\.",
    'plot_size':        r"Plot size is: (\d+)\n",
    'buffer_size':      r"Buffer size is: (.+)\n",
    'n_buckets':        r"Using (\d+) buckets\n",
    'n_threads':        r"Using (\d+) threads of stripe size \d+\n",
    'stripe_size':      r"Using \d+ threads of stripe size (\d+)\n",
}


config_fn = pathlib.Path(__file__).parent / "config.json"
with open(config_fn) as f:
    config_dict = json.load(f)
job_log_dir = pathlib.Path(config_dict['job_log_dir'])


def get_all_job_files(job_dir=None):
    if job_dir is None:
        job_dir = job_log_dir
    job_files = {}
    for job_log_file in job_dir.glob("*.log"):
        match = re.search("(\d{4})-(\d{2})-\d{2}_\d{2}_\d{2}_\d{2}", job_log_file.name)
        if not match:
            print(f"JOB NAME READ ERROR: {job_log_file}")
            continue
        file_time = datetime.datetime.strptime(match.group(), "%Y-%m-%d_%H_%M_%S")
        job_files[job_log_file] = file_time
    return job_files


def read_job_log(path):
    with open(path) as f:
        job_log = f.read()

    job_data = {}
    for key, regex in job_log_search_dict.items():
        match = re.search(regex, job_log)
        if match:
            job_data[key] = match.group(1)

    # status
    match = re.search("Created a total of 1 new plots", job_log)
    if match:
        job_data['status'] = "complete"
    else:
        match = re.search("error", job_log, flags=re.IGNORECASE)
        if match:
            job_data['status'] = "error"
        else:
            job_data['status'] = "in_progress"
    return job_data


if __name__ == "__main__":
    for file, t in get_all_job_files().items():
        data = read_job_log(file)
        data['time'] = t
        print(t, data)
