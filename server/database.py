import sqlite3
import pathlib
import json

string_fields = ["start_time", "temp_dir1", "temp_dir2", "final_dir", "plot_id", "process_id", "buffer_size", "status",
                 "harvester_ip"]
config_fn = pathlib.Path(__file__).parent / ".." / "config.json"
with open(config_fn) as f:
    config_dict = json.load(f)
if config_dict['database_dir'] in ['NULL', 'None', 0]:
    db_dir = pathlib.Path(__file__).parent
else:
    db_dir = pathlib.Path(config_dict['database_dir'])
db_path = db_dir / "chiadash_database.db"
db_fields = [
    "start_time",
    "harvester_ip",
    "temp_dir1",
    "temp_dir2",
    "final_dir",
    "plot_id",
    "process_id",
    "phase1_time",
    "phase2_time",
    "phase3_time",
    "phase4_time",
    "total_time",
    "copy_time",
    "plot_size",
    "buffer_size",
    "n_buckets",
    "n_threads",
    "stripe_size",
    "status",
]


def initialize_db():
    db_connection = sqlite3.connect(db_path)
    db_cursor = db_connection.cursor()

    create_database_script = pathlib.Path(__file__).parent / "create_database.sql"
    db_cursor.executescript(create_database_script.read_text())

    db_connection.commit()
    db_connection.close()


def update_job(db_cursor, job):
    for k in job:
        if k in string_fields:
            job[k] = f"'{job[k]}'"
    if check_job_exists(db_cursor, job['plot_id']):
        update = ", ".join([f"{key} = {value}" for key, value in job.items()])
        command = f"UPDATE jobs SET {update} WHERE plot_id={job['plot_id']}"
        # print(command)
        db_cursor.execute(command)
    else:
        command = f"INSERT INTO jobs({', '.join(job.keys())}) VALUES({', '.join([str(v) for v in job.values()])})"
        # print(command)
        db_cursor.execute(command)


def check_job_exists(db_cursor, plot_id):
    command = f"SELECT * FROM jobs WHERE plot_id={plot_id}"
    # print(command)
    job_list = db_cursor.execute(command).fetchall()
    return len(job_list) == 1


def update_database(jobs):
    if not db_path.exists():
        initialize_db()
    db_connection = sqlite3.connect(db_path)
    db_cursor = db_connection.cursor()
    for job in jobs:
        update_job(db_cursor, job)
    db_connection.commit()
    db_connection.close()


def get_current_jobs():
    if not db_path.exists():
        return []
    db_connection = sqlite3.connect(db_path)
    db_cursor = db_connection.cursor()
    command = f"SELECT * FROM jobs WHERE status='in_progress'"
    # print(command)
    job_list = db_cursor.execute(command).fetchall()
    db_connection.close()
    job_list = [
        {key: value for key, value in zip(db_fields, job)}
        for job in job_list
    ]
    return job_list


def get_all_jobs():
    if not db_path.exists():
        return []
    db_connection = sqlite3.connect(db_path)
    db_cursor = db_connection.cursor()
    command = f"SELECT * FROM jobs"
    # print(command)
    job_list = db_cursor.execute(command).fetchall()
    db_connection.close()
    job_list = [
        {key: value for key, value in zip(db_fields, job)}
        for job in job_list
    ]
    return job_list
