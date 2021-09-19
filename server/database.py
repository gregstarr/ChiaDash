import sqlite3
import pathlib


db_path = pathlib.Path(__file__).parent / "chiadash_database.db"


def initialize_db():
    db_connection = sqlite3.connect(db_path)
    db_cursor = db_connection.cursor()

    create_database_script = pathlib.Path(__file__).parent / "create_database.sql"
    db_cursor.executescript(create_database_script.read_text())

    db_connection.commit()
    db_connection.close()


def update_job(db_cursor, job):
    if check_job_exists(db_cursor, job['plot_id']):
        update = ", ".join([f"{key} = {value}" for key, value in job.items()])
        db_cursor.execute(f"UPDATE jobs SET {update} WHERE plot_id = {job['plot_id']}")
    else:
        db_cursor.execute(f"INSERT INTO jobs({', '.join(job.keys())}) VALUES({', '.join(job.values())})")


def check_job_exists(db_cursor, plot_id):
    job_list = db_cursor.execute(f"SELECT * FROM jobs WHERE condition WHERE plot_id = {plot_id}").fetchall()
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
