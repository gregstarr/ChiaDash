import asyncio
import json
import datetime
import pathlib

import job_log_collection
import chia_log_collection
import system_data_collection


class Harvester:

    def __init__(self):
        config_fn = pathlib.Path(__file__).parent / "config.json"
        with open(config_fn) as f:
            config_dict = json.load(f)
        self.server_ip_addr = config_dict['server_ip_addr']
        self.server_port = config_dict['server_port']
        self.sleep_time = 60

    async def get_data(self):
        # jobs
        all_job_files = job_log_collection.get_all_job_files()
        jobs_data = []
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=1)
        print(f"cutoff_time: {cutoff_time}")
        for job_file, job_file_time in all_job_files.items():
            if job_file_time < cutoff_time:
                continue
            jdat = job_log_collection.read_job_log(job_file)
            print(f"TIME: {job_file_time} JDAT: {jdat}")
            jdat['start_time'] = job_file_time.strftime("%Y%m%dT%H:%M:%S")
            jobs_data.append(jdat)
        # chia
        chia_data = chia_log_collection.read_chia_log()
        # system
        system_data = system_data_collection.get_system_data()

        data_dict = {
            'jobs': jobs_data,
            'chia': chia_data,
            'system': system_data
        }

        return data_dict

    async def run_client(self):
        reader, writer = await asyncio.open_connection(self.server_ip_addr, self.server_port)
        new_data_task = asyncio.create_task(self.get_data())
        while True:
            data_dict = await new_data_task  # dictionary
            print(f"DATA DICT {data_dict}")
            json_message = json.JSONEncoder().encode(data_dict)
            json_message += "$$$"
            writer.write(json_message.encode())
            await writer.drain()
            await asyncio.sleep(self.sleep_time)
            new_data_task = asyncio.create_task(self.get_data())


async def main():
    harvester = Harvester()
    await harvester.run_client()


if __name__ == "__main__":
    asyncio.run(main())
