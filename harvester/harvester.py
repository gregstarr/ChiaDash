import asyncio
import json
import datetime
import pathlib

import job_log_collection
import chia_log_collection
import system_data_collection


class Harvester:

    def __init__(self, addr=None, port=None):
        if addr is None or port is None:
            config_fn = pathlib.Path(__file__).parent / "config.json"
            with open(config_fn) as f:
                config_dict = json.load(f)
            self.server_ip_addr = config_dict['server_ip_addr']
            self.server_port = config_dict['server_port']
        else:
            self.server_ip_addr = addr
            self.server_port = port
        self.sleep_time = 15 * 60

    async def get_data(self, first=False):
        # system
        system_data = system_data_collection.get_system_data()
        # jobs
        all_job_files = job_log_collection.get_all_job_files()
        jobs_data = []
        cutoff_time = datetime.datetime.now() - datetime.timedelta(days=1)
        # print(f"cutoff_time: {cutoff_time}")
        for job_file, job_file_time in all_job_files.items():
            if not first and job_file_time < cutoff_time:
                continue
            jdat = job_log_collection.read_job_log(job_file)
            # print(f"TIME: {job_file_time} JDAT: {jdat}")
            jdat['start_time'] = job_file_time.strftime("%Y%m%dT%H:%M:%S")
            jobs_data.append(jdat)
        # chia
        chia_data = chia_log_collection.read_chia_log()

        data_dict = {
            'jobs': jobs_data,
            'chia': chia_data,
            'system': system_data
        }

        return data_dict

    async def run_client(self):
        reader, writer = await asyncio.open_connection(self.server_ip_addr, self.server_port)
        new_data_task = asyncio.create_task(self.get_data(True))
        while True:
            data_dict = await new_data_task  # dictionary
            # print(f"DATA DICT {data_dict}")
            json_message = json.JSONEncoder().encode(data_dict)
            json_message += "$$$"
            print(f"WRITING {len(json_message)} BYTES")
            writer.write(json_message.encode())
            await writer.drain()
            new_data_task = asyncio.create_task(self.get_data())
            await asyncio.sleep(self.sleep_time)


async def main():
    import sys
    if len(sys.argv) == 3:
        harvester = Harvester(sys.argv[1], sys.argv[2])
    else:
        harvester = Harvester()
    await harvester.run_client()


if __name__ == "__main__":
    asyncio.run(main())
