import asyncio
import json
import datetime
import os
import pathlib
import pprint
import queue

import job_log_collection
import chia_log_collection
import system_data_collection


class Harvester:

    def __init__(self, addr=None, port=None):
        if addr is None or port is None:
            config_fn = pathlib.Path(__file__).parent / ".." / "config.json"
            with open(config_fn) as f:
                config_dict = json.load(f)
            self.server_ip_addr = config_dict['server_ip_addr']
            self.server_port = config_dict['server_port']
        else:
            self.server_ip_addr = addr
            self.server_port = port
        self.q = queue.Queue()
        self.send_sleep = 15 * 60
        self.initial_sleep = 5
        self.data_getters = [
            self._system_data_getter,
            self._job_data_getter,
        ]
        self.active_pids = None

    async def _system_data_getter(self):
        while True:
            self.q.put(self._get_system_data())
            await asyncio.sleep(60)

    async def _job_data_getter(self):
        self.q.put(self._get_job_data(True))
        while True:
            await asyncio.sleep(15 * 60)
            self.q.put(self._get_job_data())

    async def _harvester_config_getter(self):
        self.q.put(self._get_harvester_config())

    async def run_client(self):
        reader, writer = await asyncio.open_connection(self.server_ip_addr, self.server_port)
        # kick off data getters
        await self._harvester_config_getter()
        data_getter_tasks = []
        for data_getter in self.data_getters:
            data_getter_tasks.append(asyncio.create_task(data_getter()))
        await asyncio.sleep(self.initial_sleep)
        while True:
            # send all data in queue
            for i in range(self.q.qsize()):
                try:
                    data_dict = self.q.get()
                except queue.Empty:
                    break
                json_message = json.JSONEncoder().encode(data_dict)
                json_message += "$$$"
                print(f"WRITING {len(json_message)} BYTES")
                writer.write(json_message.encode())
                await writer.drain()
            # sleep for a bit
            await asyncio.sleep(self.send_sleep)

    def _get_job_data(self, first=False):
        data_time = datetime.datetime.now()
        # jobs
        all_job_files = job_log_collection.get_all_job_files()
        jobs_data = []
        delete_cutoff = datetime.datetime.now() - datetime.timedelta(days=60)
        send_cutoff = datetime.datetime.now() - datetime.timedelta(days=1)
        for job_file, job_file_time in all_job_files.items():
            if job_file_time < delete_cutoff:
                os.remove(job_file)
                continue
            if (not first) and (job_file_time < send_cutoff):
                continue
            jdat = job_log_collection.read_job_log(job_file)
            jdat['process_id'] = int(jdat['process_id'])
            jdat['start_time'] = job_file_time.strftime("%Y%m%dT%H:%M:%S")
            if self.active_pids is not None:
                if jdat['status'] == 'in_progress' and jdat['process_id'] not in self.active_pids:
                    jdat['status'] = 'error'
            jobs_data.append(jdat)
        # chia
        chia_data = chia_log_collection.read_chia_log()

        data_dict = {
            'jobs': jobs_data,
            'chia': chia_data,
            'time': data_time.strftime("%Y%m%dT%H:%M:%S"),
        }
        return data_dict

    def _get_system_data(self):
        data_time = datetime.datetime.now()
        system_data = system_data_collection.get_system_data()
        self.active_pids = [p['pid'] for p in system_data['job_processes']]
        pprint.pprint(system_data)
        data_dict = {
            'system': system_data,
            'time': data_time.strftime("%Y%m%dT%H:%M:%S"),
        }
        return data_dict

    def _get_harvester_config(self):
        data_time = datetime.datetime.now()
        harvester_config = system_data_collection.get_harvester_config()
        pprint.pprint(harvester_config)
        data_dict = {
            'harvester_config': harvester_config,
            'time': data_time.strftime("%Y%m%dT%H:%M:%S"),
        }
        return data_dict


async def main():
    import sys
    if len(sys.argv) == 3:
        harvester = Harvester(sys.argv[1], sys.argv[2])
    else:
        harvester = Harvester()
    await harvester.run_client()


if __name__ == "__main__":
    asyncio.run(main())
