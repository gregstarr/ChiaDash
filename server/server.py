import asyncio
import json

import database
import tsdb


class Server:

    def __init__(self):
        self.connected_harvesters = {}
        self.data_handlers = {
            'harvester_config': self._handle_harvester_config,
            'system': self._handle_system_data,
            'jobs': self._handle_job_data,
            'chia': self._handle_chia_data,
        }

    async def start(self):
        server = await asyncio.start_server(self.handle_new_connection, host='0.0.0.0', port=8888, limit=2 ** 20)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

    async def handle_new_connection(self, reader, writer):
        addr, port = writer.get_extra_info('peername')
        print(addr, port)
        self.connected_harvesters[addr] = {'port': port}
        while True:
            try:
                message = await reader.readuntil(b"$$$")
            except ConnectionError:
                print(f"{addr}: Connection lost")
                del self.connected_harvesters[addr]
                break
            except asyncio.IncompleteReadError:
                print(f"{addr}: No more data")
                del self.connected_harvesters[addr]
                break
            if message:
                data = json.JSONDecoder().decode(message.decode()[:-3])
                for packet_type, packet in data.items():
                    if packet_type == 'time':
                        continue
                    self.data_handlers[packet_type](addr, data['time'], packet)

    def _handle_harvester_config(self, ip_addr, time, data):
        self.connected_harvesters[ip_addr]['nic'] = None
        self.connected_harvesters[ip_addr]['nic_addrs'] = None
        for nic, addr_dicts in data['net_addrs'].items():
            for addr_dict in addr_dicts:
                if addr_dict['address'] == ip_addr:
                    self.connected_harvesters[ip_addr]['nic'] = nic
                    self.connected_harvesters[ip_addr]['nic_addrs'] = addr_dict
                    break
            else:
                continue
            break
        self.connected_harvesters[ip_addr].update(
            disk_parts=data['disk_parts'],
            cpu_count=data['cpu_count'],
            time=time,
            ts_data_folder=tsdb.get_data_dir(ip_addr, time),
        )

    def _handle_system_data(self, ip_addr, time, data):
        if 'ts_data_folder' in self.connected_harvesters[ip_addr]:
            directory = self.connected_harvesters[ip_addr]['ts_data_folder']
            tsdb.save_job_process_data(directory, time, data['job_processes'])
            tsdb.save_disk_io_data(directory, time, data['disk_io'])
            tsdb.save_temps_data(directory, time, data['temps'])
            tsdb.save_fans_data(directory, time, data['fans'])
        else:
            print("Throwing away data:", data)

    def _handle_job_data(self, ip_addr, time, data):
        self.connected_harvesters[ip_addr]['job_update'] = time
        for job in data:
            job['harvester_ip'] = ip_addr
        database.update_database(data)

    def _handle_chia_data(self, ip_addr, time, data):
        if 'ts_data_folder' in self.connected_harvesters[ip_addr]:
            directory = self.connected_harvesters[ip_addr]['ts_data_folder']
            tsdb.save_chia_data(directory, time, data)
        else:
            print("Throwing away data:", data)


async def main():
    server = Server()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
