import asyncio
import json

import database


def format_string(s): return f"'{s}'"


FIX_DATA = {
    'jobs': {
        "start_time": format_string,
        "temp_dir1": format_string,
        "temp_dir2": format_string,
        "final_dir": format_string,
        "plot_id": format_string,
        "process_id": format_string,
        "phase1_time": float,
        "phase2_time": float,
        "phase3_time": float,
        "phase4_time": float,
        "total_time": float,
        "copy_time": float,
        "plot_size": int,
        "buffer_size": format_string,
        "n_buckets": int,
        "n_threads": int,
        "stripe_size": int,
        "status": format_string,
        "harvester_ip": format_string,
    },
    'chia': {
        "n_plots": int
    },
    'system': {

    }
}


class Server:

    def __init__(self):
        self.harvester_n_plots = {}

    async def start(self):
        server = await asyncio.start_server(self.handle_new_connection, host='0.0.0.0', port=8888)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

    async def handle_new_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(addr)
        self.harvester_n_plots[addr] = None
        while True:
            try:
                message = await reader.readuntil(b"$$$")
            except ConnectionError:
                print(f"{addr}: Connection lost")
                break
            except asyncio.streams.IncompleteReadError:
                print(f"{addr}: No more data")
                break
            if message:
                data = json.JSONDecoder().decode(message.decode()[:-3])
                for job in data['jobs']:
                    job['harvester_ip'] = addr[0]
                data = self.fix_data_types(data)
                # print(data)
                database.update_database(data['jobs'])
                self.harvester_n_plots[addr] = data['chia']['n_plots']
                print(f"TOTAL PLOTS: {data['chia']['n_plots']}")

    def fix_data_types(self, data):
        for job in data['jobs']:
            for key in job:
                job[key] = FIX_DATA['jobs'][key](job[key])
        for key in data['chia']:
            data['chia'][key] = FIX_DATA['chia'][key](data['chia'][key])
        return data


async def main():
    server = Server()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
