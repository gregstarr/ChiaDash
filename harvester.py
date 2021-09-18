import asyncio
import json


class Harvester:

    def __init__(self):
        self.server_addr = '192.168.1.143'
        self.server_port = 8888
        self.count = 0

    async def get_data(self):
        await asyncio.sleep(2)
        self.count += 1
        return {'count': self.count}

    async def run_client(self):
        reader, writer = await asyncio.open_connection(self.server_addr, self.server_port)
        new_data_task = asyncio.create_task(self.get_data())
        while True:
            data_dict = await new_data_task  # dictionary
            json_message = json.JSONEncoder().encode(data_dict)
            writer.write(json_message.encode())
            await asyncio.sleep(5)
            new_data_task = asyncio.create_task(self.get_data())


async def main():
    harvester = Harvester()
    await harvester.run_client()

asyncio.run(main())

