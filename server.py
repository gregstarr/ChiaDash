import asyncio
import json


class Server:

    def __init__(self):
        self.data = []

    async def run(self):
        serve_task = asyncio.create_task(self.start())


    async def start(self):
        server = await asyncio.start_server(self.handle_new_connection, host='0.0.0.0', port=8888)
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

    async def handle_new_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(addr)
        while True:
            try:
                message = await reader.read(100)
            except ConnectionError:
                print(f"{addr}: Connection lost")
                break
            if message:
                data = json.JSONDecoder().decode(message.decode())
                print(data)
            else:
                print(f"{addr}: No data")
                break


async def main():
    server = Server()
    await server.run()

asyncio.run(main())
