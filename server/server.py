import asyncio
import json

import database


class Server:

    def __init__(self):
        ...

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
                message = await reader.read()
            except ConnectionError:
                print(f"{addr}: Connection lost")
                break
            if message:
                data = json.JSONDecoder().decode(message.decode())
                print(data)
                database.update_database(data['jobs'])
            else:
                print(f"{addr}: No data")
                break


async def main():
    server = Server()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
