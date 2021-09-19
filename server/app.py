import asyncio
from quart import Quart
import server


app = Quart(__name__)
s = server.Server()


@app.route('/')
async def hello():
    return "\n".join([f"{addr}: {n_plots}" for addr, n_plots in s.harvester_n_plots.items()])


async def main():
    asyncio.create_task(s.start())
    await app.run_task()


if __name__ == "__main__":
    asyncio.run(main())
