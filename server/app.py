import asyncio
from quart import Quart, render_template
import server
import database


app = Quart(__name__)
s = server.Server()
navigation = [
    {'name': 'n_plots', 'caption': 'N Plots'},
    {'name': 'current_jobs', 'caption': 'Current Jobs'},
]


@app.context_processor
def get_navigation():
    return dict(navigation=navigation)


@app.route('/')
async def index():
    return await render_template("index.html")


@app.route('/n_plots')
async def n_plots():
    html = await render_template("n_plots.html", harvesters=s.harvester_n_plots)
    return html


@app.route('/current_jobs')
async def current_jobs():
    jobs = database.get_current_jobs()
    for job in jobs:
        del job['status']
        job['plot_id'] = job['plot_id'][:5] + '...' + job['plot_id'][-5:]
    return await render_template("current_jobs.html", current_jobs=jobs)


async def main():
    asyncio.create_task(app.run_task(debug=True))
    await s.start()


if __name__ == "__main__":
    asyncio.run(main())
