import asyncio
from quart import Quart, render_template, Response
import pandas
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg
from io import BytesIO

import server
import database
import tsdb

plt.style.use('ggplot')
app = Quart(__name__)
s = server.Server()
navigation = [
    {'name': 'n_plots', 'caption': 'N Plots'},
    {'name': 'current_jobs', 'caption': 'Current Jobs'},
    {'name': 'job_stats', 'caption': 'Job Stats'},
]


@app.context_processor
def get_navigation():
    return dict(navigation=navigation)


@app.route('/')
async def index():
    return await render_template("index.html")


@app.route('/n_plots')
async def n_plots():
    chia_data = tsdb.get_chia_data()
    html = await render_template("n_plots.html",
                                 harvesters={harv: dat['n_plots'].values[-1] for harv, dat in chia_data.items()})
    return html


@app.route('/n_plots/plot.png')
async def plot_png():
    chia_data = tsdb.get_chia_data()
    fig, ax = plt.subplots()
    psum = 0
    for harv, dat in chia_data.items():
        ax.plot(dat['time'], dat['n_plots'], label=harv)
        psum += dat['n_plots']
    ax.plot(dat['time'], dat['n_plots'], label="sum")
    ax.legend()
    output = BytesIO()
    FigureCanvasAgg(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


@app.route('/current_jobs')
async def current_jobs():
    jobs = database.get_current_jobs()
    for job in jobs:
        del job['status']
        job['plot_id'] = job['plot_id'][:4] + '...' + job['plot_id'][-4:]
    return await render_template("current_jobs.html", current_jobs=jobs)


@app.route('/job_stats')
async def job_stats():
    jobs = database.get_all_jobs()
    jobs = pandas.DataFrame(jobs)
    jobs = jobs[jobs['status'] == 'complete']
    time_fields = ['phase1_time', 'phase2_time', 'phase3_time', 'phase4_time', 'total_time', 'copy_time']
    stats = {'all': {'N': jobs.shape[0]}}
    for field in time_fields:
        stats['all'][field] = {
            'mean': jobs[field].mean() / 3600,
            'standard dev': jobs[field].std() / 3600,
        }
    for harvester_ip in jobs['harvester_ip'].unique():
        harvester_jobs = jobs[jobs['harvester_ip'] == harvester_ip]
        stats[harvester_ip] = {'N': harvester_jobs.shape[0]}
        for field in time_fields:
            stats[harvester_ip][field] = {
                'mean': harvester_jobs[field].mean() / 3600,
                'standard dev': harvester_jobs[field].std() / 3600,
            }
        for hdd in harvester_jobs['temp_dir1'].unique():
            hdd_jobs = harvester_jobs[harvester_jobs['temp_dir1'] == hdd]
            hdd_name = f"{harvester_ip}:{hdd}"
            stats[hdd_name] = {'N': hdd_jobs.shape[0]}
            for field in time_fields:
                stats[hdd_name][field] = {
                    'mean': hdd_jobs[field].mean() / 3600,
                    'standard dev': hdd_jobs[field].std() / 3600,
                }
    return await render_template("job_stats.html", job_stats=stats)


async def main():
    asyncio.create_task(s.start())
    await app.run_task()
    # await app.run_task(host='0.0.0.0', port=5000)


if __name__ == "__main__":
    asyncio.run(main())
