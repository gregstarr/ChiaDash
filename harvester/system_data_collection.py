import subprocess
import re
import psutil


def get_system_data():
    job_processes = []
    for process in psutil.process_iter():
        if "chia" not in process.name():
            continue
        with process.oneshot():
            proc_dict = {
                'pid': process.pid,
                'command': process.cmdline(),
                'name': process.name(),
                'cpu_num': process.cpu_num(),
                'cpu_percent': process.cpu_percent(),
                'cpu_times': process.cpu_times(),
                'status': process.status(),
                'memory': process.memory_full_info(),
            }
        job_processes.append(proc_dict)
    return {
        'job_processes': job_processes
    }
