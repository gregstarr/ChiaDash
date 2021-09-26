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
                'name': process.name(),
                'cpu_num': process.cpu_num(),
                'cpu_percent': process.cpu_percent(),
                'cpu_times': process.cpu_times()._asdict(),
                'status': process.status(),
                'memory': process.memory_info()._asdict(),
                'io': process.io_counters()._asdict(),
            }
        job_processes.append(proc_dict)
    disk_io = {
        part_name: part_io_counter._asdict()
        for part_name, part_io_counter in psutil.disk_io_counters(True, True).items()
    }
    temps = {
        sensor_name: [s._asdict() for s in sensor_values]
        for sensor_name, sensor_values in psutil.sensors_temperatures().items()
    }
    fans = {
        fan_name: [f._asdict() for f in fan_values]
        for fan_name, fan_values in psutil.sensors_fans().items()
    }
    return {
        'job_processes': job_processes,
        'disk_io': disk_io,
        'temps': temps,
        'fans': fans,
    }


def get_harvester_config():
    disk_parts = [d._asdict() for d in psutil.disk_partitions()]
    cpu_count = psutil.cpu_count()
    net_addrs = {
        nic: [a._asdict() for a in addrs]
        for nic, addrs in psutil.net_if_addrs().items()
    }
    return {
        'disk_parts': disk_parts,
        'cpu_count': cpu_count,
        'net_addrs': net_addrs,
    }
