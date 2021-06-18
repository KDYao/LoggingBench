import os
import argparse
import platform
import socket
import pandas as pd
import numpy as np

from functools import wraps
from threading import Thread
from multiprocessing import Process, Pool
from enum import Enum


def check_existance(f, type=''):
    """
    Check if file or folder exists
    Parameters
    ----------
    f: file/folder path
    type: "d" for folder, "f" for file

    Returns
    f: path if exists
    -------

    """
    if type == "d":
        if os.path.isdir(f):
            return f
        else:
            raise NotADirectoryError('Folder not exist: %s' % f)
    elif type == "f":
        if os.path.isfile(f):
            return f
        else:
            raise FileNotFoundError('File not exist: %s' % f)
    else:
        # Check both
        if os.path.isdir(f) or os.path.isfile(f):
            return f
        else:
            raise AttributeError('Path not exist: %s' % f)

def parse_args_size_level(*args, **kwargs):
    """
    Parse input params for calculating the size/SLOC of a project
    Returns
    -------
    args: parsed arguments
    """
    parser = argparse.ArgumentParser(description='Input args for calculating the size/SLOC of a project', *args, **kwargs)
    parser.add_argument('-l',
                        '--size_level',
                        type=str,
                        default=None,
                        help="""
                        Please input the size levels to be analyzed
                        The types of levels: 
                            - small
                            - medium
                            - large
                            - vlarge
                        You can specify on level or multiple levels joined by comma
                        """,
                        required=True)

    return parser.parse_known_args()

def getPath(param_str: str):
    """
    Get path according to current OS platform/System Name
    -------
    Parameters
    -------
    param_str: The string of parameters
    ------
    Returns
    -------
    path
    """
    pathes = {
        'REPO_ZIPPED_ROOT': {
            # OS
            'DARWIN': '/Users/yaokundi/Documents/Project/2021/LoggingBench/test',
            'LINUX': {
                'BRAIN2': '/home/local/SAIL/kundi/BACKUP/dataset/GitJavaLoggingRepos',
                'PINKY': '/home/local/SAIL/kundi/BACKUP/dataset/GitJavaLoggingRepos',
                'COMPUTECANADA': ''
            }
        }
    }

    try:
        # Get path: By key --> OS
        p = pathes[param_str.upper()][platform.system().upper()]
        if isinstance(p, dict):
            # Choose by server name
            serverName = socket.gethostname().upper()
            if serverName in p.keys():
                p = p[serverName]
            else:
                # If unknown server name, we assume that's from compute canada (not sure if clusters have own names)
                p = p['COMPUTECANADA']
    except Exception as e:
        raise RuntimeError('Error getting path for %s: %s' % (param_str, e))

    return check_existance(p)


def getWorkers(cpus=None):
    """
    Return the number of CPUs we can use, based on machine type;
    We basically use half of the CPUs for brain2 or pinky
    For other machines (e.g. Compute Canada Nodes) we use all available CPUs
    Returns
    -------

    """
    cpu_count = os.cpu_count()
    if cpus:
        if isinstance(cpus, float):
            return round(cpus * cpu_count)
        elif isinstance(cpus, int):
            return cpus
        else:
            raise TypeError('Unrecogzied input type %s' % str(cpus.__name__))
    limit_usage_machines = ['brain2', 'pinky']
    if socket.gethostname().lower() in limit_usage_machines:
        return cpu_count // 2
    else:
        # Use all cpus if not on SAIL servers
        return cpu_count

def run_async(func):
    """
    Run function in parallel
    Be aware that the wrapper may cause error in python 3.8 or above
    https://discuss.python.org/t/is-multiprocessing-broken-on-macos-in-python-3-8/4969
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # TODO: If queue needed, just uncomment the following code,
        #  and get by [job.get() for job in [func(x,y...) for i in ITERATION]]
        # queue = Queue()
        # thread = Thread(target=func, args=(queue,) + args, kwargs=kwargs)
        # thread.start()
        # return queue
        thread = Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        #thread.join()
        return thread
    return wrapper


def run_async_multiprocessing(func):
    """
    Run function in parallel
    Be aware that the wrapper may cause error in python 3.8 or above
    https://discuss.python.org/t/is-multiprocessing-broken-on-macos-in-python-3-8/4969
    :param func:
    :return:
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        proc = Process(target=func, args=args, kwargs=kwargs)
        proc.start()
        return proc
    return wrapper


def chunkify(data, chunks):
    """
    Split list or dataframe into multiple chunks
    :param data:
    :param chunks:
    :return:
    """
    if isinstance(data, list):
        k, m = divmod(len(data), chunks)
        project_info_chunks = [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(chunks)]
        return project_info_chunks
    elif isinstance(data, pd.DataFrame):
        df_split = np.array_split(data, chunks)
        return df_split