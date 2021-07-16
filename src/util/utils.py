import math
import os
import argparse
import platform
import socket
import pandas as pd
import numpy as np
import logging
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
            raise FileNotFoundError('Folder not exist: %s' % f)
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
            raise FileNotFoundError('Path not exist: %s' % f)

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

def parse_args_clone_detection(*args, **kwargs):
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
    parser.add_argument('--language',
                        type=str,
                        default='java',
                        help="The language type to be analyzed. \n"
                             "Currently, NiCad uses the included plugin to handle source files written in the "
                             "following languages: C (.c), C# (.cs), Java (.java), Python (.py), "
                             "PHP (.php), Ruby (. Rb), WSDL (.wsdl) and ATL (.atl).")
    parser.add_argument('--granularity',
                        type=str,
                        default='blocks',
                        help="Currently NiCad can handle granularity: functions and blocks.")
    parser.add_argument('--clonetype',
                        type=str,
                        default='default',
                        help="The clone types to be examined. \n"
                             "NiCad can detect 1-3 types of clones.\n"
                             "The predefined configuration for each NiCad configuration is inconfig/ In the subdirectory.\n"
                             "The predefined configurations include type1, type2, type2c, type3-1, type3-2, and type3-c\n"
                             "To detect type 1 (exact) clones, setthreshold=0.0\n"
                             "To detect type 2 (rename) clones, setthreshold=0.0 with rename=blind\n"
                             "To detect type 2c (consistent rename) clones, setthreshold = 0.0 with rename=consistent\n"
                             "To detect type 3-1 (near miss exact) clones, setthreshold=0.3\n"
                             "To detect type 3-2 (near miss rename) clones, setthreshold = 0.3 with rename=blind\n"
                             "To detect type 3-2c (near miss and consistently rename) clones, setthreshold=0.3 with rename=consistent\n"
                             "Note1: type 2 includes type 1, type 3-1 includes type 1, and type 3-2 includes types 1 and 2.\n"
                             "Note2: default uses type 3-2")
    return parser.parse_known_args()


def getPath(param_str: str, ischeck=True):
    """
    Get path according to current OS platform/System Name
    -------
    Parameters
    -------
    param_str: The string of parameters
    ischeck: Check if path exists
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
        },
        'NICAD_ROOT':{
            'DARWIN': '/Users/yaokundi/Documents/Project/2021/ExternalTools/NiCad-6.2',
            'LINUX': {
                'BRAIN2': '/home/local/SAIL/kundi/configs/NiCad-6.2',
                'PINKY': '/home/local/SAIL/kundi/configs/NiCad-6.2'
            }
        },
        'CLEANED_PROJ_ROOT': {
            # OS
            'DARWIN': '/Users/yaokundi/Documents/Project/2021/LoggingBench/temp/projects_clean',
            'LINUX': {
                # TODO: FIXME
                'BRAIN2': '',
                'PINKY': '',
                'COMPUTECANADA': ''
            }
        },
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

    if ischeck:
        return check_existance(p)
    else:
        return p


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
        return cpu_count // 4
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


def output_prepare(f):
    if not os.path.isdir(os.path.dirname(f)):
        os.makedirs(os.path.dirname(f))
    if os.path.isfile(f):
        os.remove(os.path.abspath(f))


def setlogger(f_log, logger=None, level=logging.INFO):
    if not os.path.isdir(os.path.dirname(f_log)):
        os.makedirs(os.path.dirname(f_log))
    logging.basicConfig(
        filename=os.path.join(f_log),
        level=level,
        format='%(asctime)s - %(process)d - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S')
    if logger:
        return logging.getLogger(logger)
    else:
        return logging.getLogger()

def setRWLock():
    # Setup io lock
    try:
        from readerwriterlock import rwlock
        lock = rwlock.RWLockWrite().gen_wlock()
    except ImportError:
        from threading import Lock
        lock = Lock()
    return lock

def csv_loader(f):
    if not os.path.isfile(f):
        raise FileNotFoundError('File {} not found'.format(f))
    return pd.read_csv(f)

def create_folder_if_not_exist(d):
    """
    Create directory if not exists
    Parameters
    ----------
    d

    Returns
    -------

    """
    if not os.path.isdir(d):
        os.makedirs(d)

def print_msg_box(msg, indent=1, width=None, title=None):
    """
    Print message-box with optional title.
    Ref: https://stackoverflow.com/questions/39969064/how-to-print-a-message-box-in-python/40080853
    """
    lines = msg.split('\n')
    space = " " * indent
    if not width:
        width = max(map(len, lines))
    box = f'╔{"═" * (width + indent * 2)}╗\n'  # upper_border
    if title:
        box += f'║{space}{title:<{width}}{space}║\n'  # title
        box += f'║{space}{"-" * len(title):<{width}}{space}║\n'  # underscore
    box += ''.join([f'║{space}{line:<{width}}{space}║\n' for line in lines])
    box += f'╚{"═" * (width + indent * 2)}╝'  # lower_border
    print(box)


def convert_size(size_bytes):
    """
    Convert bytes to KB, MB, etc.
    Parameters
    ----------
    size_bytes

    Returns
    -------

    """
    if size_bytes == 0:
       return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])