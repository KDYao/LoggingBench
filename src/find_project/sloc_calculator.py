import multiprocessing
import os
import shutil
import sys
import subprocess
import tarfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import logging
import ast
from src.util.utils import getPath, parse_args_size_level, chunkify
try:
    from readerwriterlock import rwlock
    lock = rwlock.RWLockWrite().gen_wlock()
except ImportError:
    from threading import Lock
    lock = Lock()

global logger
logger = logging.getLogger(__name__)


def csv_loader(f):
    if not os.path.isfile(f):
        raise FileNotFoundError('File {} not found'.format(f))
    return pd.read_csv(f)

def update_repo_lists(df_projects, root_dir):
    df_projects['repo_path'] = df_projects['owner_repo'].apply(
        lambda x: os.path.join(root_dir, '{}.tar.gz'.format(x.replace('/', '_')))
    )
    return df_projects


def check_uncompressed_size(df, filetype='java', out_f=None):
    res_all = []
    for idx, row in df.iterrows():
        res = row.to_dict()

        repo_path = res['repo_path']
        folder_d = repo_path.rstrip('.tar.gz')

        if os.path.isdir(folder_d): shutil.rmtree(folder_d)

        if not os.path.isfile(repo_path):
            logger.warning('Unable to locate file: %s; skip' % repo_path)
            continue

        # Unzip tar since scc seems not accept processing on the fly
        tar = tarfile.open(repo_path, "r:gz")
        tar.extractall(path=folder_d)
        tar.close()

        if isinstance(filetype, str):
            cmd = "scc --no-complexity  --include-ext {ext} -f json {d}".format(d=folder_d, ext=filetype)
        elif isinstance(filetype, list):
            cmd = "scc --no-complexity  --include-ext {exts} -f json {d}".format(
                d=folder_d, exts=','.join(filetype))
        # Example:
        # [{"Name":"Java","Bytes":3034863,"CodeBytes":0,"Lines":99397,"Code":46919,"Comment":40154,"Blank":12324,"Complexity":0,"Count":523,"WeightedComplexity":0,"Files":[]}]
        out = subprocess.check_output(cmd, shell=True).decode('utf-8').rstrip()

        if os.path.isdir(folder_d): shutil.rmtree(folder_d)

        for res_sloc_per_ext in ast.literal_eval(out):
            res_all.append({**res, **dict(res_sloc_per_ext)})
        logger.info('Finish calculating SLOC of %s' % os.path.basename(repo_path))

    if out_f:
        to_csv(res=res_all, f=out_f)

    return res_all

def check_uncompressed_size_parallel(df_projects, out_f, file_type = 'java', chunks=20, ):
    """
    Check SLOC in parallel
    :param df:
    :param chunks:
    :return:
    """
    jobs = []
    for df in chunkify(data=df_projects, chunks=2):
        jobs.append(
            multiprocessing.Process(target=check_uncompressed_size, args=(df, file_type, out_f, ))
        )
    [j.start() for j in jobs]
    [j.join() for j in jobs]



def to_csv(res, f):
    """
    Append/write list of dicts to csv
    :param df:
    :param f:
    :return:
    """
    df = pd.DataFrame(res)
    with lock:
        if os.path.isfile(f):
            df.to_csv(f, index=False, mode='a', header=False)
        else:
            df.to_csv(f, index=False, mode='w', header=True)

def setlogger(f_log):
    if not os.path.isdir(os.path.dirname(f_log)):
        os.makedirs(os.path.dirname(f_log))
    logging.basicConfig(
        filename=os.path.join(f_log),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S')

def output_prepare(f):
    if not os.path.isdir(os.path.dirname(f)):
        os.makedirs(os.path.dirname(f))
    if os.path.isfile(f):
        os.remove(os.path.abspath(f))

if __name__ == '__main__':
    # Select sizes: there are few size options
    # small, medium, large, vlarge
    args, _ = parse_args_size_level()
    size_types = [x.strip() for x in args.size_level.split(',')]
    # Compressed files directory
    root_dir = getPath('REPO_ZIPPED_ROOT')
    # The output locaiton
    out_f = os.path.abspath('../../result/proj_sloc/filesize_sloc_{}.csv'.format('_'.join(size_types)))
    output_prepare(out_f)

    # Set logger
    setlogger(os.path.abspath('../../log/sloc_calculator/proj_sloc_{}.log'.format('_'.join(size_types))))
    # Reading configs
    df_projects = csv_loader('../../conf/log_repo_all.csv')
    # Filter selected
    df_projects = df_projects.loc[df_projects['size'].isin(size_types)]
    ## The function sets local path for each repo
    df_projects = update_repo_lists(df_projects, root_dir)

    # Check size
    res_all = check_uncompressed_size_parallel(df_projects=df_projects, chunks=20, out_f=out_f)

    # If use multiprocessing, add lock & append
    # res_all = check_uncompressed_size(df_projects, 'java')
    #pd.DataFrame(res_all).to_csv(out_f, index=False)
