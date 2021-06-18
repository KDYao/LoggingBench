"""
This script merges Chen's data and list the basic info of selected projects
More information might be needed in the near future
"""

import os
import shutil
import sys
import subprocess
import tarfile

import pandas as pd
import logging
import ast
from src.util.utils import getPath, parse_args_size_level
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

global logger
logger = logging.getLogger(__name__)


def csv_loader(f):
    if not os.path.isfile(f):
        raise FileNotFoundError('File {} not found'.format(f))
    return pd.read_csv(f)

def get_repo_lists(df_projects, df_repos, root_dir):
    proj_ids = [int(x.split('-')[0]) for x in df_projects['project']]
    df_repos_filered = df_repos.loc[df_repos['project_id'].isin(proj_ids)]
    df_repos_filered['repo_path'] = df_repos_filered['owner_repo'].apply(
        lambda x: os.path.join(root_dir, '{}.tar.gz'.format(x.replace('/', '_')))
    )
    return df_repos_filered


def check_uncompressed_size(df, filetype='java'):
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
    return res_all


def setlogger(f_log):
    if not os.path.isdir(os.path.dirname(f_log)):
        os.makedirs(os.path.dirname(f_log))
    logging.basicConfig(
        filename=os.path.join(f_log),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d-%b-%y %H:%M:%S')

if __name__ == '__main__':
    # Select sizes: there are few size options
    # small, medium, large, vlarge
    size_types = ['small', 'medium', 'large', 'vlarge']
    # Compressed files directory
    root_dir = getPath('REPO_ZIPPED_ROOT')
    res_f = os.path.abspath('../../conf/log_repo_all.csv')
    if not os.path.isdir(os.path.dirname(res_f)):
        os.makedirs(os.path.dirname(res_f))

    dfs = []

    for size in size_types:
        # Reading configs
        df_projects = csv_loader('../../conf/log_all_stats.csv')
        # Filter selected
        df_projects = df_projects.loc[df_projects['size'] == size]
        df_repos = csv_loader('../../conf/project_clean.csv')
        df_repos_filered = get_repo_lists(df_projects, df_repos, root_dir)
        df_repos_filered['size'] = [size] * df_repos_filered.shape[0]

        dfs.append(df_repos_filered)
    df = pd.concat(dfs)
    df.to_csv(res_f, index=False)