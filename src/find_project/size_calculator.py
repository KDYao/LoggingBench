import os
import sys
import subprocess
import pandas as pd
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

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


def check_uncompressed_size(df):
    res_all = []
    for idx, row in df.iterrows():
        res = row.to_dict()
        repo_path = res['repo_path']
        if not os.path.isfile(repo_path):
            logger.warning('Unable to locate file: %s; skip' % repo_path)
            res['size_mb'] = None
            res_all.append(res)
            continue
        cmd = "tar tzvf %s | awk '{s+=$3} END{print (s/1024/1024), MB}'" % repo_path
        size_mb = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        res['size_mb'] = size_mb
        res_all.append(res)
        print('The size of %s is %s MB' % (os.path.basename(repo_path), str(size_mb)))
    return res_all


if __name__ == '__main__':
    # Select sizes: there are few size options
    # small, medium, large, vlarge
    size_types = ['vlarge']
    # Compressed files directory
    root_dir = '/home/local/SAIL/kundi/BACKUP/dataset/GitJavaLoggingRepos'
    res_f = 'res/filesize_mb_{}.csv'.format('_'.join(size_types))
    df_projects = csv_loader('conf/log_all_stats.csv')
    # Filter selected
    df_projects = df_projects.loc[df_projects['size'].isin(size_types)]
    df_repos = csv_loader('conf/project_clean.csv')
    df_repos_filered = get_repo_lists(df_projects, df_repos, root_dir)

    # Check size
    res_all = check_uncompressed_size(df_repos_filered)
    # If use multiprocessing, add lock & append
    pd.DataFrame(res_all).to_csv(res_f, index=False)
