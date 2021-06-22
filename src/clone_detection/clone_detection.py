"""
Clone detection in selected java projects
"""
import multiprocessing
import os
import logging
import sys
import pandas as pd
import shutil
import tarfile
import glob
import subprocess
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import src.util.utils as utils

logger = logging.getLogger(__name__)

class CloneDetection:
    def __init__(self, language, granularity, clonetype):
        self.language = language
        self.granularity = granularity
        self.clonetype = clonetype
        self.NiCadRoot = utils.getPath('NICAD_ROOT')
        # The temp folder for tar file to decompress and anlayze
        self.tmp = '../../temp/projects'
        utils.create_folder_if_not_exist(self.tmp)
        # Result archive path
        self.res_dir = '../../result/clone_detection'
        utils.create_folder_if_not_exist(self.res_dir)

    def clone_detection_in_project(self, df):
        """
        Perform clone detection with NiCad 6.2
        Returns
        -------
        df: The dataframe with projects to be analyzed
        """
        for i, row in df.iterrows():
            repo_path = row['repo_path']
            repo_id = row['project_id']

            if not os.path.isfile(repo_path):
                logger.error('Unable to find path: {}'.format(repo_path))
                continue

            # The project will be decompressed under this directory, and NiCad results will be written here as well
            tmp_out_dir = os.path.abspath(os.path.join(self.tmp, str(repo_id)))

            # Clean temp project if it exists. This could happen when a previous job collapsed
            if os.path.isdir(tmp_out_dir):
                shutil.rmtree(tmp_out_dir)

            # Decompress tar to temp folder
            tar = tarfile.open(repo_path, "r:gz")
            tar.extractall(path=tmp_out_dir)
            tar.close()

            # The temporary decompressed project directory
            tmp_out_proj_dir = os.path.join(tmp_out_dir, os.listdir(tmp_out_dir)[0])

            # NiCad clone deteciton
            # Example: ./nicad5 functions java systems/JHotDraw54b1 default-report
            cmd = ' '.join([
                './nicad6',
                self.granularity,
                self.language,
                tmp_out_proj_dir,
                self.clonetype
            ])
            p = subprocess.Popen(cmd, shell=True, cwd=self.NiCadRoot)
            try:
                p.communicate()
            except Exception as e:
                logger.error('Clone detection fail at project {}, {}'.format(row['repo_name'], str(e)))
                shutil.rmtree(tmp_out_dir)
                continue
            # Check if process succeed
            if p.returncode != 0:
                logger.error('Error in running clone detection for project {}. Command: {}"'.format(
                    row['repo_name'], cmd
                ))
                shutil.rmtree(tmp_out_dir)
                continue

            # Move result to location
            nicad_output_list = glob.glob(tmp_out_proj_dir + '_{}*'.format(self.granularity))
            # Save all results into a tar file
            res_tar_f = os.path.join(self.res_dir, '_'.join([str(repo_id), os.path.basename(repo_path)]))

            with tarfile.open(res_tar_f, mode='w:gz') as tar:
                for f_nicad_out in nicad_output_list:
                    tar.add(f_nicad_out, arcname=os.path.basename(f_nicad_out))
            logger.info('Clone detection finished. Results are saved in {}'.format(res_tar_f))
            # Remove temp out folder
            shutil.rmtree(tmp_out_dir)


def logging_setup(args):
    """
    Prepare logging,
    Parameters
    ----------
    size_types

    Returns
    -------

    """
    size_types = [x.strip() for x in args.size_level.split(',')]
    size_types_str = '_'.join(size_types)

    # out_f = os.path.abspath('../../result/proj_clone/{}.csv'.format(
    #     '_'.join([*args.language, args.granularity, args.clonetype, size_types_str])
    # ))
    # utils.output_prepare(out_f)

    # Set logger
    utils.setlogger(f_log=os.path.abspath('../../log/clone_detection/{}.log'.format(
        '_'.join([args.language, args.granularity, args.clonetype, size_types_str]),
        level=logging.INFO
    )))

def load_projects_list(args):
    """
    Choose all projects to be analyzed and merge them into the same dataframe
    Parameters
    ----------
    args

    Returns
    -------

    """
    sloc_dir = '../../result/proj_sloc'
    dfs = []

    # Concatenate all size types to be analyzed
    for size_type in args.size_level.split(','):
        df_projects = utils.csv_loader(os.path.join(sloc_dir, 'filesize_sloc_{}.csv'.format(size_type.strip())))
        dfs.append(df_projects)
    return pd.concat(dfs)

def parallel_run(df, func):
    """
    Run function in parallel
    :param df:
    :param chunks:
    :return:
    """
    jobs = []
    for df_sp in utils.chunkify(data=df, chunks=utils.getWorkers()):
        jobs.append(
            multiprocessing.Process(target=func, args=(df_sp, ))
        )
    [j.start() for j in jobs]
    [j.join() for j in jobs]

if __name__ == '__main__':
    args, _ = utils.parse_args_clone_detection()
    # Compressed files directory
    cdetec = CloneDetection(
        language=args.language,
        granularity=args.granularity,
        clonetype=args.clonetype
    )
    # Prepare logging
    logging_setup(args)
    # Load target df
    df = load_projects_list(args)

    #cdetec.clone_detection_in_project(df)
    parallel_run(df=df, func=cdetec.clone_detection_in_project)