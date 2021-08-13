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
from collections import defaultdict
from pandarallel import pandarallel
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import src.util.utils as utils
from src.log_remove.log_remover import LogRemover

logger = logging.getLogger(__name__)
lock = utils.setRWLock()

class CloneDetection:
    # Define a global logremover object
    global logremover
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
        self.f_nicad_check = os.path.join(self.res_dir, 'clone_detection_check.csv')
        # Folder to store the nicad log of failed clone detections
        self.d_failed_nicad_logs = '../../temp/failed_nicad_logs'
        utils.create_folder_if_not_exist(self.d_failed_nicad_logs)
        

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

            # Save all results into a tar file
            res_tar_f = os.path.join(self.res_dir, '_'.join([str(repo_id), os.path.basename(repo_path)]))

            # Skip detecting if analyzed file already exists
            # This is to handle cases when server breakdown
            if os.path.isfile(res_tar_f): continue

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

            with tarfile.open(res_tar_f, mode='w:gz') as tar:
                for f_nicad_out in nicad_output_list:
                    tar.add(f_nicad_out, arcname=os.path.basename(f_nicad_out))
            logger.info('Clone detection finished. Results are saved in {}'.format(res_tar_f))
            # Remove temp out folder
            shutil.rmtree(tmp_out_dir)

    def clone_detection_logging_removal(self, df):
        """
        Perform inner project clone detection with NiCad 6.2
        Theres some refactorings to do here but currently we keep it as a separate function
        Returns
        -------
        df: The dataframe with projects to be analyzed
        """
        d_clean_project_root = os.path.join(utils.getPath('TEMP_PROJ_ROOT', ischeck=False), 'inner_proj_clone_detection')
        clone_detection_result = []
        # Save newly added json locally
        logging_remove_json_new = defaultdict(dict)
        for i, row in df.iterrows():
            repo_path = row['repo_path']
            repo_id = str(row['project_id'])
            row['NiCadPassed'] = False

            # Check if the current file is already archived in the logging removal projects folder
            f_proj_logging_remove_tar = os.path.join(d_clean_project_root, '%s.tar.gz' % repo_id)

            # Check if parental folder exists
            if os.path.isfile(repo_path):
                logger.error('Unable to find path: {}'.format(repo_path))
                clone_detection_result.append(row)
                continue

            # The project will be decompressed under this directory, and NiCad results will be written here as well
            tmp_out_dir = os.path.abspath(os.path.join(self.tmp, str(repo_id)))

            # Clean temp project if it exists. This could happen when a previous job collapsed
            if os.path.isdir(tmp_out_dir):
                shutil.rmtree(tmp_out_dir)

            
            if os.path.isfile(f_proj_logging_remove_tar):
                # Decompress tar to temp folder, if this has been already logging removed
                # This will be used for clone detection directly
                tar = tarfile.open(f_proj_logging_remove_tar, "r:gz")
                tar.extractall(path=tmp_out_dir)
                tar.close()
            else:
                # If not file recorded, means the file has not been logging removed, we will perform logging removal on this file
                lrm = logremover.find_and_remove_logging(row=row)
                if lrm is not None:
                    log_remove_repo_id, log_remove_repo_detail = lrm
                    logging_remove_json_new[log_remove_repo_id] = log_remove_repo_detail

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
                clone_detection_result.append(row)
                continue
            # Check if process succeed
            if p.returncode != 0:
                logger.error('Error in running clone detection for project {}. Command: {}"'.format(
                    row['repo_name'], cmd
                ))
                shutil.rmtree(tmp_out_dir)
                clone_detection_result.append(row)
                continue

            # Move result to location
            nicad_output_list = glob.glob(tmp_out_proj_dir + '_{}*'.format(self.granularity))

            with tarfile.open(res_tar_f, mode='w:gz') as tar:
                for f_nicad_out in nicad_output_list:
                    tar.add(f_nicad_out, arcname=os.path.basename(f_nicad_out))
            logger.info('Clone detection finished. Results are saved in {}'.format(res_tar_f))
            # Remove temp out folder
            shutil.rmtree(tmp_out_dir)
            row['NiCadPassed'] = True
            clone_detection_result.append(row)
        
        logremover.dump_remove_logging_result(logging_remove_json_new)
        self.dump_nicad_clone_check_result(df=pd.DataFrame(clone_detection_result))

    def dump_nicad_clone_check_result(self, df):
        """
        Output NiCad clone check result
        :param result:
        :return:
        """

        with lock:
            if os.path.isfile(self.f_nicad_check):
                df.to_csv(self.f_nicad_check, mode='a', index=False, header=False)
            else:
                df.to_csv(self.f_nicad_check, mode='w', index=False, header=True)

    def backup_failed_log(self, d):
        """
        Backup NiCad failed logs if exist
        """
        logs = [x for x in os.listdir(d) if x.endswith('.log')]
        for lg in logs:
            shutil.copy(os.path.join(d, lg), self.d_failed_nicad_logs)

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
        '_'.join([args.language, args.granularity, args.clonetype, size_types_str]), logger='clone_detection',
        level=logging.INFO
    )))

def load_projects_list(args, fromdir, ftype):
    """
    Choose all projects to be analyzed and merge them into the same dataframe
    Parameters
    ----------
    args

    Returns
    -------

    """
    dfs = []

    # Concatenate all size types to be analyzed
    for size_type in args.size_level.split(','):
        df_projects = utils.csv_loader(os.path.join(fromdir, '{ftype}_sloc_{size}.csv'.format(ftype=ftype, size=size_type.strip())))
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

def skip_examined_projects(df):
    """
    Skip projects that have already been examiend
    Parameters
    ----------
    df

    Returns
    -------

    """
    pandarallel.initialize()
    df['Examined'] = df['repo_path'].parallel_apply(lambda x: os.path.isfile(x))
    df.drop(df.loc[df['Examined'] is True].index, inplace=True)
    return df

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

    if args.remove_logging:
        # Run logging removal
        log_remover = f_removal = '../../result/log_remove/logging_removal_lines.json'
        # Saves the dataframe after merging with LU usage table
        d_inner_proj_clone = '../../result/inner_proj_clone' 
        logremover = LogRemover(
            f_removal=f_removal, 
            sample_dir=d_inner_proj_clone,
            sample_sizes=[x.strip() for x in args.size_level.split(',')],
            repeats=0,
            sample_percentage=1.0)
        df = load_projects_list(args, fromdir=d_inner_proj_clone, ftype='inner_project_clone')
        cdetec.clone_detection_in_project(df)
    else:
        # Load target df
        df = load_projects_list(args, fromdir='../../result/proj_sloc', ftype='filesize')
        # Skip projects that have already been examined
        skip_examined_projects(df)
        #cdetec.clone_detection_in_project(df)
        parallel_run(df=df, func=cdetec.clone_detection_in_project)