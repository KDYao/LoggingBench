"""
This script removes logging statements form java projects
"""
import shutil
import tarfile
import threading
from queue import Queue
import os
import re
import itertools
import json
import pandas as pd
import ast
import subprocess
import multiprocessing
from collections import defaultdict
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import src.util.utils as ut
logger = ut.setlogger(
    f_log='../../log/log_removal/log_removal.log',
    logger="log_remover",
)
lock = ut.setRWLock()

class LogRemover:
    def __init__(self, f_removal,
                 sample_dir='../../result/proj_sample',
                 f_log_stats='../../conf/log_all_stats.csv',
                 repeats=1,
                 is_remove_cleaned_project=False,
                 is_archive_cleaned_project=True):
        self.sample_dir = sample_dir
        if not os.path.isdir(sample_dir):
            os.makedirs(sample_dir)
        self.f_removal = f_removal
        ut.create_folder_if_not_exist(os.path.dirname(f_removal))
        # f_removal records processed files and lines in JSON
        if os.path.isfile(f_removal):
            with open(f_removal) as r:
                self.logging_remove_json = json.load(r)
        else:
            self.logging_remove_json = defaultdict(dict)
        self.d_proj_size = '../../result/proj_size'
        self.sample_sizes = ['small', 'medium', 'large', 'vlarge']
        self.repeats = repeats
        self.lu_levels = self.load_lu_levels()
        self.df_proj_lus = self.load_lu_per_project(f_log_stats)
        self.d_clean_project_root = ut.getPath('CLEANED_PROJ_ROOT', ischeck=False)
        ut.create_folder_if_not_exist(self.d_clean_project_root)
        # Generate sampled projects from each size
        self.project_sample()
        self.is_remove_cleaned_project = is_remove_cleaned_project
        self.is_archive_cleaned_project = is_archive_cleaned_project
        self.archive_dir = ut.getPath('CLEAN_REPO_ARCHIVE_ROOT')
        if is_archive_cleaned_project:
            ut.create_folder_if_not_exist(self.archive_dir)

    def load_lu_per_project(self, f):
        """
        Load log_all_stats.csv and get the LUs used in each project
        This file currently is not being tracked since its generated by Chen not us
        We do not wish to disclose too much details before we have Chen's permission
        Parameters
        ----------
        f

        Returns
        -------

        """
        df = ut.csv_loader(f)
        df['project_id'] = df['project'].apply(lambda x: int(x.split('-')[0]))
        keep_cols = ['project_id', 'others'] + [x for x in self.lu_levels.keys() if x in df.columns]
        return df[keep_cols]

    def load_lu_levels(self, f='../../conf/lu_levels.json'):
        """
        Load logging utilities
        Parameters
        ----------
        f
        extra

        Returns
        -------

        """
        with open(f) as r:
         lu_levels = json.load(r)
        return lu_levels

    def filter_row(self, row):
        """
        Check if a row contain general logging utilities
        If listed LU is not in recorded dataset as a separate column, will check it through the 'others' column
        Parameters
        ----------
        row
        cols

        Returns
        If is general LU, and the LU used
        -------

        """
        general_lus = []
        for x in self.lu_levels.keys():
            if x in row.keys():
                ig = row[x]
            else:
                if isinstance(row['others'], str):
                    ig = (x in row['others'])
                else:
                    continue
            if ig is True:
                general_lus.append(x)
        return len(general_lus) > 0, general_lus


    def filter_projects_by_lus(self, df):
        """
        Filter projects by selected logging utilities
        Returns
        -------
        """
        df = pd.merge(df, self.df_proj_lus, on='project_id')
        df[['is_general', 'general_lus']] = df.apply(func=self.filter_row, axis=1, result_type='expand')
        return df[df['is_general'] is True]



    def project_sample(self, sample_percentage=0.1, overwrite=False):
        """
        Sample 10% of the projects from each size
        Parameters
        ----------
        sample_percentage: The percentage of sampling
        overwrite: if overwrite existing files

        Returns
        -------

        """
        sloc_dir = '../../result/proj_sloc'
        ut.print_msg_box('Sample {}% projects from each size'.format(sample_percentage * 100))
        # Concatenate all size types to be analyzed
        for repeat in range(1, self.repeats + 1):
            for size_type in self.sample_sizes:
                f_projects_sample = os.path.join(self.sample_dir, 'sample_{}_sloc_{}.csv'.format(repeat, size_type))
                if os.path.isfile(f_projects_sample):
                    if overwrite:
                        print('Overwrite existing project {}'.format(os.path.basename(f_projects_sample)))
                    else:
                        print('Sample projects already exist in {}; skip'.format(os.path.basename(f_projects_sample)))
                        continue
                df_projects = ut.csv_loader(os.path.join(sloc_dir, 'filesize_sloc_{}.csv'.format(size_type)))
                df_projects = self.filter_projects_by_lus(df=df_projects)
                df_projects_sample = df_projects.sample(frac=sample_percentage, random_state=repeat)
                df_projects_sample.to_csv(f_projects_sample, index=False)

    def get_total_project_size(self, proj_id_list):
        """
        Calculate the total size of selected projects after decompression
        Returns
        -------
        """
        # Merge all projects from size calculation
        df_merged = pd.concat(
            [ut.csv_loader(
                os.path.join(self.d_proj_size, 'filesize_mb_{}.csv'.format(size_type))
            ) for size_type in self.sample_sizes])
        df_merged = df_merged.loc[df_merged['project_id'].isin(proj_id_list)]
        return ut.convert_size(df_merged['size_mb'].sum() * 1024 * 1024)


    def logger_detector(self, repeat_idx):
        """
        Detect java files with logging statements such as logger, etc. followed by a function call.
        Returns
        -------
        """
        # Merge all sampled projects under the same repeat index
        df_merged = pd.concat(
            [ut.csv_loader(
                os.path.join(self.sample_dir, 'sample_{}_sloc_{}.csv'.format(repeat_idx, size_type))
            ) for size_type in self.sample_sizes])

        total_projects_count = df_merged['project_id'].count()
        total_projects_size = self.get_total_project_size(list(df_merged['project_id']))
        total_num_java = df_merged['Count'].sum()
        total_uncompressed_java_size = ut.convert_size(df_merged['Bytes'].sum())
        ut.print_msg_box('Projects Summary\n'
                         'Repeat ID:{rep_id}\n'
                         'Total Projects:{proj_count}\n'
                         'Total Projects Size:{proj_size}\n'
                         'Total Number of Java Files:{num_f_java}\n'
                         'Total Sizes of Java Files:{java_size}'.format(rep_id=repeat_idx,
                                                                        proj_count=total_projects_count,
                                                                        proj_size=total_projects_size,
                                                                        num_f_java=total_num_java,
                                                                        java_size=total_uncompressed_java_size))
        #self.remove_logging_single(df=df_merged, repeat_idx=repeat_idx)
        self.remove_logging_multiprocessing(df=df_merged, repeat_idx=repeat_idx)

    def remove_logging_single(self, df, repeat_idx):
        """
        Remove logging without parallelism
        Parameters
        ----------
        df

        Returns
        -------

        """
        # No Parallel
        logging_remove_json_new = defaultdict(dict)
        log_remove_lst = [self.find_and_remove_logging(row=row, repeat_idx=repeat_idx) for idx, row in df.iterrows()]
        for lrm in log_remove_lst:
            if lrm is not None:
                log_remove_repo_id, log_remove_repo_detail = lrm
                logging_remove_json_new[log_remove_repo_id] = log_remove_repo_detail
        self.dump_remove_logging_result(logging_remove_json_new)


    def remove_logging_multiprocessing(self, df, repeat_idx):
        # Preserve for parallelism
        jobs = []
        for d in ut.chunkify(df, ut.getWorkers()):
            jobs.append(
                multiprocessing.Process(target=self.remove_logging_multithreading, args=(d, repeat_idx, ))
            )
        [j.start() for j in jobs]
        [j.join() for j in jobs]


    def remove_logging_multithreading(self, df, repeat_idx):
        q = Queue()
        ts = [threading.Thread(target=self.find_and_remove_logging, args=(row, repeat_idx, q,)) for idx, row in df.iterrows()]
        for t in ts: t.start()
        for t in ts: t.join()

        log_remove_lst = []
        while not q.empty():
            log_remove_lst.append(q.get())
            q.task_done()

        logging_remove_json_new = defaultdict(dict)
        for lrm in log_remove_lst:
            if lrm is not None:
                log_remove_repo_id, log_remove_repo_detail = lrm
                logging_remove_json_new[log_remove_repo_id] = log_remove_repo_detail

        self.dump_remove_logging_result(logging_remove_json_new)


    def dump_remove_logging_result(self, new_json):
        """
        Save removed logging statements in JSON
        Returns
        -------

        """
        # Skip writing or updating if new_json object is empty
        if not any(new_json): return

        # Save Json
        with lock:
            if os.path.isfile(self.f_removal):
                with open(self.f_removal, 'r+') as f_update:
                    f_update.seek(0)
                    f_update.write(json.dumps(new_json, indent=4))
                    f_update.truncate()
            else:
                with open(self.f_removal, 'w') as f_update:
                    f_update.write(json.dumps(new_json, indent=4))

    def find_and_remove_logging(self, row, repeat_idx, q=None):
        """
        Decompress selected java projects and remove logging statements from them
        Parameters
        ----------
        row: dataframe row, records the information of a project
        repeat_idx: The repeat index of current experiment

        Returns
        -------

        """
        repo_path = row['repo_path']
        repo_id = int(row['project_id'])
        owner_repo = row['owner_repo']

        # Temp location to store project
        tmp_out_dir = os.path.abspath(os.path.join(
            *[self.d_clean_project_root, 'repeat_%d' % repeat_idx, str(repo_id)]
        ))
        # Archived file location
        archived_f = os.path.join(self.archive_dir, '%s.tar.gz' % str(repo_id))

        if str(repo_id) in self.logging_remove_json.keys():
            # Skip remove logging if this project has already been log removed
            if os.path.isdir(tmp_out_dir):
                print('Project %s has already been log removed; skip' % owner_repo)
                return
            # If cleaned project not in temp, but in archived location
            elif os.path.isfile(archived_f):
                # Decompress previously cleaned project from archived file
                # The project has java only so no need to remove non-java files
                print('Cleaned project %s found. Decompressing previously archived project' % owner_repo)
                self.decompress_project(f_tar=archived_f, out_d=os.path.dirname(tmp_out_dir),
                                        clean_project=False, keep_java_only=False)
                return

        # # # FIXME:###### Only for local testing
        # ###########################
        # repo_path = os.path.join(ut.getPath('REPO_ZIPPED_ROOT'), os.path.basename(repo_path))
        # if not os.path.isfile(repo_path): return
        # ###########################

        if not os.path.isfile(repo_path):
            logger.error('Cannot find project %s at %s' % (owner_repo, repo_path))
            return
        print('Start decompression and logging removal from %s' % owner_repo)
        # Decompress
        self.decompress_project(f_tar=repo_path, out_d=tmp_out_dir, keep_java_only=True)

        general_lus = ast.literal_eval(row['general_lus'])
        function_names = set(itertools.chain.from_iterable([self.lu_levels[lu] for lu in general_lus]))

        proj_logging_removal = self.logging_remover_cu_line(d=tmp_out_dir, function_names=function_names)

        # If remove cleaned project from temp folder
        if self.is_remove_cleaned_project:
            shutil.rmtree(tmp_out_dir)

        # If save cleaned project into a separate location
        if self.is_archive_cleaned_project:
            with tarfile.open(archived_f, 'w:gz') as tar:
                tar.add(tmp_out_dir, arcname=os.path.basename(tmp_out_dir))

        if q is not None:
            q.put([repo_id, proj_logging_removal])
        # Record result in json
        return (repo_id, proj_logging_removal)

    def logging_remover_cu_line(self, d, function_names):
        """
        Convert java files with keyword "log" to Compilation Unit then perform single line grep
        Parameters
        ----------
        d: The project directory
        function_names: The function names of log level

        Returns
        -------

        """

        log_related_files = self.get_files_with_keyword(keyword='log', d=d, function_names=function_names)
        self.format_java(d=d, files=log_related_files)

        proj_logging_removal = self.single_line_grep_logging(function_names=function_names, d=d)
        self.remove_logging_by_linenum(dict_removal=proj_logging_removal, d=d, function_names=function_names)
        return proj_logging_removal

    def logging_remover_parenthesis_matching(self, d, function_names):
        """
        Find full logging statements by parenthesis matching
        This is halfly done but we already decided to abandon this approach
        Parameters
        ----------
        d
        function_names

        Returns
        -------

        """
        # FIXME: Remove this function
        proj_logging_removal = self.single_line_grep_logging(function_names=function_names, d=d)

        # Iterate and filtering
        for f_path, f_info in proj_logging_removal.items():
            for line_num, line_content in f_info.items():
                line = line_content.strip().lower()
                is_normal = self.check_normal_logging(line)

    def check_lambda(self, line):
        """
        Check if the source code line contain lambda usage
        Parameters
        ----------
        line

        Returns
        -------

        """
        line_cleaned = re.sub(r'".*?"', '', line.strip())
        if '->' in line_cleaned: return True
        return False

    def check_normal_logging(self, line):
        """
        A full logging statement, start with logger and end with ;
        The parenthesis should be the same
        Parameters
        ----------
        line_content

        Returns
        -------

        """
        # If left/right parenthesis is not the same
        if not self.check_parenthesis(line):
            # If not balanced, remove content in string then check
            # Example: Log.info("**************************)"
            line_str_removed = re.sub(r'".*?"', '', line.strip())
            # Also check if
            if not self.check_parenthesis(line_str_removed):
                return False
        else:
            if 'log' in line.split('.')[0] and line.endswith(';'): return True
            return False

    def check_parenthesis(self, line):
        """
        Check if left&right parenthesis is matching
        Parameters
        ----------
        line

        Returns
        -------

        """
        left_parenthesis = list(line).count('(')
        right_parenthesis = list(line).count(')')
        if left_parenthesis != right_parenthesis: return False
        return True


    def format_java(self, d, files=None):
        """
        Convert Java format to eliminate the syntax error by multi-line greps
        Parameters
        ----------
        d: The directory of the project
        files: log_related_files
        Returns
        -------

        """
        f_javaformatter = os.path.join(*[ut.get_proj_root(), 'resources', 'javaformatter', 'JavaFormatter.jar'])
        if files is None:
            for root, dirnames, filenames in os.walk(d):
                for filename in filenames:
                    if filename.endswith('.java'):
                        cmd = 'java -jar {f_jf} "{f_java}"'.format(f_jf=f_javaformatter, f_java=os.path.join(root, filename))
                        subprocess.Popen(cmd, shell=True).wait()
        else:
            for filename in files:
                if filename.endswith('.java'):
                    cmd = 'java -jar {f_jf} "{f_java}"'.format(f_jf=f_javaformatter,
                                                               f_java=os.path.join(d, filename))
                    subprocess.Popen(cmd, shell=True).wait()

    def get_files_with_keyword(self, keyword, d, function_names):
        """
        Grep files that contain the given keyword
        Parameters
        ----------
        keyword: The keyword(s) to be searched
        d: The directory of the project
        Returns
        -------

        """

        cmd = """grep -ri "%s" --include=*.java . | grep -E "%s" | awk '{print $1}'""" % (keyword, ('|'.join(function_names)))
        out_raw = subprocess.check_output(cmd, shell=True, cwd=d)
        try:
            out = out_raw.decode('utf-8')
        except UnicodeError:
            out = out_raw.decode('iso-8859-1')
        return set([x[:-1] for x in out.split('\n') if x != ''])

    def single_line_grep_logging(self, function_names, d):
        """
        Grep logging statements of single-lined logging
        Cannot handle logging statements that are across multiple lines (unless reformatted)
        Parameters
        ----------
        function_names: logging levels function names regarding to the LU used in this project
        d: The directory of the project

        Returns
        -------
        """
        cmd = 'grep -rinE "(.*log.*)\.({funcs})\(.*\)" --include=\*.java .'.format(
            funcs='|'.join(function_names))
        out_raw = subprocess.check_output(cmd, shell=True, cwd=d)
        try:
            out = out_raw.decode('utf-8')
        except UnicodeError:
            out = out_raw.decode('iso-8859-1')
        # Process results
        re_match = re.compile(r'^./(.*\.java)\:(\d+)\:(.*)$')
        proj_logging_removal = defaultdict(lambda: defaultdict(dict))
        for line in out.split('\n'):
            if line == "": continue
            f_path, line_num, line_content = re_match.match(line).groups()
            # Skip lines in comments
            if line_content.lower().strip().startswith((r'//', r'/*', r'*/')): continue
            line_type = self.check_logging_type(line=line_content.lower().strip(), functions=function_names)
            proj_logging_removal[f_path][int(line_num)] = {'line': line_content, 'linetype': line_type}
        return proj_logging_removal

    def check_logging_guard_type(self, line, functions, supplement_keywords=[]):
        """
        Check if logging guard
        Parameters
        ----------
        line: source code lines
        functions: log level functions to be added
        supplement_keywords: Some other keywords you want to add other search keywords

        Returns
        -------
        """
        # Grep usages such as if(log.isDebug()) or if(trace); this statement can either start with "if" or "else (if)"
        re_match = re.compile('^(if|else\s+if)\s+\((.*?)\)[\s+{].*$')
        keywords = list(functions) + supplement_keywords
        try:
            condition = re_match.match(line).groups()[1]
            if any([x in condition for x in keywords]):
                return 'logging_guard'
            else:
                return 'condition'
        except AttributeError:
            if line.startswith('else '):
                return 'condition'
            return None

    def check_logging_type(self, line, functions):
        """
        Check the type of logging
        Parameters
        ----------
        line

        Returns
        -------
        """
        # Check if lambda: Do not consider lambda
        if self.check_lambda(line=line):
            return 'lambda'
        # Check if logging guard: Replace full line of logging guard
        logging_guard = self.check_logging_guard_type(line=line, functions=functions, supplement_keywords=['log'])
        if logging_guard:
            return logging_guard
        # # Check if normal logging: Replace full line of normal logging
        # if self.check_normal_logging(line=line):
        #     return 'normal'
        # # Replace in full
        # return 'other'
        return 'normal'

    def remove_logging_by_linenum(self, dict_removal, d, function_names):
        """
        Remove logging by line number with "sed" command

        Parameters
        ----------
        dict_removal
        d

        Returns
        -------

        """
        # Iterate each file and remove logging statements
        for f_path, line_info in dict_removal.items():

            # The lines needs to replace specific logging statement
            lst_replace_logging = []
            # The lines can simply replace whole line
            lst_replace_line = []

            for line_id, line_content_info in line_info.items():
                if line_content_info['linetype'] == 'lambda':
                    pass
                elif line_content_info['linetype'] == 'condition':
                    lst_replace_logging.append(line_id)
                else:
                    lst_replace_line.append(line_id)
            f = os.path.join(d, f_path)
            if len(lst_replace_line) > 0:
                # Remove logging statements by line number
                # Cannot write to original file directly since > has a higher priority
                cmd = "awk '%s {gsub(/.*/,\"\")}; {print}' %s > %s_lrm_temp && mv %s_lrm_temp %s" % \
                      (' || '.join(['NR == %d' % x for x in lst_replace_line]), f, f, f, f)
                p = subprocess.Popen(cmd, shell=True)

                try:
                    p.communicate()
                except Exception as ex:
                    logger.error('Fail to remove log for {f}; {ex}'.format(f, str(ex)))

            if len(lst_replace_logging) > 0:
                # Find logging statements and remove them
                # TODO:
                with open(f, 'r+') as fw:
                    f_lines = fw.readlines()
                    for line_id in lst_replace_logging:
                        line_content = f_lines[line_id - 1]
                        try:
                            line_logging = re.match('.*(.*log.*\.({levels})\(.*\))'.format(levels='|'.join(function_names)), line_content, re.IGNORECASE).groups()[0]
                            f_lines[line_id - 1] = line_content.replace(line_logging, '')
                        except Exception as e:
                            logger.error('Fail to replace logging statement in file: {file}, '
                                         'line_num:{line_num}, line: {line}; Error: {e}'.format(
                                file=f_path, line_num=line_id, line=line_content, e=e))
                    # Rewrite file
                    fw.seek(0)
                    fw.write('\n'.join(f_lines))
                    fw.truncate()


    def decompress_project(self, f_tar, out_d, clean_project=True, keep_java_only=True):
        """
        Decompress project into a temporary location
        Parameters
        ----------
        f_tar
        repo_id

        Returns
        -------

        """
        # Clean temp project if it exists. This could happen when a previous job collapsed
        if clean_project:
            if os.path.isdir(out_d):
                shutil.rmtree(out_d)

        # Decompress tar to temp folder
        tar = tarfile.open(f_tar, "r:gz")
        tar.extractall(path=out_d)
        tar.close()

        # If only keep java files
        if keep_java_only:
            subprocess.Popen("find . -type f ! -name '*.java' -delete", shell=True, cwd=out_d).wait()


if __name__ == '__main__':
    f_removal = '../../result/log_remove/logging_removal_lines.json'
    logremover = LogRemover(f_removal)
    for repeat_idx in range(1, 1 + logremover.repeats):
        logremover.logger_detector(repeat_idx)