#!/usr/bin/python3
import subprocess as sp
import sys
import os
import shutil
import math
import json

config_path = sys.argv[1]

with open(config_path, 'r') as f:
    config = json.load(f)

DATA_DIR = config['data_dir']
STAGING_DIR = config['staging_dir']

ANTENNA_IQ_BACKUP_DIR = config['antenna_iq_backup_dir']
BFIQ_BACKUP_DIR = config['bfiq_backup_dir']
RAWACF_BACKUP_DIR = config['rawacf_backup_dir']


PYDARN_ENV = config['pydarn_env']
DATA_FLOW_LOCATION = config['data_flow_location']

REMOTE = config['remote']
REMOTE_FOLDER = config['remote_folder']

LOG_DIR = config['log_dir']

EMAILS = config['emails']

MAX_LOOPS = 10

# Delete files if filesystem usage is over this threshold
CAPACITY_LIMIT = 95
CAPACITY_TARGET = 92

# How many files should be deleted at a time in the loop?
DELETE_X_FILES = 12

# The following constant is how many minutes threshold the script will use to find FILES
# to move to the site linux computer. This is so that the script doesn't try to move the current data
# file being written to.
CUR_FILE_THRESHOLD_MINUTES=5

def execute_cmd(cmd):
    """
    Execute a shell command and return the output

    :param      cmd:  The command
    :type       cmd:  string

    :returns:   Decoded output of the command.
    :rtype:     string
    """
    output = sp.check_output(cmd, shell=True)
    return output.decode('utf-8')

def do_mail(subject, body):
    """
    Sends an email with script results.

    :param      subject:  The subject of the email.
    :type       subject:  str
    :param      body:     The body of the email.
    :type       body:     str
    """
    subject_ = "[Data Flow] {}".format(subject)
    mail_cmd = 'echo {} | mail -s {} {}'.format(body, subject_, EMAILS)

    execute_cmd(mail_cmd)

def do_rsync(source, dest, source_files, args=""):
    """
    Formats the list of files into an rsync command and then executes.

    :param      source:        The source directory.
    :type       source:        string
    :param      dest:          The destination directory
    :type       dest:          string
    :param      source_files:  A string of files as an output from find.
    :type       source_files:  string
    :param      args:          Additional arguments to rsync
    :type       args:          string
    """
    rsync = 'rsync -av {} --files-from=- --from0 {} {}'.format(args, source, dest)

    fmt_src_files = source_files.replace(source+'/', '')

    cmd = 'printf "{}" | tr \'\\n\' \'\\0\'| '.format(fmt_src_files) + rsync
    print(cmd)

    try:
        execute_cmd(cmd)
    except sp.CalledProcessError as e:
        print(e)


def do_find(source, pattern, args=''):
    """
    Find files in a directory using a pattern.

    :param      source:   The source directory.
    :type       source:   string
    :param      pattern:  The pattern to match files to.
    :type       pattern:  string
    :param      args:     The arguments to supply to find.
    :type       args:     string

    :returns:   The string of files matching the pattern.
    :rtype:     string
    """

    find = 'find {} -name {} {} 2>/dev/null'.format(source, pattern, args)

    print(find)
    output = execute_cmd(find)

    return output


def do_delete(source_files):
    """
    Deletes the files from the hdd.

    :param      source_files:  The list of source files.
    :type       source_files:  string
    """
    remove = 'printf "{}" | tr \'\\n\' \'\\0\'| xargs -0 rm'.format(source_files)
    print(remove)

    try:
        execute_cmd(remove)
    except sp.CalledProcessError as e:
        print(e)



def clear_old_temp_files():
    """
    Removes any old borealis temp files that might exist if data write was killed before the file
    could be deleted.
    """

    pattern = '*.*.*.*.*.*.*.*.site'
    args = '-cmin +{}'.format(CUR_FILE_THRESHOLD_MINUTES)

    temp_files = do_find(DATA_DIR, pattern, args)

    if temp_files != "":
        do_delete(temp_files)
    else:
        print("No temp files to delete")


def move_new_files():
    """
    Moves new data files from the Borealis output directory to a staging area where they can be
    processed.
    """
    pattern = '*.*.*.*.*.*.*.site'
    args = '-cmin +{}'.format(CUR_FILE_THRESHOLD_MINUTES)

    files_to_move = do_find(DATA_DIR, pattern, args)

    if files_to_move != "":
        rsync_arg = "--remove-source-files"
        do_rsync(DATA_DIR, STAGING_DIR, files_to_move, rsync_arg)
    else:
        subject = "No new files to process and transfer"
        body = "There are no new files to transfer in the output data directory"
        do_mail(subject, body)
        sys.exit(-1)


def restructure_files():
    """
    Convert site files to the array based files.
    """
    pattern = '*.site'

    files_to_restructure = do_find(STAGING_DIR, pattern)

    if files_to_restructure != "":
        pydarn_env = "source {}/bin/activate".format(PYDARN_ENV)
        python_cmd = "python3 {}/data_flow/site-linux/borealis_convert_file.py".format(DATA_FLOW_LOCATION)
        restructure_cmd = 'printf "{}" | tr \'\\n\' \'\\0\' | parallel -0 -P 2 "{};{} {{}}"'.format(files_to_restructure, pydarn_env, python_cmd)

        print(restructure_cmd)
        restructure_output = execute_cmd(restructure_cmd)
    else:
        subject = "Unable to restructure files"
        body = "Unable to restructure files. Files may be missing or corrupted."
        do_mail(subject, body)
        sys.exit(-1)


def backup_files():
    """
    Backup converted files on site.
    """
    pattern = '*.{}.hdf5'

    pattern_dir_pairs = [('rawacf', RAWACF_BACKUP_DIR),
                         ('bfiq', BFIQ_BACKUP_DIR),
                         ('antennas_iq', ANTENNA_IQ_BACKUP_DIR)]

    for pd in pattern_dir_pairs:
        file_pattern = pattern.format(pd[0])

        files_to_backup = do_find(STAGING_DIR, file_pattern)

        if files_to_backup != "":
            do_rsync(STAGING_DIR, pd[1], files_to_backup)
        else:
            subject = "Unable to backup files"
            body = "Unable to backup files. Files may be missing or corrupted."
            do_mail(subject, body)
            sys.exit(-1)

def compress_log_files():
    """
    Compress down the log files being produced by borealis.
    """

    # Compress every file in the LOG_DIR that is: 1) Not compressed with bz2 already and 2)
    # not in use (fuser will return 1 if a file is in use)
    compress_cmd = ('find {} -type f |'
                    ' grep -v *.bz2 |'
                    ' while read filename ; do fuser -s $filename || echo $filename ; done |'
                    ' parallel "bzip2 -z {}/{{}}"').format(LOG_DIR)

    execute_cmd(compress_cmd)


def send_files_home():
    """
    REWRITE WITH GLOBUS
    """
    remote_dest = REMOTE + ":" + REMOTE_FOLDER

    try:
        for ext in ['hdf5', 'bz2']:
            pattern = "*.rawacf.{}".format(ext)
            files_to_send = do_find(STAGING_DIR, p)
            print("sending", files_to_send)

            rsync_arg = '--append-verify --timeout=180'
            do_rsync(STAGING_DIR, remote_dest, rsync_arg)

            rsync_arg = '--checksum --timeout=180'
            do_rsync(STAGING_DIR, remote_dest, rsync_arg)

    except sp.CalledProcessError as e:
        print(e)

def verify_files_are_home():
    """
    REWRITE WITH GLOBUS
    """
    remote_dest = REMOTE + ":" + REMOTE_FOLDER

    extensions = ['bz2', 'hdf5']

    md5sum_ext = []
    for ext in extensions:
        ext_type = "*.rawacf.{}".format(ext)
        md5sum_ext.append('find {} -name ' + ext_type + ' -exec md5sum {{}} +| awk \'{{ print $1 }}\'')


    remote_hashes = []
    our_hashes = []
    for md5sum_cmd in md5sum_ext:

        remote_md5 = md5sum_cmd.format(REMOTE_FOLDER)

        escapes = remote_md5.replace('$', '\\$').replace('*', '\\*')
        get_remote_hashes = 'ssh {} "{}"'.format(REMOTE, escapes)

        output = execute_cmd(get_remote_hashes)
        remote_hashes.extend(output.splitlines())

        get_our_md5 = md5sum_cmd.format(STAGING_DIR)
        output = execute_cmd(get_our_md5)
        our_hashes.extend(output.splitlines())


    if set(our_hashes).issubset(set(remote_hashes)):
        body = "The following file hashes match after transfer.\n"
        body += "\n".join(our_hashes)

        delete = 'rm -r {}/*'.format(STAGING_DIR)
        try:
            execute_cmd(delete)
            subject = "Files were transfered successfully"
        except sp.CalledProcessError as e:
            subject = "Files were transfered but unable to delete staged files afterword"

        do_mail(subject, body)

    else:
        differences = list(set(remote_hashes) - set(out_hashes))

        subject = "Files failed to transfer"

        body = "The following file hashes don't exist/match for files transfered to the server.\n"
        body += "\n".join(differences)

        do_mail(subject, body)


def rotate_files():
    """
    Rotate out old backup files. If a backup drive is starting to fill, this will delete oldest
    files to make space.
    """
    pattern = '*.hdf5'
    args = "-printf \'%T+ %p\\n\'"

    deleted_files = []

    body = ""
    for backup_dir in [ANTENNA_IQ_BACKUP_DIR, BFIQ_BACKUP_DIR, RAWACF_BACKUP_DIR]:

        def get_utilization()
            du = shutil.disk_usage(backup_dir)
            total = float(du[0])
            used = float(du[1])
            return math.ceil(used/total * 100)

        utilization = get_utilization()

        if utilization > CAPACITY_LIMIT:
            loop = 0
            while loop < MAX_LOOPS:
                utilization = get_utilization()

                if utilization > CAPACITY_TARGET:
                    files_to_remove = do_find(backup_dir, pattern, args)

                    files_list = files_to_remove.splitlines()
                    files_list = sorted(files_list)
                    files_list = files_list[:DELETE_X_FILES]
                    files_list = [file.split()[1] for file in files_list]

                    files_str = "\n".join(files_list)

                    if files_str != "":
                        do_delete(files_str)
                        body += files_str + '\n'
                    else:
                        "No files to rotate"

                else:
                    break

                loop += 1

    if body != "":
        subject = "The following old files were rotated"
        do_mail(subject, body)


if not os.path.exists(DATA_DIR):
    subject = "Data directory does not exist"
    body = "The radar data directory does not exist!"
    do_mail(subject, body)
    sys.exit(-1)

if not os.path.exists(LOG_DIR):
    subject = "Log directory does not exist"
    body = "The radar log directory does not exist!"
    do_mail(subject, body)
    sys.exit(-1)

mkdir = 'mkdir -p ' + STAGING_DIR
execute_cmd(mkdir)

mkdir = 'mkdir -p ' + ANTENNA_IQ_BACKUP_DIR
execute_cmd(mkdir)

mkdir = 'mkdir -p ' + BFIQ_BACKUP_DIR
execute_cmd(mkdir)

mkdir = 'mkdir -p ' + RAWACF_BACKUP_DIR
execute_cmd(mkdir)

send_files_home()
verify_files_are_home()

rotate_files()
clear_old_temp_files()
move_new_files()
restructure_files()
backup_files()
compress_log_files()

send_files_home()
verify_files_are_home()















