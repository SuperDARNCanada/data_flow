import subprocess
from os.path import getsize

# The purpose of this script is to abstract the 'gatekeeper' tests (bzip2 integrity and file size) such that all three
# mirrors may import this script as a module. This module contains 2 functions: the first function performs the bzip2
# integrity test; the second function performs the file size check. In both cases, a returned value of 0 represents the
# rawacf file passing the corresponding test and a return value of 1 represents the rawacf file failing the
# corresponding test.
# See details below.


def bzip2_integrity_check(file, filepath):
    """

    Parameters
    ----------
    file (Str): Name of rawacf file to be checked
    filepath (Str): Directory where rawacf file exists.

    Returns
    -------
    return_code (Int): 2 if file not found
                       1 if file failed bzip2 integrity check
                       0 if file passed bzip2 integrity check
    log_msg (Str): Log message describing the output of the bzip2 integrity test.
    """
    # Perform bzip test on data file (-t == test)
    bunzip2_process = subprocess.Popen(f"cd {filepath}; bunzip2 -t {file}",
                                       shell=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
    bzip_return_code = bunzip2_process.returncode

    if bzip_return_code == 1 or bzip_return_code == 3:
        # File not found
        log_msg = f"Error. File {file} not found by bunzip2 test."
        return_code = 2
    elif bzip_return_code == 2:
        # Error with bz2 integrity of file.
        log_msg = f"Error. File {file} failed bzip2 integrity test!"
        return_code = 1
    else:
        log_msg = f"Success. File {file} passed bzip2 integrity test!"
        return_code = 0

    return return_code, log_msg


def file_size_check(file, filepath):
    """

    Parameters
    ----------
    file (Str): Name of rawacf file to be checked
    filepath (Str): Directory where rawacf file exists.

    Returns
    -------
    return_code (Int): 1 if file is empty (fail)
                       0 if file is not empty (pass)
    log_msg (Str): Log message stating whether the rawacf file is empty or contains data.
    """
    filesize = getsize(f"{filepath}{file}")

    if filesize <= 55:
        # BZ2 header is 55 bytes
        log_msg = f"Error. File {file} is empty or smaller than bz2 header file."
        return_code = 1
    else:
        log_msg = f"Success. File {file} contains data (not empty)."
        return_code = 0

    return return_code, log_msg
