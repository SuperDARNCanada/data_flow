import subprocess
from os.path import getsize

# The purpose of this script is to abstract the 'gatekeeper' tests (bzip2 integrity and file size) such that all three
# mirrors may import this script as a module. This module contains 2 functions: the first function performs the bzip2
# integrity test and outputs the return code (0, 1, 2, or 3); the second function performs the file size check and
# outputs either 0 or 1. In both cases, a returned value of 0 represents the rawacf file passing the corresponding test.
# See details below.

# NOTE: Both functions return 0 upon success of file. Nonzero return values represent failed files.


def bzip2_integrity_check(file, filepath):
    """

    Parameters
    ----------
    file (Str): Name of rawacf file to be checked
    filepath (Str): Directory where rawacf file exists.

    Returns
    -------
    return_code (Int): 1 or 3 if file is not found at filepath
                       2 if file fails unzip test
                       0 if file passes unzip test
    """
    # Perform bzip test on data file (-t == test)
    bunzip2_process = subprocess.Popen(f"cd {filepath}; bunzip2 -t {file}",
                                       shell=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
    return bunzip2_process.returncode


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
    """
    filesize = getsize(f"{filepath}{file}")

    if filesize <= 55:
        return 1
    else:
        return 0
