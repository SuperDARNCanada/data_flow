import subprocess
from os.path import getsize
import hashlib

# The purpose of this script is to abstract the 'gatekeeper' tests (bzip2 integrity and file size) such that all three
# mirrors may import this script as a module. This module contains 2 functions: the first function performs the bzip2
# integrity test; the second function performs the file size check. In both cases, a returned value of 0 represents the
# rawacf file passing the corresponding test and a return value of 1 represents the rawacf file failing the
# corresponding test.
# See details below.


# Function to check if current rawacf file is in blocklist (get from DDWG github)
# Return 0 if not in blocklist, 1 if in blocklist
def blocklist_check(file, blocklist):
    """
    Parameters
    ----------
    file (Str): Name of rawacf file to be checked
    blocklist (Str): Blocklist file along with its local path. (/path/to/blocklist.txt)

    Returns
    -------
    return_code (Int): 1 if file found in blocklist
                       0 if file not found in blocklist
    log_msg (Str): Log message stating whether the file exists in the blocklist.
    """
    with open(blocklist, 'r') as bl:
        blocklist_files = bl.readlines()

    if file in blocklist_files:
        log_msg = f"File {file} found in blocklist."
        return_code = 1
    else:
        log_msg = f"File {file} not found in blocklist."
        return_code = 0

    return return_code, log_msg


def sha1hashing(file, filepath):
    """
    Function that computes sha1sum hash of a given file. Needs python package 'hashlib'

    Parameters
    ----------
    file (Str): Name of rawacf file to be hashed
    filepath (Str): Directory where rawacf file exists.

    Returns
    -------
    hash (Str): Sha1sum hash of given rawacf file. If function returns empty string for hash, sha1sum failed.
    logmsg (Str): Log message stating whether sha1sum was successful.
    """
    hash = ""
    try:
        sha1 = hashlib.sha1()
        with open(f'{filepath}/{file}', 'rb') as file_to_hash:
            while data := file_to_hash.read(65536):  # true until end of buffer
                sha1.update(data)
        hash = sha1.hexdigest()
        logmsg = f"Successfully hashed {file} in {filepath}: {hash}"
    except Exception as e:
        logmsg = f"Failed to hash {file} in {filepath} with error: {e}"

    return hash, logmsg


# Function to compare sha1sum hash of rawacf file with yyyymm.hashes.
# Return 0 if dne on mirror, 1 if hashes match, 2 if hashes don't match, 3 if file failed to be hashed.
def hash_check(file, filepath, hashfile, hashfile_path):
    """
    Function that compares sha1sum hash of a given rawacf file to its corresponding yyyymm.hashes on the mirror.

    Parameters
    ----------
    file (Str): Name of rawacf file to be hashed
    filepath (Str): Directory where rawacf file exists.

    Returns
    -------
    return_code (Int): 3 if file failed to be hashed in sha1hashing() function
                       2 if file exists on mirror but hashes don't match
                       1 if file exists on mirror and hashes do match
                       0 if file does not exist on mirror
    logmsg (Str): Log message stating results of sha1sum comparison
    """

    hash, sha1_log = sha1hashing(file, filepath)

    if hash == "":
        log_msg = sha1_log
        return_code = 3
    else:
        # Store yyyymm.hashes file in dictionary format
        ym_hashes = {}
        with open(f"{hashfile_path}/{hashfile}", 'r') as hash_file:
            for line in hash_file:
                (val, key) = line.split()
                ym_hashes[key] = val

        if file in ym_hashes.keys():     # Rawacf already exists on mirror
            if hash != ym_hashes[file]:  # Hashes don't match
                log_msg = f"File {file} exists on mirror but hash does not match!"
                return_code = 2
            else:                        # Hashes match
                log_msg = f"File {file} exists on mirror and hashes match!"
                return_code = 1
        else:                            # Rawacf does not exist on mirror yet. New data!
            log_msg = f"File {file} does not exist on mirror. New rawacf data!"
            return_code = 0

    return return_code, log_msg


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

    # Should this be 14 or 55 bytes?
    if filesize <= 55:
        # BZ2 header is 55 bytes
        log_msg = f"Error. File {file} is empty or smaller than bz2 header file."
        return_code = 1
    else:
        log_msg = f"Success. File {file} contains data (not empty)."
        return_code = 0

    return return_code, log_msg
