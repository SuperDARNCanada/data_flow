# coding: utf-8
""" Last modified: 202410 by Saif Marei

 Last modification 202304 by Kevin Krieger
 ported from gatekeeper

 This script is designed to log on to the University of Saskatchewan globus
 SuperDARN mirror in order to upload rawacf files for a specific pattern.
 The script performs various checks on all specified rawacf files in a
 local holding directory and only transfers the files which pass all tests
 to the SuperDARN mirror. Files which fail any of the checks are then
 moved to an appropriate location, given the nature of the failure.

 Call the script like so with the following arguments:
 /path/to/script/gatekeeper_globus -d /path/to/local/holding/dir/ -m /path/to/mirror/root/ -p [pattern]
 Argument 1 is a path to a local holding directory with data you wish to put on the mirror
 Argument 2 is a path to the root of data mirror under which appear the directories for data type
 Argument 3 is the optional pattern, omit to sync all rawacf files
 Run
 python /path/to/script/gatekeeper_globus -h for more information on the usage of this script.

 The script needs to be run on the same machine that the globus personal endpoint is
 running on (i.e. the same machine where the local holding directory is located)
"""
from __future__ import print_function
import globus_sdk
from globus_sdk.scopes import TransferScopes
import inspect
from datetime import datetime, timedelta
from os.path import expanduser, isfile, getsize, isdir
from os import listdir, mkdir, remove, rename, stat
import shutil
import fnmatch
import sys
import subprocess
import time
# Import smtp library and email MIME function for email alerts
import smtplib
from email.mime.text import MIMEText
import pydarnio
import logging
import argparse

from gatekeeper_class import Gatekeeper, parse_data_filename

# Make sure there is only one instance running of this script
from tendo import singleton

me = singleton.SingleInstance()

HOME = expanduser("~")
TRANSFER_RT_FILENAME = f"{HOME}/.globus_transfer_rt"
PERSONAL_UUID_FILENAME = f"{HOME}/.globusonline/lta/client-id.txt"

if isfile(PERSONAL_UUID_FILENAME):
    with open(PERSONAL_UUID_FILENAME) as f:
        PERSONAL_UUID = f.readline().strip()

# Client ID retrieved from https://auth.globus.org/v2/web/developers
gatekeeper_app_CLIENT_ID = 'bc9d5b7a-6592-4156-bfb8-aeb0fc4fb07e'


def main():
    start_time = datetime.now().strftime("%s")

    parser = argparse.ArgumentParser(description='Given a local holding directory and a mirror directory this program'
                                                 'will perform checks on all local rawacf files, transfer all files to'
                                                 'the mirror (or other designated location -- e.g., failed,'
                                                 'blocklisted, nomatch), and update the hash files accordingly.')
    parser.add_argument('-d', '--holding', type=str, default='', help='Path to local holding directory.')
    parser.add_argument('-m', '--mirror', type=str, default='', help='Path to root directory on mirror.')
    parser.add_argument('-p', '--pattern', type=str, default="*rawacf.bz2",
                        help='Sync pattern of rawacf files, default is rawacf.bz2')
    args = parser.parse_args()

    ###################################################################################################################
    # Step 1)
    # Check for refresh token and relevant consents

    # If we have refresh token, try initializing gatekeeper object with it for auto authentication
    if isfile(TRANSFER_RT_FILENAME):
        with open(TRANSFER_RT_FILENAME) as f:
            print("Found refresh token for automatic authentication")
            gk = Gatekeeper(gatekeeper_app_CLIENT_ID, transfer_rt=f.readline())
    # Otherwise, manually authenticate and get a refresh token for future auto authentication
    else:
        print("Need to get transfer refresh token manually for future automatic authentication")
        gk = Gatekeeper(gatekeeper_app_CLIENT_ID)
        # Now check for all possible consents required on the globus endpoint and personal endpoint
        gk.check_for_consent_required()
        gk.check_for_consent_required(PERSONAL_UUID, gk.get_holding_dir())
        if gk.consents:
            print("One of the endpoints being used requires extra consent in order to be used, "
                  "and you must login a second time (dumb, I know) to get those consents.")
        gk.get_auth_with_login(gk.consents)

    ###################################################################################################################
    # Step 2)
    # Check script arguments as well as existence of various directories
    logger = gk.logger

    # Clear out working directory /home/dataman/tmp/* before use
    if isdir(gk.get_working_dir()):
        shutil.rmtree(gk.get_working_dir())
        mkdir(gk.get_working_dir())
        logger.info(f"Clearing out working directory: {gk.get_working_dir()}")
    if not isdir(gk.get_working_dir()):
        sub = f"Directory {gk.get_working_dir()} DNE"
        gk.log_email_exit(logger.error, 1, 1, sub=sub)

    logger.info(f"Args: {args.holding}  {args.mirror}  {args.pattern}")

    # Set holding directory, mirror directory, and sync pattern from parsed arguments
    gk.set_holding_dir(args.holding)
    gk.set_mirror_root_dir(args.mirror)
    gk.set_sync_pattern(args.pattern)

    logger.info("Checking for holding and mirror directories...\n")

    if not isdir(gk.get_holding_dir()):
        sub = f"Holding dir {gk.get_holding_dir()} DNE"
        gk.log_email_exit(logger.error, 1, 1, sub=sub)

    if not gk.check_for_file_existence(gk.get_mirror_root_dir()):
        sub = f"Mirror root dir {gk.get_mirror_root_dir()} DNE"
        gk.log_email_exit(logger.error, 1, 1, sub=sub)

    ###################################################################################################################
    # Step 3)
    # Make a list of files_to_upload consisting of all rawacf files in the holding directory
    # Get some files from mirror: master hashes, failed files list, blocklist directory

    # Get list of files to upload from the holding directory
    # Create files to upload dictionary where keys are filenames and values are empty dictionaries
    # Values will be set in Step 5) after the holding directory is hashed
    files_to_upload = gk.list_of_files_to_upload()
    files_to_upload.sort()
    files_to_upload_dict = {file: {} for file in files_to_upload}
    if len(files_to_upload) == 0:
        msg = "No files to upload. Exiting."
        gk.log_email_exit(logger.error, 0, 1, msg=msg)
    else:
        logger.info(f"Initial set of files to upload ({len(files_to_upload)}): {files_to_upload}\n")

    # Get master hashes file
    logger.info("Getting master hashes file...")
    gk.get_master_hashes()
    if not gk.wait_for_last_task():
        msg = "get_master_hashes timeout. Exiting."
        gk.log_email_exit(logger.error, 0, 1, msg=msg)

    # Get failed files list
    logger.info("Getting failed files list (all_failed.txt)...")
    gk.get_failed()
    if not gk.wait_for_last_task():
        msg = "get_failed timeout. Exiting."
        gk.log_email_exit(logger.error, 0, 1, msg=msg)

    # Recursively get blocklist folder and generate list of blocked files
    logger.info("Getting blocklist directory...\n")
    gk.get_blocklist(dest_path=f"{gk.get_working_dir()}/blocklist/")
    if not gk.wait_for_last_task(timeout_s=120):
        msg = "get_blocklist timeout. Exiting"
        gk.log_email_exit(logger.error, 0, 1, msg=msg)

    ###################################################################################################################
    # Step 4)
    # Make a list of blocked data files from the blocklist/ directory obtained above
    # Remove all blocked data files from files_to_upload
    # Log the list of blocked files in holding_dir and move them to holding_dir/blocked

    # Store all txt files from blocklist directory
    blocklist_files = []
    for f in listdir(f"{gk.get_working_dir()}/blocklist/"):
        if fnmatch.fnmatch(f, "*.txt"):
            blocklist_files.append(f)

    # Store the filenames within the txt files
    # Append filename from beginning of line
    blocked_data = []
    for f in blocklist_files:
        with open(f"{gk.get_working_dir()}/blocklist/{f}") as blocklist_file:
            for line in blocklist_file:
                blocked_data.append(line.strip('\n').strip('\r'))

    # If file in files_to_upload appears in the blocklist, add file to blocked_files_to_remove to be removed below
    blocked_files_to_remove = []
    for data_file in sorted(files_to_upload):
        for blocked_file in blocked_data:
            if data_file in blocked_file:
                blocked_files_to_remove.append(data_file)
    blocked_files_to_remove = sorted(list(set(blocked_files_to_remove)))

    # Remove blocked files from files_to_upload
    # Make blocked dir in holding_dir, /holding_dir/blocked/cur_date/
    # Move blocked files to /holding_dir/blocked/cur_date/
    if len(blocked_files_to_remove) > 0:
        logger.info(f"Found blocked files ({len(blocked_files_to_remove)}): {blocked_files_to_remove}")
        for file_to_remove in blocked_files_to_remove:
            files_to_upload_dict.pop(file_to_remove)

        gk.move_files_to_subdir("Blocked", blocked_files_to_remove)

    ###################################################################################################################
    # Step 5)
    # Hash holding directory and fill files_to_upload dictionary with relevant metadata

    # Do a sha1sum on all files in holding directory,
    sha1sum_process = subprocess.Popen(f"cd {gk.get_holding_dir()}; sha1sum {gk.get_sync_pattern()}",
                                       shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = sha1sum_process.communicate()
    sha1sum_output = out.decode().split("\n")
    sha1sum_error = err.decode().split("\n")
    # Remove empty items from the sha1sum output
    sha1sum_output = [x for x in sha1sum_output if x]
    if sha1sum_process.returncode != 0 or len(sha1sum_output) == 0:
        msg = "Error hashing files. Exiting."
        gk.log_email_exit(logger.error, 0, 1, msg=msg)

    # Fill files_to_upload_dict with relevant metadata
    for item in sha1sum_output:
        filename = item.split()[1]
        data_hash = item.split()[0]
        elements = parse_data_filename(filename)
        data_type = elements[7]
        if data_type == "rawacf":
            data_type = "raw"
        metadata = {'year': f'{elements[0]}', 'month': f'{elements[1]}', 'day': f'{elements[2]}',
                    'yearmonth': filename[0:6], 'hash': data_hash, 'type': data_type}
        files_to_upload_dict[filename].update(metadata)

    ###################################################################################################################
    # Step 6)
    # Get unique list of yyyymm combos from files to upload dictionary
    # Create yearmonth dictionary to organize files_to_upload by their yearmonth
    # Loop through yyyymm combos and get the yyyymm.hashes file from the mirror on each iteration
    # Perform sha1sum comparison between rawacfs in holding dir and recently acquired hashfile in working dir
    # Handle each file individually depending on the result of the sha1sum comparison
    # Log the list of nonmatching files and move them to holding_dir/nomatch/

    # Get unique list of yyyymm combos and create dictionary
    # Keys are yyyymm and values are the files_to_upload_dict items corresponding to the given yyyymm
    yearmonth = sorted(list(set([filename[0:6] for filename in files_to_upload_dict.keys()])))
    yearmonth.sort()
    yearmonth_dict = {ym: {} for ym in yearmonth}
    for ym in yearmonth:
        d = {k: v for k, v in files_to_upload_dict.items() if k[0:6] == ym}
        yearmonth_dict[ym].update(d)

    # Get appropriate hashes files for yyyymm for all files in list
    logger.info(f"Set of years and months for data files in holding directory: {str(yearmonth)}")
    non_matching_files = []
    for ym in yearmonth:
        hash_path = gk.get_hash_file_path(int(ym[0:4]), int(ym[4:6]))
        logger.info(f"Checking if {hash_path} exists on mirror...")
        if gk.check_for_file_existence(hash_path):
            # Get yyyymm.hashes from mirror to working dir
            gk.get_hashes(int(ym[0:4]), int(ym[4:6]), dest_path=gk.get_working_dir())
            if not gk.wait_for_last_task():
                logger.warning(f"Get hashes for {ym} didn't complete. Removing files from files_to_upload")
                # Remove all files w/ given yyyymm from files_to_upload if get_hashes timed out
                for item in list(yearmonth_dict[ym].keys()):
                    files_to_upload_dict.pop(item)
                yearmonth_dict.pop(ym)
            else:
                logger.info(f"{ym} hash file retrieved from mirror.")
                # sha1sum files in holding_dir and compare to yyyymm.hashes now in working dir (-c == compare)
                command_string = f"cd {gk.get_holding_dir()}; sha1sum -c {gk.get_working_dir()}/{ym}.hashes"
                sha1sum_process = subprocess.Popen(command_string, shell=True,
                                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = sha1sum_process.communicate()
                sha1sum_decoded_output = out.decode().split("\n")
                sha1sum_decoded_error = err.decode().split("\n")
                # Loop through result of sha1sum comparison for each file
                # Only remove from files_to_upload if file exists both in holding_dir and hashfile in working_dir
                # Need further investigation into "Failed open or read" and "" results
                for sha1sum_result in sha1sum_decoded_output:
                    hashed_file = sha1sum_result.split(":")[0]
                    if sha1sum_result.find("FAILED open or read") != -1:
                        pass
                    # If hashes do not match, add file to nonmatching files list, remove from files_to_upload
                    elif sha1sum_result.find("FAILED") != -1:
                        logger.warning(f"{hashed_file} hash doesn't match. Adding to no match list, and removing from list of files to upload.")
                        non_matching_files.append(hashed_file)
                        files_to_upload_dict.pop(hashed_file)
                    # If hashes match, remove from files_to_upload as it is already on mirror
                    elif sha1sum_result.find("OK") != -1:
                        logger.info(f"{hashed_file} already exists on mirror and hash matches. Removing from files to upload.")
                        files_to_upload_dict.pop(hashed_file)
                        # Comment out removal of matching files from holding dir for testing purposes
                        ##try:
                        ##    remove(f"{gk.get_holding_dir()}/{hashed_file}")
                        ##except OSError as error:
                        ##    logger.error(f"Error trying to remove file: {error}.")
                    elif sha1sum_result == "":
                        pass
                    else:
                        logger.warning(f"Error, I don't know how to deal with: {sha1sum_result}.")
        # If yyyymm.hashes DNE, create it ONLY IF yyyymm is the current year and month
        else:
            # Need to check if this is the current month, otherwise error out
            # No need to do the above checks for this yearmonth as there is clearly no data for it yet
            if gk.cur_month == int(ym[4:6]) and gk.cur_year == int(ym[0:4]):
                logger.info(f"Hash file for {ym} doesn't exist, creating new directory.")
                gk.create_new_data_dir(ym[0:4], ym[4:6])
            else:
                # Error, previous month's hash files should exist already
                sub = f"Hash file {ym}.hashes not found. Exiting."
                gk.log_email_exit(logger.error, 1, 1, sub=sub)

    # Make nomatch dir in holding_dir, /holding_dir/nomatch/cur_date/
    # Move non-matching files to /holding_dir/nomatch/cur_date/
    if len(non_matching_files) > 0:
        logger.info(f"Found non-matching files ({len(non_matching_files)}): {non_matching_files}\n")
        gk.move_files_to_subdir("Nomatch", non_matching_files)

    ###################################################################################################################
    # Step 7)
    # Bzip check all files in list, and do other checks like file size check
    # Create a dictionary of failed_files, the keys are the filenames (string) and the values are
    # the hash and the reason for failure (strings) in a tuple, which is immutable and fixed in size
    # Log the dictionary of failed files (hash  filename  |  reason for failure)

    failed_files = {}
    # Loop through files_to_upload_dict as it contains only rawacfs still eligible for transfer
    files_to_upload = sorted(list(files_to_upload_dict.keys()))
    for filename in files_to_upload:
        data_file = filename
        data_file_hash = files_to_upload_dict[filename]['hash']
        logger.info(f"bunzip -t {data_file}")
        # Perform bzip test on data file (-t == test)
        bunzip2_process = subprocess.Popen(f"cd {gk.get_holding_dir()}; bunzip2 -t {data_file}",
                                           shell=True, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
        out, err = bunzip2_process.communicate()
        bunzip2_process_output = out.decode().split("\n")
        bunzip2_process_error = err.decode().split("\n")

        filesize = getsize(f"{gk.get_holding_dir()}{data_file}")
        # Check error code of bzip test and log a relevant error message
        # Remove from files_to_upload if any nonzero error code
        if bunzip2_process.returncode == 1 or bunzip2_process.returncode == 3:
            logger.warning(f"OUTPUT: {bunzip2_process_output}")
            logger.warning(f"ERROR: {bunzip2_process_error}")
            # File not found. Remove from files to upload
            logger.warning(f"Error. File {data_file} not found by bunzip2 test. Removing from list.")
            files_to_upload_dict.pop(data_file)
        elif bunzip2_process.returncode == 2:
            # Error with bz2 integrity of file.
            logger.warning(f"Error. File {data_file} failed the bzip2 test! Removing from list.")
            files_to_upload_dict.pop(data_file)
            failed_files[data_file] = (data_file_hash, "Failed BZ2 integrity test")
        elif filesize == 14 or filesize == 0:
            # Data file is empty (header of rawacf is 14 bytes)
            logger.warning(f"File {data_file} empty. Removing from list.")
            files_to_upload_dict.pop(data_file)
            failed_files[data_file] = (data_file_hash, "File contains no records (empty)")
        elif filesize < 14:
            # Data file is smaller than the header (14 bytes)
            logger.warning(f"File {data_file} too small. Removing from list.")
            files_to_upload_dict.pop(data_file)
            failed_files[data_file] = (data_file_hash, "File contains no records (empty)")
        # If file passed bzip test and is not empty, run bzcat to unzip the file
        else:
            # Try using backscatter package to test dmap integrity
            unzipped_filename = data_file.split(".bz2")[0]
            logger.info(f"bzcat {data_file} > {unzipped_filename}")
            bzcat_process = subprocess.Popen(f"cd {gk.get_holding_dir()}; bzcat {data_file} > {unzipped_filename}",
                                             shell=True, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
            out, err = bzcat_process.communicate()
            bzcat_process_output = out.decode().split("\n")
            bzcat_process_error = err.decode().split("\n")
            # Check error code of bzcat and log a relevant error message
            # Remove from files_to_upload if any nonzero error code
            if bzcat_process.returncode == 1 or bzcat_process.returncode == 3:
                logger.warning(f"OUTPUT: {bzcat_process_output}")
                logger.warning(f"ERROR: {bzcat_process_error}")
                # File not found. Remove from files to upload
                logger.warning(f"Error. File {data_file} not found by bzcat. Removing from list.")
                files_to_upload_dict.pop(data_file)
            elif bunzip2_process.returncode == 2:
                # Error with bz2 integrity of file.
                logger.warning(f"Error. File {data_file} failed with bzcat! Removing from list.")
                files_to_upload_dict.pop(data_file)
                failed_files[data_file] = (data_file_hash, "Failed BZ2 integrity test")
            # If bzcat succeeded on file, test dmap integrity and read using pyDARNio
            # If failed, log error message, remove from files_to_upload, add to failed_files
            else:
                try:
                    dmap_stream = open(f"{gk.get_holding_dir()}/{unzipped_filename}", 'rb').read()
                    reader = pydarnio.SDarnRead(dmap_stream, True)
                    records = reader.read_rawacf()
                except Exception as error:
                    errstr = "Error. File {0} failed with error {1}".format(data_file,
                                                                            str(error).replace("\n",
                                                                                               ""))
                    logger.warning(' '.join(errstr.split()))
                    files_to_upload_dict.pop(data_file)
                    errstr = ' '.join(str(error).replace("\n", "").split())
                    failed_files[data_file] = (data_file_hash, errstr)
                else:
                    # At this point, remaining files passed bzip, bzcat, and dmap integrity test
                    # Remaining files are also not empty
                    logger.info(f"{data_file} passed pydarnio dmap tests.")
                finally:
                    # Remove unzipped rawacf from holding_dir (created by bzcat test)
                    remove(f"{gk.get_holding_dir()}/{unzipped_filename}")

    if len(failed_files) > 0:
        logger.info(f"Found failed files ({len(failed_files)}): ")
        for failed in failed_files:
            logger.info(f"{failed_files[failed][0]}  {failed} | {failed_files[failed][1]}")

    ###################################################################################################################
    # Step 8)
    # Append failed files to all_failed.txt in working dir and upload to mirror
    # Transfer failed files to failed directory on mirror
    # Move failed files to holding_dir/failed/

    # Update all_failed.txt with new failed files and upload to mirror
    logger.info("Updating all_failed.txt\n")
    try:
        result = gk.update_failed(failed_files)
        if result is None:
            msg = "Error with updating failed files list on mirror, please check it manually\r\n"
            sub = "error updating all_failed.txt"
            gk.log_email_exit(logger.warning, 1, 0, msg=msg, sub=sub)
        while not gk.wait_for_last_task(timeout_s=300):
            logger.info("Still waiting for failed files list to upload and complete...")
    except Exception as e:
        msg = f"Error: {e}. Please update manually\r\n"
        sub = "error updating all_failed.txt"
        gk.log_email_exit(logger.warning, 1, 0, msg=msg, sub=sub)

    # Upload failed files to failed dir on mirror with a timeout of 60s plus an extra 10s
    # for each additional file
    if len(failed_files) > 0:
        upload_timeout = 60 + 10 * len(failed_files)
        logger.info(f"Uploading failed files to mirror failed dir with {upload_timeout} s timeout")

        if not gk.sync_failed_files_from_list(list(failed_files)):
            msg = "Failed to sync failed files, sync manually."
            sub = "sync_failed_files_from_list failed"
            gk.log_email_exit(logger.warning, 1, 0, msg=msg, sub=sub)

        gk.wait_for_last_task(timeout_s=upload_timeout)
        while not gk.wait_for_last_task():
            logger.info("Still waiting for failed files to upload and complete...")
        if not gk.last_task_succeeded():
            msg = "Don't know which failed files were transferred successfully and which were not!"
            sub = "sync_failed_files_from_list failed to sync failed files, sync manually."
            gk.log_email_exit(logger.warning, 1, 0, msg=msg, sub=sub)

        # Make failed dir in holding_dir, /holding_dir/failed/cur_date/
        # Move failed files to /holding_dir/failed/cur_date/
        gk.move_files_to_subdir("Failed", failed_files)

    ###################################################################################################################
    # Step 9)
    # Upload files_to_upload to mirror

    # Get updated list of files_to_upload from dictionary
    files_to_upload = sorted(list(files_to_upload_dict.keys()))
    logger.info(f"Final set of files to upload ({len(files_to_upload)}): {files_to_upload}")

    # Exit if there are no files to upload
    if len(files_to_upload) == 0:
        msg = "No files to upload. Exiting."
        gk.log_email_exit(logger.info, 0, 1, msg=msg)

    # Similar to failed files, timeout is 60 seconds plus an additional 10 seconds for each file
    upload_timeout = 60 + 10 * len(files_to_upload)
    logger.info(f"Uploading files to mirror with {upload_timeout} s timeout...\n")

    # Now sync the files up to the mirror in the appropriate place
    gk.sync_files_from_list(files_to_upload)
    gk.wait_for_last_task(timeout_s=upload_timeout)
    while not gk.wait_for_last_task():
        logger.info("Still waiting for last task to complete...")
    if not gk.last_task_succeeded():
        msg = "Don't know which files were transferred successfully and which were not!"
        sub = "sync_files_from_list failed"
        gk.log_email_exit(logger.error, 1, 1, msg=msg, sub=sub)

    ###################################################################################################################
    # Step 10)
    # Get a list of files that succeeded the transfer and a list of files that were skipped
    # Create a dictionary and store the string to append to yyyymm.hashes for each yyyymm
    # Remove succeeded files from holding_dir

    # Check which files succeeded in the transfer. If a file was skipped it won't appear in this
    succeeded = gk.get_task_successful_transfers()
    # Create lists of succeeded and skipped files
    succeeded_files = [str(info['destination_path'].split('/')[-1]) for info in succeeded]
    skipped_files = [filename for filename in files_to_upload_dict if filename not in succeeded_files]

    logger.info(f"Skipped files list: {skipped_files}")
    logger.info(f"Skipped files: {gk.get_num_files_skipped()}")
    logger.info(f"Transferred files: {len(succeeded_files)}")
    logger.info(f"Total files: {gk.get_num_files_skipped() + len(succeeded_files)}")
    logger.info(f"Files to upload: {len(files_to_upload)}\n")

    # Make a dictionary to store the '<hash1> <file1> \n <hash2> <file2>' string for each yyyymm.hashes file
    # Use list of succeeded files to get yyyymm bc only succeeded files should be added to hash file
    yearmonth = list(set([filename[0:6] for filename in succeeded_files]))
    yearmonth.sort()
    yearmonth_hash_dict = {ym: "" for ym in yearmonth}

    # All the metadata of interest below is stored in files_to_upload_dict
    # Remove each succeeded file from the holding dir and append "<hash> <filename> \n" to dictionary for yyyymm
    files_not_found = []
    for filename in sorted(list(succeeded_files)):
        file_data = files_to_upload_dict[filename]
        ym = file_data['yearmonth']
        # Make sure "succeeded" file actually made it to the mirror
        data_type = file_data['type']
        year = file_data['year']
        month = file_data['month']
        # Make sure "succeeded" file is truly on the mirror
        # If not, leave file in holding_dir for next script run and do not update yyyymm.hashes for this file
        if gk.check_for_file_existence(f"{gk.mirror_root_dir}/{data_type}/{int(year):04d}/{int(month):02d}/{filename}"):
            data_hash = files_to_upload_dict[filename]['hash']
            #remove(f"{gk.get_holding_dir()}/{filename}")  # Comment this line for testing purposes
            yearmonth_hash_dict[ym] += f"{data_hash}  {filename}\n"
        else:
            files_not_found.append(filename)

    # log and email list of files that appeared to succeed transfer but are not found on mirror
    if files_not_found:
        msg = f"Transfer of {files_not_found} listed as succeeded but not found on mirror! Files will remain in holding directory."
        gk.log_email_exit(logger.warning, 1, 0, msg=msg)

    ###################################################################################################################
    # Step 11)
    # Update the yyyymm.hashes files with their corresponding succeeded files and upload to mirror

    yearmonth = sorted(list(yearmonth_hash_dict.keys()))
    logger.info(f"Updating hash files: {yearmonth}")
    # Update yyyymm.hashes from dictionary and upload to mirror
    for ym in yearmonth:
        hash_string = yearmonth_hash_dict[ym]
        hashfile_path = f"{gk.get_working_dir()}/{ym}.hashes"
        # If string is not empty, append it to hashfile
        if hash_string != "":
            hash_string.strip("\n")
            with open(hashfile_path, 'a') as f:
                f.write(f"{hash_string}")
            # Upload hashfile to mirror
            gk.put_hashes(int(ym[0:4]), int(ym[4:6]),
                          source_path=gk.get_working_dir())
            while not gk.wait_for_last_task():
                logger.info("Still waiting for hashes task to finish... ")
                continue
        else:
            msg = f"{hashfile_path} update string is empty..."
            gk.log_email_exit(logger.info, 0, 0, msg=msg)

    ###################################################################################################################
    # Step 12)
    # Update master.hashes for the yyyymm.hashes file(s) modified in Step 11) above.

    # Logic of method to update master hashes:
    # 1) get master hash from mirror
    # 2) read master hash into a dictionary
    # 3) if updated ym in master hash, replace hash
    # 4) if new ym, add to master hash
    # 5) upload master hash to mirror

    # Get master hashes file
    logger.info("Getting master hashes file...")
    gk.get_master_hashes()
    if not gk.wait_for_last_task():
        sub = "get_master_hashes timeout. Master hashes not updated... Exiting."
        gk.log_email_exit(logger.error, 1, 1, sub=sub)

    # Read master hashes file in as dictionary with filenames as keys and hashes as values
    # "Filenames" are of the form ./raw/yyyymm.hashes and ./dat/yyyymm.hashes
    hashes = {}
    with open(f"{gk.get_working_dir()}/master.hashes", 'r') as master_file:
        for line in master_file:
            (val, key) = line.split()
            hashes[key] = val

    # For each yyyymm in holding dir which passed all tests
    #    - hash the corresponding yyyymm.hashes
    #    - update/append the key, value pair to the hashes dictionary
    for ym in yearmonth:
        raw_hash_dir = f"{gk.get_working_dir()}/raw"
        if not isdir(raw_hash_dir):
            mkdir(raw_hash_dir)
        # Move hash file to working_dir/raw/ to ensure entry in master hash of the form ./raw/yyyymm.hashes
        logger.info(f"Moving {ym}.hashes to {raw_hash_dir}\n")
        rename(f"{gk.get_working_dir()}/{ym}.hashes",
               f"{raw_hash_dir}/{ym}.hashes")
        # From working_dir, hash yyyymm.hashes file in working_dir/raw/
        hash_process = subprocess.Popen(f"cd {gk.get_working_dir()}; sha1sum ./raw/{ym}.hashes",
                                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        hash_process_out, hash_process_err = hash_process.communicate()
        hash_process_output = hash_process_out.decode().split("\n")

        # Add yyyymm.hashes to dictionary if it doesn't exist, update existing hash o/w.
        hashes[f"./raw/{ym}.hashes"] = hash_process_output[0].split()[0]

    # Overwrite entire master.hashes file with dictionary
    with open(f"{gk.get_working_dir()}/master.hashes", 'w') as master_file:
        for key in sorted(list(hashes.keys())):
            master_file.write(f"{hashes[key]}  {key}\n")

    # Upload master hash to mirror
    logger.info("Updating master hashes")
    try:
        gk.put_master_hashes()
        if not gk.wait_for_last_task():
            msg = "Updating of master hashes didn't complete."
            gk.log_email_exit(logger.warning, 1, 0, msg=msg)
    except globus_sdk.GlobusError as error:
        msg = f"Updating of master hashes didn't complete. {error}"
        gk.log_email_exit(logger.error, 1, 0, msg=msg)
    except Exception as error:
        msg = f"Updating master hashes failed. {error}"
        gk.log_email_exit(logger.error, 1, 0, msg=msg)

    finish_time_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    finish_time = datetime.now().strftime("%s")

    logger.info(f"Finished at: {finish_time_utc}")
    total_time = (int(finish_time) - int(start_time))/60
    logger.info(f"Script finished. Total time: {total_time} minutes")


if __name__ == "__main__":
    main()


# TODO: go through all "print" statements and either remove them or add them to logger
