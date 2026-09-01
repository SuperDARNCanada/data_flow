#!/usr/bin/env python
# coding: utf-8
"""
Last modification 201706 by Kevin Krieger

This script is designed to log on to the University of Saskatchewan globus
SuperDARN mirror in order to check for and remove files given a list of files

Example of script call:
python /path/to/delete_files_globus.py -t 'raw' -r 'chroot/sddata/' -d 'local_data/deletions/'
        -l '~/logs/deletions_globus/' ~/mirror_blocklists/cve/${year}_cve_files_to_delete.txt

See 'Removing Blocked Files from the Mirror' subsection of Data Flow section of SDARN wiki for more info
"""

import globus_sdk
from gatekeeper_class import Gatekeeper, sha1hashing
from os.path import expanduser, isfile, isdir
from os import mkdir, rename
import argparse
import sys
from datetime import datetime

HOME = expanduser("~")
TRANSFER_RT_FILENAME = f"{HOME}/.globus_transfer_rt"
GATEKEEPER_APP_FILENAME = f"{HOME}/mirror_id_files/gatekeeper_app_id.txt"

# Client ID retrieved from https://auth.globus.org/v2/web/developers
if isfile(GATEKEEPER_APP_FILENAME):
    with open(GATEKEEPER_APP_FILENAME) as f:
        file = f.readlines()
    for line in file:
        if "CM app" in line:
            CLIENT_ID = line.split("=")[1].split()[0]

data_types = ['raw', 'dat', 'fit', 'map', 'grid', 'summary']

if __name__ == '__main__':
    start_time = datetime.now().strftime("%s")
    cur_date = datetime.now().strftime("%Y%m%d.%H%M")

    parser = argparse.ArgumentParser()
    parser.add_argument("file_list",
                        help="List of files to delete from the mirror, one per line")
    parser.add_argument("-t", "--data_type",
                        help="One of {} Default: 'raw'".format(data_types),
                        default='raw')
    parser.add_argument("-r", "--mirror_root", help="Mirror root directory",
                        default="~/test_mirror")
    parser.add_argument("-d", "--deletions_directory",
                        help="Directory on endpoint to store deleted files",
                        default="~/test_mirror/test_deletions")
    args = parser.parse_args()
    file_list = args.file_list
    data_type = args.data_type
    mirror_root = args.mirror_root
    deletions_directory = args.deletions_directory

    # Open the transfer refresh token file if it exists
    if isfile(TRANSFER_RT_FILENAME):
        with open(TRANSFER_RT_FILENAME) as f:
            gk = Gatekeeper(CLIENT_ID, transfer_rt=f.readline(), mode='block')
    else:
        gk = Gatekeeper(CLIENT_ID, mode='block')

    logger = gk.logger
    logger.info(f"{cur_date}")

    gk.set_mirror_root_dir(mirror_root)

    # Get the files to delete into a clean python list 
    # (get rid of newlines, whitespaces, incorrect datatype lines)
    with open(file_list) as f:
        files_to_delete = f.readlines()
    files_to_delete = [x.strip() for x in files_to_delete]
    files_to_delete = [x for x in files_to_delete if data_type in x]

    # Download hashes files
    gk.get_hashes_all(data_type=data_type)
    if not gk.wait_for_last_task(timeout_s=600):
        sub = "Get hashes all didn't complete in time. Exiting"
        gk.log_email_exit(logger.error, 1, 1, sub=sub)

    # Now for each file to remove from the mirror, go through hashes files and find it, remove
    # the line and put back all updated hashes files
    updated_hashes = []
    files_not_found = []
    yearmonth = []
    for file_to_delete in files_to_delete:
        year = file_to_delete[0:4]
        month = file_to_delete[4:6]
        logger.info(f"{file_to_delete}: year: {year} month: {month}")
        try:
            with open(f"{gk.get_working_dir()}/{year}{month}.hashes") as hashfile:
                files_list = hashfile.readlines()
        except IOError:
            # Just exit if we didn't find the hash file, that's a problem requiring human insight
            sub = "Could not open {year}{month}.hashes, does it exist? Exiting"
            gk.log_email_exit(logger.error, 1, 1, sub=sub)

        found = False
        for f in files_list:
            if file_to_delete in f:
                # it exists. Remove it from the list
                found = True
                files_list = [x for x in files_list if f not in x]
                logger.info(f"Removed {f.strip()} from {year}{month}.hashes")
                updated_hashes.append(f"{year}{month}.hashes")
                yearmonth.append(f"{year}{month}")
                with open(f"{gk.get_working_dir()}/{year}{month}.hashes", 'w') as hashfile:
                    hashfile.writelines(files_list)
                break
        if not found:
            files_not_found.append(file_to_delete)
            logger.info(f"{file_to_delete} DNE in {year}{month}.hashes for data type {data_type}")
            files_to_delete = [x for x in files_to_delete if x != file_to_delete]

    logger.info(f"Files to delete ({len(files_to_delete)}):\n{files_to_delete}")
    logger.info(f"Files not found ({len(files_not_found)}):\n{files_not_found}")
    updated_hashes = sorted(list(set(updated_hashes)))
    yearmonth = sorted(list(set(yearmonth)))
    logger.info(f"Updated hashes files: {updated_hashes}")
    logger.info(f"Updated yearmonths: {yearmonth}")

    # Now that we have files to delete and updated_hashes files, upload the new hashes and then
    # remove the files, making sure both succeed
    for updated_hash_file in updated_hashes:
        year = updated_hash_file[0:4]
        month = updated_hash_file[4:6]
        gk.put_hashes(year, month, data_type)
        while not gk.wait_for_last_task():
            logger.info(f"Still waiting for {year}{month}.hashes to upload...")
            continue

    if len(files_to_delete) > 0:
        gk.move_files_on_endpoint(files_to_delete, f"{deletions_directory}/{cur_date}/", data_type=data_type)
    files_to_delete = []
    for f in files_not_found:
        year = f[0:4]
        month = f[4:6]
        if gk.check_for_file_existence(f"{gk.get_mirror_root_dir()}/{data_type}/{year}/{month}/{f}"):
            logger.info(f"{f} on mirror but not in hashes file! Removing")
            files_to_delete.append(f)

    logger.info(f"Files not found in hashes but still on mirror ({len(files_to_delete)}):\n{files_to_delete}")
    if len(files_to_delete) > 0:
        gk.move_files_on_endpoint(files_to_delete, f"{deletions_directory}/{cur_date}", data_type=data_type)

    # Update master.hashes for the yyyymm.hashes modified above
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
        data_hash = sha1hashing(gk.get_working_dir(), f"./raw/{ym}.hashes")
        # Add yyyymm.hashes to dictionary if it doesn't exist, update existing hash o/w.
        hashes[f"./raw/{ym}.hashes"] = data_hash

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

    finish_time = datetime.now().strftime("%s")

    total_time = (int(finish_time) - int(start_time)) / 60
    logger.info(f"Script finished. Total time: {total_time} minutes")
