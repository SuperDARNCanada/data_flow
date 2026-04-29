#!/usr/bin/env python
# coding: utf-8
"""
Last modification 201706 by Kevin Krieger

This script is designed to log on to the University of Saskatchewan globus
SuperDARN mirror in order to check for and remove files given a list of files

Example of script call:
python delete_files_globus.py -t 'raw' -r 'chroot/sddata/' -d 'local_data/deletions/'
        -l '/home/dataman/logs/deletions_globus/' /home/dataman/mirror_blocklists/cve/${year}_cve_files_to_delete.txt

See 'Removing Blocked Files from the Mirror' subsection of Data Flow section of SDARN wiki for more info
"""

from gatekeeper_class import Gatekeeper
from os.path import expanduser, isfile, isdir
import argparse
import sys
from datetime import datetime

HOME = expanduser("~")
TRANSFER_RT_FILENAME = HOME + "/.globus_transfer_rt"
CLIENT_ID = 'e70228d0-56a2-4d85-bf63-7fbccc92dcd3'
data_types = ['raw', 'dat', 'fit', 'map', 'grid', 'summary']

if __name__ == '__main__':
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
    parser.add_argument("-l", "--logging_directory",
                        help="Directory to store log files",
                        default=HOME+"/logs/deletions_globus/")
    args = parser.parse_args()
    file_list = args.file_list
    data_type = args.data_type
    mirror_root = args.mirror_root
    deletions_directory = args.deletions_directory
    log_directory = args.logging_directory

    if not isdir(log_directory):
        sys.exit("Logging directory {} doesn't exist.".format(log_directory))

    log_file_name = "{}/{}".format(log_directory, cur_date)
    logfile = open(log_file_name, 'a')
    logfile.write(cur_date+"\n")

    # Open the transfer refresh token file if it exists
    if isfile(TRANSFER_RT_FILENAME):
        with open(TRANSFER_RT_FILENAME) as f:
            gk = Gatekeeper(CLIENT_ID, transfer_rt=f.readline())
    else:
        gk = Gatekeeper(CLIENT_ID)

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
        logfile.write("Get hashes all didn't complete in time. Exiting\n")
        sys.exit("Get hashes all didn't complete in time.")

    # Now for each file to remove from the mirror, go through hashes files and find it, remove
    # the line and put back all updated hashes files
    updated_hashes = []
    files_not_found = []
    for file_to_delete in files_to_delete:
        year = file_to_delete[0:4]
        month = file_to_delete[4:6]
        logfile.write("{}: year: {} month: {}\n".format(file_to_delete, year, month))
        try:
            with open("{}/{}{}.hashes".format(gk.get_working_dir(), year, month)) as hashfile:
                files_list = hashfile.readlines()
        except IOError:
            # Just exit if we didn't find the hash file, that's a problem requiring human insight
            logfile.write("Could not open {}{}.hashes, does it exist? Exiting\n".format(year, month))
            sys.exit(1)

        found = False
        for f in files_list:
            if file_to_delete in f:
                # it exists. Remove it from the list
                found = True
                files_list = [x for x in files_list if f not in x]
                logfile.write("Removed {} from {}{}.hashes\n".format(f.strip(), year, month))
                updated_hashes.append("{}{}.hashes".format(year, month))
                with open("{}/{}{}.hashes".format(gk.get_working_dir(),
                                                  year, month), 'w') as hashfile:
                    hashfile.writelines(files_list)
                break
        if not found:
            files_not_found.append(file_to_delete)
            logfile.write("{} DNE in {}{}.hashes for data type {}\n".format(file_to_delete,
                                                                            year, month,
                                                                            data_type))
            files_to_delete = [x for x in files_to_delete if x != file_to_delete]

    logfile.write("Files to delete:\n")
    logfile.writelines(files_to_delete)
    logfile.write("\nFiles not found:\n")
    logfile.writelines(files_not_found)
    updated_hashes = list(set(updated_hashes))
    logfile.write("Updated hashes files: {}\n".format(updated_hashes))

    # Now that we have files to delete and updated_hashes files, upload the new hashes and then
    # remove the files, making sure both succeed
    for updated_hash_file in updated_hashes:
        year = updated_hash_file[0:4]
        month = updated_hash_file[4:6]
        gk.put_hashes(year, month, data_type)
        while not gk.wait_for_last_task():
            logfile.write("Still waiting for {}{}.hashes to upload...\n".format(year, month))
            continue

    if len(files_to_delete) > 0:
        gk.move_files_on_endpoint(files_to_delete,
                                  "{}/{}/".format(deletions_directory, cur_date),
                                  data_type=data_type)
    files_to_delete = []
    for f in files_not_found:
        year = f[0:4]
        month = f[4:6]
        if gk.check_for_file_existence("{}/{}/{}/{}/{}".format(gk.get_mirror_root_dir(),
                                                               data_type, year, month,
                                                               f.strip('\n'))):
            logfile.write("{} on mirror but not in hashes file! Removing\n".format(f.strip('\n')))
            files_to_delete.append(f)

    logfile.write("Files not found in hashes but still on mirror:\n")
    logfile.writelines(files_to_delete)
    if len(files_to_delete) > 0:
        gk.move_files_on_endpoint(files_to_delete,
                                  "{}/{}".format(deletions_directory, cur_date),
                                  data_type=data_type)
