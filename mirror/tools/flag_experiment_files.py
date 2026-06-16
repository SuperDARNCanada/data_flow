import pydarnio
import os
import argparse
"""
Script to check for and move local special experiment files to a subdirectory. Main usage is to move files out of the
holding directory when special experiment files are not flagged earlier in the data flow chain

Usage:
Call check_files(filepath) for a given path to see the list of files (if any) that are from special experiments.
Then, call move_files(filepath) to move those files to a subdirectory.
This could probably be done in a better way where the output of check_files() could be used for the move_files()
    function to avoid redundancy. This has been done this way to allow for a manual check of the file list before
    moving the flagged files to the subdirectory.

Normal CPIDs:
    151  -> Normalscan
    157  -> Normalsound
    191  -> Interleavesound
    3503 -> Twofsound
"""


# Function to find and move files with abnormal CPIDs at a given path to a subdirectory
def move_files(filepath):
    normal_cpid = (151, 157, 191, 3503)
    file_path = filepath
    subdir = f"{file_path}/special_experiments/"
    for f in sorted(os.listdir(file_path)):
        if os.path.isfile(f"{file_path}/{f}"):
            rec = pydarnio.read_rawacf(f"{file_path}/{f}", mode="sniff")
            if rec['cp'] not in normal_cpid:
                print(f"Not normal op! cpid = {rec['cp']}. Moving file {f}")
                os.rename(f"{file_path}/{f}", f"{subdir}/{f}")
            else:
                print(f"Normal op! cpid = {rec['cp']}. {f}")


# Function to find and log files with abnormal CPIDs at a given path
def check_files(filepath):
    normal_cpid = (151, 157, 191, 3503)
    file_path = filepath
    file_list = []
    for f in sorted(os.listdir(file_path)):
        if os.path.isfile(f"{file_path}/{f}"):
            rec = pydarnio.read_rawacf(f"{file_path}/{f}", mode="sniff")
            if rec['cp'] not in normal_cpid:
                print(f"Not normal op! cpid = {rec['cp']}. Moving file {f}")
                file_list.append(f)
            else:
                print(f"Normal op! cpid = {rec['cp']}. {f}")

    print(file_list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", help="Directory to scan for special experiment files", default='')
    args = parser.parse_args()
    filepath = args.path

    check_files(filepath)
    # move_files(filepath)
