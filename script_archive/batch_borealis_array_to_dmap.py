# Copyright 2021 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller

import argparse
import sys
import os
import copy
import glob
import subprocess as sp
import numpy as np
import warnings
import tables
from multiprocessing import Process
import deepdish as dd

from borealis_array_to_dmap import array_to_dmap

def usage_msg():
    """
    Return the usage message for this process.

    This is used if a -h flag or invalid arguments are provided.

    :returns: the usage message
    """

    usage_message = """ batch_borealis_array_to_dmap.py [-h] output_data_dir path_regex

    **** NOT TO BE USED IN PRODUCTION ****
    **** USE WITH CAUTION ****

    Batch dmap borealis files."""

    return usage_message


def script_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("output_data_dir", nargs=1, help="Path to place the dmap file in.")
    parser.add_argument("path_regex", nargs='+', help="Path regex you want to match. Will"
        " find the files that match to modify. Alternatively, list files separately and "
        " all listed will be processed.")
    return parser


if __name__ == "__main__":
    parser = script_parser()
    args = parser.parse_args()

    files_to_update = args.path_regex # should be a list

    jobs = []

    files_left = True
    filename_index = 0
    num_processes = 4

    output_data_dir = args.output_data_dir[0] # only 1

    while files_left:
        for procnum in range(num_processes):
            try:
                filename = files_to_update[filename_index + procnum]
                print('Dmapping: ' + filename)
            except IndexError:
                if filename_index + procnum == 0:
                    print('No files found!')
                    raise
                files_left = False
                break
            p = Process(target=array_to_dmap, args=(filename, output_data_dir))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        filename_index += num_processes
