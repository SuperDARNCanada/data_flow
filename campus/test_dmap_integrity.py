# Copyright 2023 SuperDARN Canada, University of Saskatchewan
# Author: Remington Rohel
"""
Usage:

test_dmap_integrity.py [-h] dmap_file

Pass in the rawacf filename you wish to check.

The script will try to read the file with pyDARNio to verify that its contents
conform with the DMAP file format.

Requires pydarnio v1.0.

"""

import argparse
import sys

from pydarnio import SDarnRead

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help='Path to DMAP rawacf file', type=str)
    args = parser.parse_args()

    # try:
    dmap_stream = open(args.infile, 'rb').read()
    reader = SDarnRead(dmap_stream, True)
    records = reader.read_rawacf()
    # except:
        # sys.exit(1)    # If the file could not be interpreted correctly, return 1
    # else:
        # sys.exit(0)     # If the file was correctly read, return 0
