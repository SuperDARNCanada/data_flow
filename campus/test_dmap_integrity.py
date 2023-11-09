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

from pydarnio import SDarnRead

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help='Path to DMAP rawacf file', type=str)
    args = parser.parse_args()

    dmap_stream = open(args.infile, 'rb').read()
    reader = SDarnRead(dmap_stream, True)
    records = reader.read_rawacf()

    # If the file couldn't be read correctly, 1 is returned
    # Otherwise if no errors are thrown, 0 is returned
