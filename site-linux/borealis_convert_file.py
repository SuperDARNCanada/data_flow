# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller
"""
Usage:

borealis_converter.py [-h] rawacf_file

Pass in the filename you wish to convert (should end in '.h5'). The script will convert the records to DMAP format,
compression with bzip2.
"""

import argparse
import datetime
import os
import sys

from pydarnio import BorealisConvert
from pydarnio.exceptions.borealis_exceptions import BorealisConvert2RawacfError


def usage_msg():
    """
    Return the usage message for this process.

    This is used if a -h flag or invalid arguments are provided.

    :returns: the usage message
    """

    usage_message = """ borealis_converter.py [-h] rawacf_file

    Pass in the filename you wish to convert (should end in '.h5').

    The script will convert the records to a DMAP style file, keeping the timestamp prefix of the filename.
    """

    return usage_message


def borealis_conversion_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("rawacf_file", help="Path to the rawacf HDF5 file that you wish to convert")

    return parser


def create_dmap_filename(filename_to_convert):
    """
    Creates a dmap filename in the same directory as the source HDF5 file, to write the DARN dmap file to.

    Filename provided should have slice ID at [-3] position in name, i.e. '...X.rawacf.h5'
    """
    basename = os.path.basename(filename_to_convert)
    slice_id = int(basename.split('.')[-3])  # X.rawacf.h5
    filetype = basename.split('.')[-2]  # X.rawacf.h5
    ordinal = slice_id + 97

    if filetype != "rawacf":
        raise ValueError(f"Unable to generate DMAP file from type {filetype}: {filename_to_convert}")

    if ordinal not in range(97, 123):
        # we are not in a-z
        errmsg = f'Cannot convert slice ID {slice_id} to channel identifier '\
                 'because it is outside range 0-25 (a-z).'
        if filetype == 'rawacf':
            raise BorealisConvert2RawacfError(errmsg)
        else:
            raise Exception(errmsg)
    file_channel_id = chr(ordinal)

    # e.g. turn `[timestamp].0.rawacf.h5` into `[timestamp].a.rawacf.bz2`
    dmap_basename = '.'.join(basename.split('.')[0:-3] + [file_channel_id, filetype, 'bz2'])
    containing_directory = os.path.dirname(filename_to_convert)
    if containing_directory == "":
        return dmap_basename
    else:
        return containing_directory + '/' + dmap_basename


def borealis_to_dmap_files(filename, borealis_filetype, slice_id, dmap_filename):
    """
    Takes a Borealis file, and writes the SDARN converted file to the same directory as the input file.
    """
    BorealisConvert(filename, borealis_filetype, dmap_filename, slice_id)


def main():
    parser = borealis_conversion_parser()
    args = parser.parse_args()

    start_time = datetime.datetime.utcnow()

    if not args.rawacf_file.endswith('.h5'):
        print(f"{args.rawacf_file} already in DMAP format")
        sys.exit(1)

    borealis_filetype = args.rawacf_file.split('.')[-2]  # XXX.h5
    if borealis_filetype != 'rawacf':
        print(f"Cannot convert file {args.rawacf_file} from Borealis filetype "
              f"{borealis_filetype}")
        sys.exit(1)

    slice_id = int(args.rawacf_file.split('.')[-3])  # X.rawacf.h5

    try:
        dmap_filename = create_dmap_filename(args.rawacf_file)
        borealis_to_dmap_files(args.rawacf_file, borealis_filetype, slice_id, dmap_filename)
        print(f'Wrote dmap to: {dmap_filename}')
        dmap_time = datetime.datetime.utcnow()
        print(f"Conversion time: {(dmap_time-start_time).total_seconds():.2f} seconds")
    except (BorealisConvert2RawacfError, Exception) as e:
        print(f"Unable to convert {args.rawacf_file} to DMAP file.")
        print(f"Due to error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
