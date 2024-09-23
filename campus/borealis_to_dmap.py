# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller
"""
Usage:

borealis_to_dmap.py [-h] borealis_file

Pass in the filename you wish to convert (should end in '.h5').

The script will convert the records to a dmap dictionary and then write to file as the given filename, with extensions
'.[borealis_filetype].h5' replaced with [dmap_filetype].bz2.

Requires pydarnio v2
"""

import argparse
import datetime
import os
import sys

import pydarnio


def usage_msg():
    """
    Return the usage message for this process.
     
    This is used if a -h flag or invalid arguments are provided.
     
    :returns: the usage message
    """

    usage_message = """ borealis_to_dmap.py [-h] borealis_file

    Pass in the filename you wish to convert (should end in '.h5'). 

    The script will convert the records to a dmap dictionary and then 
    write to file as the given filename, with extensions 
    '.[borealis_filetype].h5' replaced with '.[dmap_filetype].bz2'.
    """

    return usage_message


def borealis_conversion_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("borealis_file", help="Path to the borealis file that you wish to convert. "
                                                    "(e.g. 20190327.2210.38.sas.0.bfiq.h5)")
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
            raise pydarnio.borealis_exceptions.BorealisConvert2RawacfError(errmsg)
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


def convert_borealis_to_dmap(filename, borealis_filetype, slice_id, dmap_filename):
    """
    Takes a Borealis file and writes the converted DMAP file to the same directory as the input array file.
    """
    pydarnio.BorealisConvert(filename, borealis_filetype, dmap_filename, slice_id)


def borealis_to_dmap(borealis_file):
    borealis_filetype = borealis_file.split('.')[-2]  # XXX.h5
    slice_id = int(borealis_file.split('.')[-3])  # X.rawacf.h5
    
    dmap_filetypes = {'rawacf': 'rawacf', 'bfiq': 'iqdat'}

    if borealis_filetype in dmap_filetypes.keys():
        dmap_filename = create_dmap_filename(borealis_file)
        convert_borealis_to_dmap(borealis_file, borealis_filetype, slice_id, dmap_filename)

        print(f'Wrote dmap to : {dmap_filename}')

    else:
        print(f'Cannot convert file {borealis_file} from Borealis filetype {borealis_filetype}')
        sys.exit(1)


def main():
    parser = borealis_conversion_parser()
    args = parser.parse_args()

    start_time = datetime.datetime.utcnow()
    borealis_to_dmap(args.borealis_file)

    end_time = datetime.datetime.utcnow()
    print(f"Conversion time: {(end_time-start_time).total_seconds():.2f} seconds") 


if __name__ == "__main__":
    main()
