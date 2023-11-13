# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller
"""
Usage:

borealis_array_to_dmap.py [-h] borealis_array_file

Pass in the filename you wish to convert (should end in '.hdf5'). 

The script will convert the records to a dmap dictionary and then 
write to file as the given filename, with extensions 
'.[borealis_filetype].hdf5' replaced with [dmap_filetype].dmap. 
The script will also bzip the resulting dmap file.

Requires pydarnio v1.0.

"""

import argparse
import bz2
import datetime
import os
import sys

from pydarnio import BorealisConvert


def usage_msg():
    """
    Return the usage message for this process.
     
    This is used if a -h flag or invalid arguments are provided.
     
    :returns: the usage message
    """

    usage_message = """ borealis_array_to_dmap.py [-h] borealis_array_file

    Pass in the filename you wish to convert (should end in '.hdf5'). 

    The script will convert the records to a dmap dictionary and then 
    write to file as the given filename, with extensions 
    '.[borealis_filetype].hdf5' replaced with '.[dmap_filetype]'. 
    The script will also bzip the resulting dmap file. 
    """

    return usage_message


def borealis_conversion_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("borealis_array_file", help="Path to the array file that you wish to "
                                                    "convert. "
                                                    "(e.g. 20190327.2210.38.sas.0.bfiq.hdf5)")
    parser.add_argument("--scaling_factor", help="Scale to multiply by in conversion, default = 1")
    return parser


def create_dmap_filename(filename_to_convert, dmap_filetype, output_data_dir):
    """
    Creates a dmap filename in the same directory as the source .hdf5 file, 
    to write the DARN dmap file to.
    """
    basename = os.path.basename(filename_to_convert)

    slice_id = int(basename.split('.')[-3]) # X.rawacf.hdf5
    ordinal = slice_id + 97

    if ordinal not in range(97, 123):
        # we are not in a-z
        errmsg = f"Cannot convert slice ID {slice_id} to channel identifier " \
                 "because it is outside range 0-25 (a-z)."
        if dmap_filetype == 'iqdat':
            raise BorealisConvert2IqdatError(errmsg)
        elif dmap_filetype == 'rawacf':
            raise BorealisConvert2RawacfError(errmsg)
        else:
            raise Exception(errmsg)

    file_channel_id = chr(ordinal)

    # e.g. remove .rawacf.hdf5.site, sub file_channel_id for slice_id, add dmap_filetype extension.
    dmap_basename = '.'.join(basename.split('.')[0:-3] + [file_channel_id, dmap_filetype]) 
    dmap_filename = output_data_dir + '/' + dmap_basename
    return dmap_filename


def decompress_bz2(filename):
    """
    Decompress a file and return the new file's name without .bz2. 
    Reads and writes 100 kB at a time.
    """
    basename = os.path.basename(filename) 
    newfilepath = os.path.dirname(filename) + '/' + '.'.join(basename.split('.')[0:-1]) # all but bz2

    with open(newfilepath, 'wb') as new_file, bz2.BZ2File(filename, 'rb') as bz2_file:
        for data in iter(lambda : bz2_file.read(100 * 1024), b''):
            new_file.write(data)    

    return newfilepath


def compress_bz2(filename):
    """
    Compress a file and return the new file's name with .bz2. 
    Reads and writes 100 kB at a time.
    """
    bz2_filename = filename + '.bz2'

    with open(filename, 'rb') as og_file, bz2.BZ2File(bz2_filename, 'wb') as bz2_file:
        for data in iter(lambda : og_file.read(100 * 1024), b''):
            bz2_file.write(data)   

    return bz2_filename


def borealis_array_to_dmap_files(filename, borealis_filetype, slice_id,
                       dmap_filename, scaling_factor):
    """
    Takes a Borealis array structured file, and writes the SDARN converted
    file to the same directory as the input array file.

    Returns
    -------
    bz2_filename
        bzipped dmap filename
    """
    borealis_converter = BorealisConvert(filename, borealis_filetype,
                            dmap_filename, slice_id, borealis_file_structure='array',
                            scaling_factor=scaling_factor)

    dmap_filename = borealis_converter.sdarn_filename # overwrite to as generated

    bz2_filename = compress_bz2(dmap_filename) # compress (and adds .bz2 to filename)
    os.remove(dmap_filename) # remove uncompressed
    del borealis_converter # mem savings
    return bz2_filename


def array_to_dmap(borealis_array_file, output_data_dir, scaling_factor=1):
    borealis_filetype = borealis_array_file.split('.')[-2] # XXX.hdf5
    slice_id = int(borealis_array_file.split('.')[-3]) # X.rawacf.hdf5
    
    dmap_filetypes = {'rawacf': 'rawacf', 'bfiq': 'iqdat'}

    if borealis_filetype in dmap_filetypes.keys():
        # for 'rawacf' and 'bfiq' types, we can convert to arrays and to dmap.
        # Most efficient way to do this is to only read once and write 
        # using the arrays from the BorealisConvert class.
        dmap_filetype = dmap_filetypes[borealis_filetype]
        dmap_filename = create_dmap_filename(borealis_array_file, 
                                             dmap_filetype, output_data_dir)

        written_dmap_filename = \
            borealis_array_to_dmap_files(borealis_array_file, 
                                    borealis_filetype, slice_id, 
                                    dmap_filename, scaling_factor)

        print(f'Wrote dmap to : {written_dmap_filename}')

    else:
        print(f'Cannot convert file {borealis_array_file} from Borealis filetype '
                f'{borealis_filetype}')
        sys.exit(1)


def main():
    parser = borealis_conversion_parser()
    args = parser.parse_args()

    start_time = datetime.datetime.utcnow()

    borealis_array_file = args.borealis_array_file

    if args.scaling_factor:
        scaling_factor = int(args.scaling_factor)
    else:
        scaling_factor = 1

    array_to_dmap(borealis_array_file, os.path.dirname(borealis_array_file), 
                  scaling_factor)

    end_time = datetime.datetime.utcnow()
    print(f"Conversion time: {(end_time-start_time).total_seconds():.2f} seconds") 


if __name__ == "__main__":
    main()
