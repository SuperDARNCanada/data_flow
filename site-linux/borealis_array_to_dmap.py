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

"""

import argparse
import bz2
import datetime
import os
import sys

from pydarn import BorealisRead, BorealisWrite, BorealisConvert


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
    '.[borealis_filetype].hdf5' replaced with [dmap_filetype].dmap. 
    The script will also bzip the resulting dmap file. 
    """

    return usage_message


def borealis_conversion_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("borealis_array_file", help="Path to the array file that you wish to "
                                                    "convert. "
                                                    "(e.g. 20190327.2210.38.sas.0.bfiq.hdf5)")

    return parser


def create_dmap_filename(filename_to_convert, dmap_filetype):
    """
    Creates a dmap filename in the same directory as the source .hdf5 file, 
    to write the DARN dmap file to.
    """
    basename = os.path.basename(filename_to_convert)
    basename_without_ext = '.'.join(basename.split('.')[0:-2]) # all but .rawacf.hdf5, for example.
    dmap_filename = os.path.dirname(filename_to_convert) + '/' + basename_without_ext + '.' + dmap_filetype 
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
                       dmap_filename):
    """
    Takes a Borealis array structured file, and writes the SDARN converted
    file to the same directory as the input array file.

    Returns
    -------
    bz2_filename
        bzipped dmap filename
    """
    borealis_converter = BorealisConvert(filename, borealis_filetype,
                            dmap_filename, slice_id, borealis_file_structure='array')

    dmap_filename = borealis_converter.sdarn_filename # overwrite to as generated

    bz2_filename = compress_bz2(dmap_filename) # compress (and adds .bz2 to filename)
    os.remove(dmap_filename) # remove uncompressed

    return bz2_filename


def main():
    parser = borealis_conversion_parser()
    args = parser.parse_args()

    time_now = datetime.datetime.utcnow().strftime('%Y%m%d %H:%M:%S')
    sys_call = ' '.join(sys.argv[:])
    print(time_now)
    print(sys_call)

    borealis_array_file = args.borealis_array_file
    borealis_filetype = borealis_array_file.split('.')[-2] # XXX.hdf5
    slice_id = int(borealis_array_file.split('.')[-3]) # X.rawacf.hdf5
    
    dmap_filetypes = {'rawacf': 'rawacf', 'bfiq': 'iqdat'}

    if borealis_filetype in dmap_filetypes.keys():
        # for 'rawacf' and 'bfiq' types, we can convert to arrays and to dmap.
        # Most efficient way to do this is to only read once and write 
        # using the arrays from the BorealisConvert class.
        dmap_filetype = dmap_filetypes[borealis_filetype]
        dmap_filename = create_dmap_filename(borealis_array_file, dmap_filetype)

        written_dmap_filename = \
            borealis_array_to_dmap_files(borealis_array_file, 
                                    borealis_filetype, slice_id, 
                                    dmap_filename)

        print('Wrote dmap to : {}'.format(written_dmap_filename))

    else:
        print('Cannot convert file {} from Borealis filetype '
            '{}'.format(borealis_array_file, borealis_filetype))
        sys.exit(1)

if __name__ == "__main__":
    main()
