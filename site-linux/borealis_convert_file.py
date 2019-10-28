# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller
"""
Usage:

borealis_converter.py [-h] borealis_site_file

Pass in the filename you wish to convert (should end in '.hdf5.site' 
('.bz2' optional)). The script will decompress if a bzipped hdf5 site 
file with 'bz2' extension is provided. 

The script will :
1. convert the records to an array style file, writing the file as 
    the borealis_site_file with the last extension (should be '.site') 
    removed.
2. convert the records to a dmap dictionary and then write to file as 
    the given filename, with extensions '.[borealis_filetype].hdf5.site' 
    replaced with [dmap_filetype].dmap. The script will also bzip the 
    resulting dmap file.

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

    usage_message = """ borealis_converter.py [-h] borealis_site_file 
    
    Pass in the filename you wish to convert (should end in '.hdf5.site' ('.bz2' optional)). 
    The script will decompress if a bzipped hdf5 site file with 'bz2' extension is provided. 

    The script will :
    1. convert the records to an array style file, writing the file as the borealis_site_file
       with the last extension (should be '.site') removed.
    2. convert the records to a dmap dictionary and then write to file as the given filename,
       with extensions '.[borealis_filetype].hdf5.site' replaced with [dmap_filetype].dmap. 
       The script will also bzip the resulting dmap file. """

    return usage_message


def borealis_conversion_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("borealis_site_file", help="Path to the site file that you wish to "
                                                    "convert. "
                                                    "(e.g. 20190327.2210.38.sas.0.bfiq.hdf5.site)")

    return parser


def create_dmap_filename(filename_to_convert, dmap_filetype):
    """
    Creates a dmap filename in the same directory as the source .hdf5 file, 
    to write the DARN dmap file to.
    """
    basename = os.path.basename(filename_to_convert)
    basename_without_ext = '.'.join(basename.split('.')[0:-3]) # all but .rawacf.hdf5.site, for example.
    dmap_filename = os.path.dirname(filename_to_convert) + '/' + basename_without_ext + '.' + dmap_filetype + '.dmap'
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


def borealis_site_to_array_and_dmap_files(filename, borealis_filetype, slice_id,
                       dmap_filename, array_filename):
    """
    Takes a Borealis site structured file, and writes both the SDARN converted
    file and an array restructured file to the same directory as the input 
    site file.

    Returns
    -------
    array_filename
        array restructured filename (zlib compressed)
    bz2_filename
        bzipped dmap filename
    """
    borealis_converter = BorealisConvert(filename, borealis_filetype,
                            dmap_filename, slice_id, borealis_file_structure='site')

    dmap_filename = borealis_converter.sdarn_filename # overwrite to as generated

    bz2_filename = compress_bz2(dmap_filename) # compress (and adds .bz2 to filename)
    os.remove(dmap_filename) # remove uncompressed

    arrays = borealis_converter.arrays # restructures the input records to arrays
    borealis_writer = BorealisWrite(array_filename, arrays, borealis_filetype,
                                    borealis_file_structure='array')

    array_filename = borealis_writer.filename # overwrite to as generated
    return array_filename, bz2_filename


def borealis_site_to_array_file(filename, borealis_filetype, array_filename):
    """
    Takes a Borealis site structured file and writes an array restructured file 
    to the same directory as the input site file.

    Returns
    -------
    array_filename
        array restructured filename (zlib compressed)
    """
    borealis_reader = BorealisRead(filename, borealis_filetype,
                                   borealis_file_structure='site')
    arrays = borealis_reader.arrays # restructures the input records to arrays
    borealis_writer = BorealisWrite(array_filename, arrays, borealis_filetype,
                                    borealis_file_structure='array')

    array_filename = borealis_writer.filename # overwrite to as generated
    return array_filename


def main():
    parser = borealis_conversion_parser()
    args = parser.parse_args()

    time_now = datetime.datetime.utcnow().strftime('%Y%m%d %H:%M:%S')
    sys_call = ' '.join(sys.argv[:])
    print('')
    print(time_now)
    print(sys_call)

    # Check if the file is bz2, decompress if necessary
    if os.path.basename(args.borealis_site_file).split('.')[-1] in ['bz2', 'bzip2']:
        borealis_site_file = decompress_bz2(args.borealis_site_file)
        __bzip2 = True
    else:
        borealis_site_file = args.borealis_site_file
        __bzip2 = False

    # .bz2, if at end of filename, was removed in the decompression.
    borealis_filetype = borealis_site_file.split('.')[-3] # XXX.hdf5.site
    slice_id = int(borealis_site_file.split('.')[-4]) # X.rawacf.hdf5.site
    array_filename = '.'.join(borealis_site_file.split('.')[0:-1]) # all but .site  
    
    dmap_filetypes = {'rawacf': 'rawacf', 'bfiq': 'iqdat'}

    if borealis_filetype in dmap_filetypes.keys():
        # for 'rawacf' and 'bfiq' types, we can convert to arrays and to dmap.
        # Most efficient way to do this is to only read once and write 
        # using the arrays from the BorealisConvert class.
        dmap_filetype = dmap_filetypes[borealis_filetype]
        dmap_filename = create_dmap_filename(borealis_site_file, borealis_filetype)

        written_array_filename, written_dmap_filename = \
            borealis_site_to_array_and_dmap_files(borealis_site_file, 
                                    borealis_filetype, slice_id, 
                                    dmap_filename, array_filename)

        print('Wrote dmap to : {}'.format(written_dmap_filename))
        print('Wrote array to : {}'.format(written_array_filename))

    elif borealis_filetype == 'antennas_iq':
        # we can only convert this type to arrays.
        
        written_array_filename = borealis_site_to_array_file(borealis_site_file, 
                                                        borealis_filetype,
                                                        array_filename)
        print('Wrote array to : {}'.format(written_array_filename))

    else:
        print('Cannot convert file {} from Borealis filetype '
            '{}'.format(borealis_site_file, borealis_filetype))
        sys.exit(1)

    if __bzip2:
        # remove the decompressed site file from the directory because it was 
        # generated.
        os.remove(borealis_site_file)

if __name__ == "__main__":
    main()