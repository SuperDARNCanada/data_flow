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

from pydarnio import BorealisRead, BorealisWrite, BorealisConvert
from pydarnio.exceptions.borealis_exceptions import \
     BorealisConvert2RawacfError, BorealisConvert2IqdatError

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

    Filename provided should have slice ID at [-4] position in name, 
    ie '...X.rawacf.hdf5.site'
    """
    basename = os.path.basename(filename_to_convert)

    slice_id = int(basename.split('.')[-4]) # X.rawacf.hdf5.site
    ordinal = slice_id + 97

    if ordinal not in range(97, 123):
        # we are not in a-z
        errmsg = 'Cannot convert slice ID {} to channel identifier '\
                 'because it is outside range 0-25 (a-z).'.format(slice_id)
        if dmap_filetype == 'iqdat':
            raise BorealisConvert2IqdatError(errmsg)
        elif dmap_filetype == 'rawacf':
            raise BorealisConvert2RawacfError(errmsg)
        else:
            raise Exception(errmsg)

    file_channel_id = chr(ordinal)

    # e.g. remove .rawacf.hdf5.site, sub file_channel_id for slice_id, add dmap_filetype extension.
    dmap_basename = '.'.join(basename.split('.')[0:-4] + [file_channel_id, dmap_filetype])
    containing_directory = os.path.dirname(filename_to_convert)
    if containing_directory == "":
        return dmap_basename
    else:
        return containing_directory + '/' + dmap_basename


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

def borealis_array_to_dmap_files(filename, borealis_filetype, slice_id, dmap_filename):
    """
    Takes a Borealis array structured file, and writes the SDARN converted
    file to the same directory as the input site file.

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

def borealis_site_to_dmap_files(filename, borealis_filetype, slice_id, dmap_filename):
    """
    Takes a Borealis site structured file, and writes the SDARN converted
    file to the same directory as the input site file.

    Returns
    -------
    bz2_filename
        bzipped dmap filename
    """
    borealis_converter = BorealisConvert(filename, borealis_filetype,
                            dmap_filename, slice_id, borealis_file_structure='site')

    dmap_filename = borealis_converter.sdarn_filename # overwrite to as generated

    bz2_filename = compress_bz2(dmap_filename) # compress (and adds .bz2 to filename)
    os.remove(dmap_filename) # remove uncompressed

    return bz2_filename


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
    if borealis_filetype not in ['bfiq', 'rawacf', 'antennas_iq']:
        print('Cannot convert file {} from Borealis filetype '
            '{}'.format(borealis_site_file, borealis_filetype))
        sys.exit(1)

    slice_id = int(borealis_site_file.split('.')[-4]) # X.rawacf.hdf5.site
    array_filename = '.'.join(borealis_site_file.split('.')[0:-1]) # all but .site

    written_array_filename = borealis_site_to_array_file(borealis_site_file,
                                                        borealis_filetype,
                                                        array_filename)

    dmap_filetypes = {'rawacf': 'rawacf', 'bfiq': 'iqdat'}

    if borealis_filetype in dmap_filetypes.keys():

        try:
            # for 'rawacf' and 'bfiq' types, we can convert to arrays and to dmap.
            dmap_filetype = dmap_filetypes[borealis_filetype]
            dmap_filename = create_dmap_filename(borealis_site_file, dmap_filetype)
            written_dmap_filename =  borealis_array_to_dmap_files(written_array_filename,
                                    borealis_filetype, slice_id,
                                    dmap_filename)
            print('Wrote dmap to : {}'.format(written_dmap_filename))
        except (BorealisConvert2RawacfError, BorealisConvert2IqdatError, Exception) as e:
            print("Unable to convert {} to DMAP file.".format(written_array_filename))
            print("Due to error: {}".format(e))
            sys.exit(1)

    print('Wrote array to : {}'.format(written_array_filename))

    if __bzip2:
        # remove the decompressed site file from the directory because it was
        # generated.
        os.remove(borealis_site_file)

if __name__ == "__main__":
    main()
