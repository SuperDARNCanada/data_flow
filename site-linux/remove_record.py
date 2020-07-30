# Copyright 2020 SuperDARN Canada, University of Saskatchewan
# Authors: Devin Huyghebaert, Marci Detwiller
"""
Usage:

remove_record.py [-h] borealis_site_file record_name

Pass in the filename you wish to convert (should end in '.hdf5'). 

The script will convert the records to a dmap dictionary and then 
write to file as the given filename, with extensions 
'.[borealis_filetype].hdf5' replaced with [dmap_filetype].dmap. 
The script will also bzip the resulting dmap file.

Requires pydarn v1.1 to make use of scaling factor.

"""
import argparse
import deepdish
import h5py

def usage_msg():
    """
    Return the usage message for this process.
     
    This is used if a -h flag or invalid arguments are provided.
     
    :returns: the usage message
    """

    usage_message = """ remove_record.py [-h] borealis_site_file

    Pass in the filename you wish to check. If no record name is given, the 
    file will be searched for records where the data arrays are not the correct
    size for the given num_sequences, then remove those records.

    If a record name is provided, only the given record will be removed. 
    """

    return usage_message


def remove_records_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("borealis_site_file", help="Path to the array file that needs correction.")
    parser.add_argument("--record_name", default='', help="Group name in the hdf5 file to remove (Borealis record name).")
    return parser

def find_borealis_sequence_errors(filename):
	"""
	Removes any records in the file where the shape of the sequences data 
	is not the correct size for the given num_sequences. 
	"""
    records = sorted(deepdish.io.load(filename).keys())
    for record_name in records:
        groupname = '/' + record_name
        data = deepdish.io.load(iq_file_1,group=groupname)
        if data['sqn_timestamps'].shape[0]!=data['num_sequences']:
            remove_record(filename, groupname)

def remove_record(file_1, groupname):
    with h5py.File(file_1,  "a") as f:
        del f[groupname]
        print('Deleted: ', groupname)

if __name__ == '__main__':
    parser = remove_records_parser()
    args = parser.parse_args()
    if args.record_name:
    	remove_record(args.borealis_site_file, args.record_name)
    else:
    	find_borealis_sequence_errors(args.borealis_site_file)