# Copyright 2020 SuperDARN Canada, University of Saskatchewan
# Authors: Devin Huyghebaert, Marci Detwiller
"""
Usage:

remove_record.py [-h] borealis_site_file record_name

Pass in the filename you wish to check. If no record name is given, the 
file will be searched for records where the data arrays are not the correct
size for the given num_sequences, then remove those records.

If a record name is provided, only the given record will be removed. 

"""
import argparse
import h5py
import pydarnio


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
    parser.add_argument("--remove-bad-recs",
                        help="Flag to remove records that are missing fields, have extra fields, or have fields of the "
                             "wrong type.", default=False, action="store_true")
    return parser


def find_borealis_sequence_errors(filename):
    """
    Removes any records in the file where the shape of the sequences data 
    is not the correct size for the given num_sequences. 
    """
    with h5py.File(filename, 'a') as f:
        records = sorted(list(f.keys()))
        for record_name in records:
            data = f[record_name]
            if data['sqn_timestamps'].shape[0] != data.attrs['num_sequences']:
                del f[record_name]
                print(f'Deleted: {record_name}')


def find_borealis_record_errors(filename):
    """
    Removes any records in the file where there are missing fields.
    """
    try:
        _ = pydarnio.BorealisRead(filename, 'rawacf', borealis_file_structure='site')
    except pydarnio.borealis_exceptions.BorealisBadRecordsError as err:
        bad_records = (list(err.missing_fields.keys()) +
                       list(err.extra_fields.keys()) +
                       list(err.incorrect_fields.keys()))
        if len(bad_records) > 0:
            with h5py.File(filename, 'r+') as f:
                for rec in bad_records:
                    del f[rec]
            print(f'Removed records {bad_records} from {filename}')


if __name__ == '__main__':
    parser = remove_records_parser()
    args = parser.parse_args()
    if args.remove_bad_recs:
        find_borealis_record_errors(args.borealis_site_file)
    find_borealis_sequence_errors(args.borealis_site_file)
