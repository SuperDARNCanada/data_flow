"""
Copyright 2022 SuperDARN Canada, University of Saskatchewan
Author: Theodore Kolkman

TODO: Clean up this sandbox
What we want to parse:
- All files that failed conversion/transfer
- All files that have records removed (conv_and_rest)
- If no files are transferred/converted
- If bzip2 / hdf5 tests fail
- If files older than x days are present
"""
import argparse
import os
from glob import glob
from socket import gethostbyaddr
from datetime import datetime, timedelta


def usage_msg():
    """
    Return the usage message for this process.
    This message is printed if -h flag passed or invalid arguments are provided
    :return: The usage message
    """

    usage_message = """ parse_dataflow_log.py [-h] [-n NUM_DAYS] log_directory
    
    This script will parse the summary log files for all found dataflow scripts, and produce a json file containing 
    information on the logs.
    """

    return usage_message


def argument_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("log_dir", help="Path to the directory that holds all data flow summary logfiles to be parsed.")
    parser.add_argument("-n", metavar="NUM_DAYS", type=int, default=7, nargs="?",
                        help="Number of days to collect logfile information for. Defaults to 7 days")

    return parser


def get_dataflow_overview(log_directory, scripts):
    """

    :param log_directory: Directory to start searching for summary logfiles
    :param scripts: List of all scripts to search for. Possible scripts are: ['rsync_to_nas', 'convert_and_restructure',
    'rsync_to_campus', 'convert_on_campus', 'distribute_borealis_data']
    :return: Dictionary containing all summary data
    """

    if not os.path.isdir(log_directory):
        raise NotADirectoryError(f"{log_directory} is not a valid directory")

    possible_scripts = ["rsync_to_nas", "convert_and_restructure", "rsync_to_campus", "convert_on_campus",
                        "distribute_borealis_data"]

    if isinstance(scripts, str):
        scripts = [scripts]

    for s in scripts:
        if s not in possible_scripts:
            raise ValueError(f"{s} not a valid script name")

    # Search for each data flow script's logfile, and parse it for overview information
    summary_data = {}

    # print(scripts)
    for script in scripts:
        summary_data[script] = {}

        # Find all log files for script
        logs = []
        pattern = f'*{script}*.log'
        for directory, _, _, in os.walk(log_directory):
            logs.extend(glob(os.path.join(directory, pattern)))

        # Get the latest logfile to get most up-to-date summary info
        latest_log = max(logs, key=os.path.getmtime)

        # Get the last entered log entry
        latest_entry = []
        with open(latest_log) as f:
            for line in f:
                latest_entry.append(line.strip())
                if line.startswith("########"):
                    latest_entry = []

        # Iterate through latest entry and fill out summary dictionary
        conversion_files = ['rawacf', 'antennas_iq']  # Filetypes converted
        transferring_files = ['array', 'dmap']  # Filetypes sent back to campus
        converting_on_campus = True  # If convert_on_campus is converting for this site

        for line in latest_entry:

            # Get hostname executing script
            if line.startswith("Executing"):
                # Ex): "Executing /home/radar/data_flow/borealis/rsync_to_nas on sasborealis"
                summary_data[script]['host'] = line.split()[-1]

            # Summary entries unique to rsync_to_nas
            if script == 'rsync_to_nas':
                if line.startswith("Transferring from:"):
                    summary_data[script]['source'] = line.split()[-1]

                if line.startswith("Transferring to"):
                    # Ex): "Transferring to NAS: /borealis_nfs/borealis_data/daily/"
                    summary_data[script]['destination'] = line.split()[2][:-1]  # Trim ':' off end of string
                    summary_data[script]['destination_directory'] = line.split()[3]

            # Summary entries unique to convert_and_restructure
            if script == 'convert_and_restructure':
                if line.startswith("Conversion"):
                    # Ex): "Conversion directory: /borealis_nfs/borealis_data"
                    summary_data[script]['data_directory'] = line.split()[-1]

                if line.startswith("Not converting:"):
                    # Ex): "Not converting: /borealis_nfs/borealis_data/daily/20221101.0400.00.sas.0.antennas_iq.hdf5.site"
                    file_name = line.split()[-1]
                    if 'rawacf' in file_name:
                        conversion_files.remove('rawacf')
                    if 'antennas_iq' in file_name:
                        conversion_files.remove('antennas_iq')

            # Summary entries unique to rsync_to_campus
            if script == 'rsync_to_campus':
                if line.startswith("Transferring from:"):
                    # Ex) Transferring from: /borealis_nfs/borealis_data
                    summary_data[script]['source'] = line.split()[-1]

                if line.startswith("Transferring to:"):
                    # Ex) "Transferring to: mrcopy@128.233.224.39:/sddata/sas_data/"
                    full_destination = line.split()[-1].split(':')
                    dest_addr = full_destination[0]  # Ex) mrcopy@128.233.224.39
                    dest_ip = dest_addr.split("@")[1]  # Ex) 128.233.224.39

                    summary_data[script]['destination'] = gethostbyaddr(dest_ip)[0]  # Convert IP to domain name
                    summary_data[script]['destination_directory'] = full_destination[1]  # Ex) /sddata/sas_data/

                if line.startswith("Not transferring any"):
                    if 'dmap' in line:
                        transferring_files.remove('dmap')
                    if 'array' in line:
                        transferring_files.remove('array')

            if script == 'convert_on_campus':
                if line.startswith("Not converting"):
                    # Ex) "Not converting files for sas."
                    converting_on_campus = False

            if script == 'distribute_borealis_data':
                if line.startswith("Distributing from:"):
                    # Ex) "Distributing from: /sddata/sas_data"
                    summary_data[script]['source'] = line.split()[-1]

                if line.startswith("NAS:"):
                    # Ex) "    NAS: /data/borealis_site_data"
                    summary_data[script]['NAS_directory'] = line.split()[-1]

                if line.startswith("Mirror:"):
                    # Ex) "    Mirror: /data/holding/globus
                    summary_data[script]['mirror_staging'] = line.split()[-1]

                if line.startswith("Institutions:"):
                    # Ex) "    Institutions: /home/bas/outgoing/sas    /home/vtsd/outgoing/sas
                    summary_data[script]['institutions_staging'] = line.split()[1:3]

        if script == 'convert_and_restructure':
            summary_data[script]['convert_filetype'] = conversion_files

        if script == 'rsync_to_campus':
            summary_data[script]['transfer_filetype'] = transferring_files

        if script == 'convert_on_campus':
            summary_data[script]['is_converting'] = converting_on_campus

    return summary_data


def parse_logfile(log_directory, scripts, n):
    """
    Parse the logfiles for a given script and return statistics
    :param log_directory: Directory to start searching logfiles
    :param scripts: List of all scripts to parse for. Possible scripts are: ['rsync_to_nas', 'convert_and_restructure',
    'rsync_to_campus', 'convert_on_campus', 'distribute_borealis_data']
    :param n: Parse last n days of scripts
    :return: Dictionary containing stats for specified script
    """

    if not os.path.isdir(log_directory):
        raise NotADirectoryError(f"{log_directory} is not a valid directory")

    possible_scripts = ["rsync_to_nas", "convert_and_restructure", "rsync_to_campus", "convert_on_campus",
                        "distribute_borealis_data"]

    if isinstance(scripts, str):
        scripts = [scripts]

    for s in scripts:
        if s not in possible_scripts:
            raise ValueError(f"{s} not a valid script name")

    dataflow_stats = {}
    threshold = 1

    for script in scripts:
        # Find all log files for script
        logs = []
        pattern = f'*{script}*.log'
        for directory, _, _, in os.walk(log_directory):
            logs.extend(glob(os.path.join(directory, pattern)))

        # Get the n latest logfiles
        latest_logs = sorted(logs, key=os.path.getmtime, reverse=True)[0:n]
        # print(latest_logs)

        if script == 'rsync_to_nas':
            dataflow_stats['rsync_to_nas'] = parse_transfer_logs(latest_logs, threshold)

        if script == 'convert_and_restructure':
            dataflow_stats['convert_and_restructure'] = parse_convert_logs(latest_logs, threshold)

        if script == 'rsync_to_campus':
            dataflow_stats['rsync_to_campus'] = parse_transfer_logs(latest_logs, threshold)

        if script == 'convert_on_campus':
            dataflow_stats['convert_on_campus'] = parse_convert_logs(latest_logs, threshold)

        if script == 'distribute_borealis_data':
            dataflow_stats['distribute_borealis_data'] = parse_transfer_logs(latest_logs, threshold)

    return dataflow_stats


def parse_transfer_logs(logfiles, threshold):
    """
    Parse given rsync_to_nas or rsync_to_campus logfiles for telemetry info. Gets following info:
        - Number of successful and total transfers
        - All files that failed transfer
        - All files produced more than "threshold" days before the transfer
    :param logfiles: list of rsync_to_nas or rsync_to_campus logfile absolute paths
    :param threshold: How old a file can be before raising notification
    :return: dictionary containing status of rsync_to_nas or rsync_to_campus file transfers from provided logs
    """

    stats = {}
    threshold = timedelta(days=threshold)  # How old a file is to raise a flag

    failed_files = []       # List containing all files that failed transfer
    old_files = []          # List containing all files older than the
    successful_files = 0    # Number of successful files transferred
    no_action = False       # Flag to check if script executed transfers when it was triggered
    empty_runs = 0          # Number of times the script performed no transfers

    for log in logfiles:
        with open(log) as f:
            lines = f.readlines()
            for index, line in enumerate(lines):
                # Loop through each logfile and collect statistics

                # Get time of transfer
                if line.startswith("Executing"):
                    if no_action:
                        empty_runs += 1
                    no_action = True
                    date_string = lines[index+1].split()[0:2]
                    date_string = ' '.join(date_string)
                    dt_format = "%Y%m%d %H:%M:%S"
                    transfer_dt = datetime.strptime(date_string, dt_format)     # datetime transfer occurred

                # Check if successful file is old
                if 'successful' in line.lower():
                    successful_files += 1
                    filename = line.split()[-1]
                    file_dt = get_file_datetime(filename)   # datetime file was created
                    if transfer_dt - threshold > file_dt:
                        old_files.append(filename)
                    no_action = False

                # Record all failed transfers
                if 'failed' in line:
                    filename = line.split()[-1]
                    failed_files.append(filename)
                    no_action = False

    total_files = successful_files + len(failed_files)

    stats['total_file_count'] = total_files
    stats['successful_file_count'] = successful_files
    stats['failed_files'] = failed_files
    stats['old_files'] = old_files
    stats['empty_runs'] = empty_runs

    return stats


def parse_convert_logs(logfiles, threshold):
    """
    Parse given convert_and_restructure or convert_on_campus logfiles for telemetry info. Gets following info:
        - Number of successful and total conversions for each type
        - All files that failed conversion/restructuring
        - All files produced more than "threshold" days before the transfer
    :param logfiles: list of convert_and_restructure or convert_on_campus logfile absolute paths
    :param threshold: How old a file can be before raising notification
    :return: dictionary containing status of conversion actions on files from provided logs
    """

    scriptname = ""
    stats = {}
    threshold = timedelta(days=threshold)  # How old a file is to raise a flag

    failed_rawacf = []          # List containing all rawacf that failed conversion
    failed_antennas_iq = []     # List containing all antennas_iq that failed restructuring
    records_removed = []        # List containing all files that had records removed
    old_files = []              # List containing all files older than the
    successful_rawacf = 0       # Number of successful rawacf conversions
    successful_antennas_iq = 0  # Number of successful antennas_iq conversions
    no_action = False       # Flag to check if script executed transfers when it was triggered
    empty_runs = 0          # Number of times the script performed no transfers

    for log in logfiles:
        with open(log) as f:
            lines = f.readlines()
            for index, line in enumerate(lines):
                # Loop through each logfile and collect statistics

                # Get time of transfer
                if line.startswith("Executing"):
                    if no_action:
                        empty_runs += 1
                    no_action = True

                    if 'convert_and_restructure' in line:
                        scriptname = 'convert_and_restructure'
                    if 'convert_on_campus' in line:
                        scriptname = 'convert_on_campus'

                    date_string = lines[index+1].split()[0:2]
                    date_string = ' '.join(date_string)
                    dt_format = "%Y%m%d %H:%M:%S"
                    transfer_dt = datetime.strptime(date_string, dt_format)     # datetime transfer occurred

                # Check if successful file is old
                if line.startswith("Successfully converted:"):
                    filename = line.split()[-1]
                    if 'rawacf' in filename:
                        successful_rawacf += 1
                    if 'antennas_iq' in filename:
                        successful_antennas_iq += 1

                    file_dt = get_file_datetime(filename)   # datetime file was created
                    if transfer_dt - threshold > file_dt:
                        old_files.append(filename)
                    no_action = False

                # Record all failed conversions/restructures
                if line.startswith("File failed to convert:"):
                    filename = line.split()[-1]
                    if 'rawacf' in filename:
                        failed_rawacf.append(filename)
                    if 'antennas_iq' in filename:
                        failed_antennas_iq.append(filename)
                    no_action = False

                # Record all files that have records removed
                if line.startswith("Removed records from"):
                    filename = line.split()[-1][:-1]
                    records_removed.append(filename)

    total_rawacf = successful_rawacf + len(failed_rawacf)
    total_antennas_iq = successful_antennas_iq + len(failed_antennas_iq)
    total_file_count = total_antennas_iq + total_rawacf

    if scriptname == 'convert_and_restructure':
        stats['total_file_count'] = total_file_count
        stats['successful_rawacf_count'] = successful_rawacf
        stats['successful_antennas_iq_count'] = successful_antennas_iq
        stats['failed_rawacf'] = failed_rawacf
        stats['failed_antennas_iq'] = failed_antennas_iq
        stats['old_files'] = old_files
        stats['records_removed'] = records_removed
        stats['empty_runs'] = empty_runs
    elif scriptname == 'convert_on_campus':
        stats['total_file_count'] = total_file_count
        stats['successful_file_count'] = successful_rawacf
        stats['failed filed'] = failed_rawacf
        stats['old_files'] = old_files
        stats['empty_runs'] = empty_runs
    else:
        raise Exception(f"Script name {scriptname} not valid")

    return stats


def get_file_datetime(filename):
    """
    Parse a given filename and return a datetime object of its timestamp
    :param filename: Name of file to parse
    :return: Datetime object corresponding to filename
    """

    dt_format = "%Y%m%d.%H%M"
    filename = filename.split('/')[-1]
    filename = filename.split('.')[0:2]
    filename = '.'.join(filename)
    return datetime.strptime(filename, dt_format)


if __name__ == '__main__':
    p = argument_parser()
    args = p.parse_args()

    scripts = ["rsync_to_nas", "convert_and_restructure", "rsync_to_campus", "convert_on_campus",
               "distribute_borealis_data"]
    summary_dict = get_dataflow_overview("/home/radar/testing/pythonTesting", scripts)

    detailed_dict = parse_logfile("/home/radar/testing/pythonTesting", scripts, 3)

    overall_dict = {}
    for script in scripts:
        overall_dict[script] = {}
        overall_dict[script]['summary'] = summary_dict[script]
        overall_dict[script]['stats'] = detailed_dict[script]

    for i in overall_dict:
        print(i)
        for j in overall_dict[i]:
            print('\t', j)
            for k in overall_dict[i][j]:
                print('\t\t', k, ':', overall_dict[i][j][k])
