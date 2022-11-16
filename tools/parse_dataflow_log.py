"""
Copyright 2022 SuperDARN Canada, University of Saskatchewan
Author: Theodore Kolkman

This script parses data flow logfiles in a specific directory for telemetry data on the data flow.
"""
import argparse
import os
import json
from glob import glob
from socket import gethostbyaddr
from datetime import datetime, timedelta, time


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
    parser.add_argument("out_file", help="File that the parsed data will be written to. Will be JSON format")
    parser.add_argument("-n", metavar="NUM_DAYS", type=int, default=7, nargs="?",
                        help="Number of days to collect logfile information for. Defaults to 7 days")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print final parsed output in readable format")

    return parser


def get_dataflow_overview(log_directory, scripts):
    """
    Collects summary data on each scripts operation. This data includes hostname, source and destination directories,
    and types of SuperDARN files operated on
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
        transferring_files = ['array', 'dmap']  # Filetypes sent back to campus
        converting_on_campus = True  # If convert_on_campus is converting for this site

        for index, line in enumerate(latest_entry):

            # Get hostname executing script
            if line.startswith("Executing"):
                # Ex): "Executing /home/radar/data_flow/borealis/rsync_to_nas on sasborealis"
                summary_data[script]['host'] = line.split()[-1]
                # Get last execution time
                date_string = latest_entry[index + 1].split()[0:2]
                date_string = ' '.join(date_string)
                dt_format = "%Y%m%d %H:%M:%S"
                dt = datetime.strptime(date_string, dt_format)
                date_string = dt.strftime("%Y-%d-%m %H:%M:%S")
                summary_data[script]['last_executed'] = date_string

            # Get git repo info
            if line.startswith(("data_flow", "pyDARNio")):
                git_repo = line.split()[0].split(':')[0]  # data_flow or pyDARNio, trim off ':'
                git_info = [line.split()[-7][:-1], ' '.join(line.split()[-3:-1])]
                summary_data[script][f'{git_repo}_branch'] = git_info

            # Summary entries unique to transfer scripts
            if script in ['rsync_to_nas', 'rsync_to_campus', 'distribute_borealis_data']:
                if line.startswith(("Transferring from:", "Distributing from:")):
                    summary_data[script]['source'] = line.split()[-1]

            # Summary entries unique to conversion scripts
            if script in ['convert_and_restructure', 'convert_on_campus']:
                if line.startswith("Conversion"):
                    # Ex): "Conversion directory: /borealis_nfs/borealis_data"
                    summary_data[script]['data_directory'] = line.split()[-1]

            # Summary entries unique to rsync_to_nas
            if script == 'rsync_to_nas':
                if line.startswith("Transferring to"):
                    # Ex): "Transferring to NAS: /borealis_nfs/borealis_data/daily/"
                    summary_data[script]['destination'] = line.split()[2][:-1]  # Trim ':' off end of string

            # Summary entries unique to rsync_to_campus
            if script == 'rsync_to_campus':
                if line.startswith("Transferring to:"):
                    # Ex) "Transferring to: mrcopy@128.233.224.39:/sddata/sas_data/"
                    full_destination = line.split()[-1].split(':')
                    dest_addr = full_destination[0]  # Ex) mrcopy@128.233.224.39
                    dest_ip = dest_addr.split("@")[1]  # Ex) 128.233.224.39

                    summary_data[script]['destination'] = gethostbyaddr(dest_ip)[0].split('.')[0]  # Convert IP to domain name
                    # summary_data[script]['destination_directory'] = full_destination[1]  # Ex) /sddata/sas_data/

                if line.startswith("Not transferring any"):
                    if 'dmap' in line:
                        transferring_files.remove('dmap')
                    if 'array' in line:
                        transferring_files.remove('array')

            # Summary entries unique to convert_on_campus
            if script == 'convert_on_campus':
                if line.startswith("Not converting"):
                    # Ex) "Not converting files for sas."
                    converting_on_campus = False

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
    old_file_threshold = 1  # Days
    old_log_threshold = timedelta(days=n)

    for script in scripts:
        # Find all log files for script
        logs = []
        pattern = f'*{script}*.log'
        for directory, _, _, in os.walk(log_directory):
            logs.extend(glob(os.path.join(directory, pattern)))

        # Get the n latest logfiles
        latest_logs = sorted(logs, key=os.path.getmtime, reverse=True)[0:n]
        # Remove all logs older than n days
        copy = latest_logs.copy()
        for log in latest_logs:
            log_dt = datetime.fromtimestamp(os.path.getmtime(log))
            current_dt = datetime.now()
            if current_dt - old_log_threshold > log_dt:
                copy.remove(log)
        latest_logs = copy

        if script == 'rsync_to_nas':
            dataflow_stats['rsync_to_nas'] = parse_transfer_logs(latest_logs, old_file_threshold)

        if script == 'convert_and_restructure':
            dataflow_stats['convert_and_restructure'] = parse_convert_logs(latest_logs, old_file_threshold)

        if script == 'rsync_to_campus':
            dataflow_stats['rsync_to_campus'] = parse_transfer_logs(latest_logs, old_file_threshold)

        if script == 'convert_on_campus':
            dataflow_stats['convert_on_campus'] = parse_convert_logs(latest_logs, old_file_threshold)

        if script == 'distribute_borealis_data':
            dataflow_stats['distribute_borealis_data'] = parse_transfer_logs(latest_logs, old_file_threshold)

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
    execution_times = []    # List of times each script took to execute

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
                    filename = line.split()[-1].split('/')[-1]
                    file_dt = get_file_datetime(filename)   # datetime file was created
                    if transfer_dt - threshold > file_dt:
                        old_files.append(filename)
                    no_action = False

                # Record all failed transfers
                if 'failed' in line:
                    filename = line.split()[-1].split('/')[-1]
                    failed_files.append(filename)
                    no_action = False

                # Calculate the total transfer time
                if line.startswith("Finished"):
                    date_string = ' '.join(line.split()[-3:-1])
                    dt_format = "%Y%m%d %H:%M:%S"
                    end_dt = datetime.strptime(date_string, dt_format)     # datetime transfer occurred
                    transfer_time = end_dt - transfer_dt
                    execution_times.append(transfer_time)

    # Calculate the script success rate
    total_files = successful_files + len(failed_files)
    if total_files > 0:
        success_rate = successful_files/total_files*100
    else:
        success_rate = 0

    # Calculate average time for the script to execute
    if len(execution_times) > 0:
        avg = sum(execution_times, timedelta(0)) / len(execution_times)
    else:
        avg = sum(execution_times, timedelta(0))
    average_time = (datetime.min + avg).time()

    stats['file_count'] = total_files
    stats['success_rate'] = f"{success_rate:.2f}%"
    stats['average_time'] = average_time.strftime("%H:%M:%S")
    stats['empty_runs'] = empty_runs
    stats['old_files'] = old_files
    stats['failed_files'] = failed_files

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
    no_action = False           # Flag to check if script executed transfers when it was triggered
    empty_runs = 0              # Number of times the script performed no transfers
    execution_times = []        # List of times each script took to execute

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
                    filename = line.split()[-1].split('/')[-1]
                    print(filename)
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
                    filename = line.split()[-1].split('/')[-1]
                    if 'rawacf' in filename:
                        failed_rawacf.append(filename)
                    if 'antennas_iq' in filename:
                        failed_antennas_iq.append(filename)
                    no_action = False

                # Record all files that have records removed
                if line.startswith("Removed records from"):
                    filename = line.split()[-1][:-1].split('/')[-1]
                    records_removed.append(filename)

                # Calculate the total transfer time
                if line.startswith("Finished"):
                    date_string = ' '.join(line.split()[-3:-1])
                    dt_format = "%Y%m%d %H:%M:%S"
                    end_dt = datetime.strptime(date_string, dt_format)  # datetime transfer occurred
                    transfer_time = end_dt - transfer_dt
                    execution_times.append(transfer_time)

    total_rawacf = successful_rawacf + len(failed_rawacf)
    total_antennas_iq = successful_antennas_iq + len(failed_antennas_iq)
    file_count = total_antennas_iq + total_rawacf

    if file_count > 0:
        success_rate = (successful_rawacf + successful_antennas_iq)/file_count*100
    else:
        success_rate = 0

    if len(execution_times) > 0:
        avg = sum(execution_times, timedelta(0)) / len(execution_times)
    else:
        avg = sum(execution_times, timedelta(0))
    average_time = (datetime.min + avg).time()

    stats['file_count'] = file_count
    stats['success_rate'] = f"{success_rate:.2f}%"
    stats['average_time'] = average_time.strftime("%H:%M:%S")
    stats['empty_runs'] = empty_runs
    stats['old_files'] = old_files

    # Store script specific data
    if scriptname == 'convert_and_restructure':
        stats['failed_rawacf'] = failed_rawacf
        stats['failed_antennas_iq'] = failed_antennas_iq
        stats['records_removed'] = records_removed
    else:
        stats['failed files'] = failed_rawacf

    return stats


def get_file_datetime(filename):
    """
    Parse a given filename and return a datetime object of its timestamp
    :param filename: Name of file to parse
    :return: Datetime object corresponding to filename
    """

    dt_format = "%Y%m%d.%H%M"
    filename = filename.split('.')[0:2]
    filename = '.'.join(filename)
    return datetime.strptime(filename, dt_format)


if __name__ == '__main__':
    p = argument_parser()
    args = p.parse_args()
    # Read in arguments
    log_dir = args.log_dir
    out_file = args.out_file
    num_days = args.n
    verbose = args.verbose

    scripts = ["rsync_to_nas", "convert_and_restructure", "rsync_to_campus", "convert_on_campus",
               "distribute_borealis_data"]
    summary_dict = get_dataflow_overview(log_dir, scripts)

    detailed_dict = parse_logfile(log_dir, scripts, num_days)

    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{today}")
    print(f"Parsing logs in {log_dir}")

    overall_dict = {}
    for script in scripts:
        overall_dict[script] = {}
        overall_dict[script]['summary'] = summary_dict[script]
        overall_dict[script]['stats'] = detailed_dict[script]
        overall_dict[script]['stats']['days_parsed'] = f"{num_days} days"

    # Print output in readable format
    if verbose:
        for i in overall_dict:
            print(i)
            for j in overall_dict[i]:
                print('\t', j)
                for k in overall_dict[i][j]:
                    val = overall_dict[i][j][k]
                    if isinstance(val, list) and len(val) > 0:
                        print('\t\t', k, ':', val[0])
                        for index, item in enumerate(val):
                            if index == 0:
                                pass
                            else:
                                print('\t\t  ', ' '*len(k), item)
                    else:
                        print('\t\t', k, ':', overall_dict[i][j][k])

    # Write parsed dictionary to json file
    with open(out_file, 'w') as fp:
        json.dump(overall_dict, fp, indent=4)

    print("Finished parsing logs")