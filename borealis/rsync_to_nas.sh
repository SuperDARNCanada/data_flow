#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Kevin Krieger
#
# Last edited: September 2022 by Theodore Kolkman
#
# A singleton script to copy the data FILES to the on-site linux computer, 
# removing them locally after a successful copy. 
#
# Dependencies: 
#	- jq (installed via zypper)
# 	- BOREALISPATH and SITE_LINUX set as environment variables in $HOME/.profile
#
# TODO: Update when inotify method is completed.
# The script should be run via crontab like so:
# 8,45 */2 * * * . $HOME/.profile; $HOME/data_flow/rsync_to_linux.sh >> $HOME/rsync_to_linux.log 2>&1


# Ensure that only a single instance of this script runs.
if pidof -o %PPID -x -- "$(basename -- $0)" > /dev/null; then
	echo "Error: Script $0 is already running."
	exit 1
fi


# Define variables
SCRIPT_NAME=$(basename "$0")

# Directory the files will be transferred to
readonly DEST=/borealis_nfs/borealis_data/daily/

# Directory
# Use jq with -r option source for the data from Borealis config file
readonly SOURCE=$(cat ${BOREALISPATH}/config.ini | jq -r '.data_directory')

# Log file
readonly LOGFILE=/home/radar/logs/rsync_to_nas.log

# Threshold (in minutes) for selecting files to transfer with `find`.
# Set to filter out files currently being written to by Borealis.
FILE_THRESHOLD=0.1

# A temp directory for rsync to use in case rsync is killed, it will start up where it left off
TEMPDEST=.rsync_partial

# Directory of md5sum file to verify rsync transfer
MD5=/tmp/md5

# Date in UTC format for logging
echo ${SCRIPT_NAME} >> $LOGFILE 2>&1
date -u >> $LOGFILE 2>&1

files=$(find ${SOURCE} \( -name '*rawacf.hdf5.*' -o -name '*bfiq.hdf5.*' -o -name '*antennas_iq.hdf5.*' \) -cmin +${FILE_THRESHOLD})

echo "Placing following files in ${DEST}:" >> $LOGFILE 2>&1
printf '%s\n' "${files[@]}"
for file in $files
do
	# Get the file name, directory it's in and rsync it
	datafile=$(basename $file)	
	path=$(dirname $file)
	cd $path
	rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${datafile} ${DEST}
        
	# check if transfer was okay using the md5sum program, then remove the file if it matches
	md5sum -b ${DEST}${datafile} > ${MD5}
	md5sum -c ${MD5}
	mdstat=$?
	if [[ ${mdstat} -eq 0 ]]
       	then
		echo "Deleting file: ${file}"
		rm -v ${file}
	fi
done

exit
