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


# Transfer to NAS flag. If false, transfer to $SITE_LINUX computer instead
readonly TRANSFER_TO_NAS=true


# Borealis directory files are transferring from
# Use jq with -r option source for the data from Borealis config file
readonly SOURCE="$(cat ${BOREALISPATH}/config.ini | jq -r '.data_directory')"

# Directory the files will be transferred to
if [[ "$TRANSFER_TO_NAS" == true ]]; then
	readonly DEST="/borealis_nfs/borealis_data/daily/"	# NAS
else
	readonly DEST="/data/borealis_data/daily"			# Site Linux
fi

# Threshold (in minutes) for selecting files to transfer with `find`.
# Set to filter out files currently being written to by Borealis.
readonly FILE_THRESHOLD=0.1		# 0.1 min = 6 s

# A temp directory for rsync to use in case rsync is killed, it will start up where it left off
readonly TEMPDEST=.rsync_partial

# Location of md5sum file to verify rsync transfer
readonly MD5=/tmp/md5

# Location of inotify flags on site linux
readonly FLAG_DIR=/home/transfer/logging/.dataflow_flags


# Date in UTC format for logging
basename "$0"
date -u 

# Sleep for specified time to differentiate files done writing from files currently writing
sleep 6

# Check if transferring to NAS or site computer. 
# If transferring to site computer, only send rawacf
if [[ "$TRANSFER_TO_NAS" == true ]]; then
	search="-name *rawacf.hdf5.* -o -name *bfiq.hdf5.* -o -name *antennas_iq.hdf5.*"
else
	search="-name *rawacf.hdf5.*"
fi

# Get all files that aren't currently being written do
files=$(find ${SOURCE} \( ${search} \) -cmin +${FILE_THRESHOLD})

if [[ -n $files ]]; then
	echo "Placing following files in ${DEST}:" 
	printf '%s\n' "${files[@]}"
else
	echo "No files to be transferred."
fi

# Transfer files
for file in ${files}
do
	if [[ "$TRANSFER_TO_NAS" == true ]]; then
		# rsync file to NAS
		rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${file} ${DEST}	
			
		# check if transfer was okay using the md5sum program, then remove the file if it matches
		md5sum -b ${DEST}${file} > ${MD5}
		md5sum -c ${MD5}
		mdstat=$?
		if [[ ${mdstat} -eq 0 ]]
			then
			echo "Deleting file: ${file}"
			rm -v ${file}	
		fi
	else
		# rsync file to site computer
		rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${file} ${SITE_LINUX}:${DEST}	
		
		# TODO: Add some sort of checking when transferring to other computer
	fi
done

# Send "flag" file to notify data flow computer to start next script
flag=/home/radar/dataflow/.rsync_to_nas_flag
touch $flag
rsync -av --rsh=ssh ${flag} ${SITE_LINUX}:${FLAG_DIR}

printf "Finished transferring. End time: $(date -u)\n\n\n"

exit
