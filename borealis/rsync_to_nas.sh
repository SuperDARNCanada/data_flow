#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Kevin Krieger
#
# Modification: September 2022
# Refactored for inotify usage
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

##############################################################################

# Transfer to NAS flag. If not true, transfer to $SITE_LINUX computer instead
readonly TRANSFER_TO_NAS=true

##############################################################################

# Specify error behaviour
set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

source ${HOME}/data_flow/library/data_flow_functions.sh

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
readonly TEMPDEST=".rsync_partial"

# Location of inotify watch directory for flags on site linux
readonly FLAG_DEST="/home/transfer/logging/.dataflow_flags"

# Flag to send to start next script
readonly FLAG="/home/radar/data_flow/.rsync_to_nas_flag"

# Create log file
readonly LOGFILE="/home/transfer/logs/rsync_to_nas.log"

##############################################################################

# Redirect all stdout and sterr in this script to $LOGFILE
exec &> $LOGFILE

# Date in UTC format for logging
basename "$0"
date --utc

# Ensure that only a single instance of this script runs.
if pidof -o %PPID -x -- "$(basename -- $0)" > /dev/null; then
	echo "Error: Script $0 is already running. Exiting..."
	exit 1
fi

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
		verify_transfer $file ${DEST}/$(basename $file)
		if [[ $? -eq 0 ]]; then		# Check return value of verify_transfer
			echo "Successfully transferred, deleting file: ${file}"
			# rm --verbose ${file}	
		else
			echo "Transfer failed, file not deleted: ${file}"
		fi
	else
		# rsync file to site computer
		rsync -av --partial --partial-dir="${TEMPDEST}" --timeout=180 --rsh=ssh "${file}" "${SITE_LINUX}:${DEST}"

		verify_transfer $file ${DEST}/$(basename $file) ${SITE_LINUX}
		if [[ $? -eq 0 ]]; then
			echo "Successfully transferred, deleting file: ${file}"
			rm --verbose ${file}	# TODO: Do we want to delete file after transferring to site linux?	
		else
			echo "Transfer failed, file not deleted: ${file}"
		fi
	fi
done

# Send "flag" file to notify data flow computer to start next script
touch "${FLAG}"
rsync -av --rsh=ssh "${FLAG}" "${SITE_LINUX}:${FLAG_DEST}"

printf "Finished transferring. End time: $(date -u)\n\n\n"

exit
