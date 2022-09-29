#!/bin/bash
# rsync_to_campus
# Author: Dieter Andre
# Modification: August 6th 2019
# Simplified and changed to a loop over all files instead of acting on all files at once.
#
# Modification: November 22 2019
# Removed SDCOPY details from file
#
# Last Edited: September 2022 by Theo Kolkman
# Refactored for inotify usage
#
# A singleton script to transfer rawacf dmap and hdf5 files from site NAS to
# superdarn-cssdp on campus and removing them locally after a successful copy.
#
# Dependencies:
# 	- RADARID and SDCOPY set as environment variables in ${HOME_DIR}/.bashrc
#
# TODO: Update when inotify method is completed.
#
# Should be called from crontab like so:
# */5 * * * * ${HOME}/data_flow/site-linux/rsync_to_sas.sh >> ${HOME}/rsync_to_sas.log 2>&1

##############################################################################

# Specify error behaviour
set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

# readonly HOME_DIR="/home/transfer" # ${HOME} doesn't work since script is run by root
HOME_DIR=/home/radar #Testing
SDCOPY=radar@10.65.0.16 #Testing

source "${HOME_DIR}/.bashrc" # source the RADARID, SDCOPY and other things
source "${HOME_DIR}/data_flow/library/data_flow_functions.sh" # Load dataflow functions

##############################################################################

# Specify which sites will transfer each file type
readonly DMAP_SITES=("sas" "pgr" "inv") #Testing
readonly HDF5_SITES=("sas" "pgr" "inv" "cly" "rkn" "lab") #Testing

# Location rawacf files are transferring from
# readonly DATA_DIR="/borealis_nfs/borealis_data"
DATA_DIR=${HOME_DIR}/testing/data_flow_testing/data
readonly DMAP_SOURCE="${DATA_DIR}/rawacf_dmap/"
readonly ARRAY_SOURCE="${DATA_DIR}/rawacf_array/"

# If site isn't transferring dmap files, send to holding directory for campus conversion
if [[ ! " ${DMAP_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
	# readonly DEST="/sddata/${RADARID}_holding_dir"
	DEST=/home/radar/testing/data_flow_testing/sddata/lab_holding_dir #Testing
else
	readonly DEST="/sddata/${RADARID}_data/"
	# DEST=/home/radar/testing/data_flow_testing/lab_dir #Testing
fi

# A temp directory for rsync to use in case rsync is killed, it will start up where it left off
readonly TEMPDEST=".rsync_partial"

# Flag received from rsync_to_nas script to trigger this script
readonly FLAG_IN="${HOME_DIR}/data_flow/.inotify_watchdir/.convert_and_restructure_flag"

# Location of inotify watch directory for flags on superdar-cssdp
# readonly FLAG_DEST="/home/mrcopy/data_flow/.inotify_watchdir"
readonly FLAG_DEST="/home/radar/data_flow/.inotify_watchdir"	# TESTING

# Flag sent out to trigger auto_borealis_share script
readonly FLAG_OUT="${HOME_DIR}/data_flow/.inotify_flags/.rsync_to_campus_flag_${RADAR_ID}"

# Create log file. New file created daily
readonly LOGGING_DIR="${HOME_DIR}/logs/rsync_to_campus/$(date +%Y)/$(date +%m)"
mkdir --parents --verbose "${LOGGING_DIR}"
readonly LOGFILE="${LOGGING_DIR}/$(date +%Y%m%d).rsync_to_campus.log"
readonly SUMMARY_DIR="${HOME_DIR}/logs/rsync_to_campus/summary/$(date +%Y)/$(date +%m)"
mkdir --parents --verbose "${SUMMARY_DIR}"
readonly SUMMARY_FILE="${SUMMARY_DIR}/$(date -u +%Y%m%d).rsync_to_campus_summary.log"

##############################################################################

# Redirect all stdout and sterr in this script to $LOGFILE
exec &>> $LOGFILE

printf "################################################################################\n\n" | tee --append $SUMMARY_FILE

# Date in UTC format for logging
echo "Executing $(basename "$0") on $(hostname)" | tee --append $SUMMARY_FILE
date --utc "+%Y%m%d %H:%M:%S UTC" | tee --append $SUMMARY_FILE

# Ensure that only a single instance of this script runs.
if pidof -o %PPID -x -- "$(basename -- $0)" > /dev/null; then
	echo "Error: Script $0 is already running. Exiting..."
	exit 1
fi

# Check if this site is transferring dmaps to campus
if [[ " ${DMAP_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
	# Find all dmap files to transfer
	files=$(find ${DMAP_SOURCE} -name '*rawacf.bz2' -printf '%p\n')

	if [[ -n $files ]]; then
		printf "\nPlacing following dmap files in ${SDCOPY}:${DEST}:\n"
		printf '%s\n' "${files[@]}"
	else
		printf "\nNo dmap files found to be transferred.\n"
	fi

	# Transfer all files found
	for file in $files; do
		printf "\nTransferring: ${file}\n"
		rsync -av --partial --partial-dir=$TEMPDEST --timeout=180 --rsh=ssh $file $SDCOPY:$DEST

		# check if transfer was okay using the md5sum program, then remove the file if it matches
		verify_transfer $file "${DEST}/$(basename $file)" "${SDCOPY}"
		return_value=$?
		if [[ $return_value -eq 0 ]]; then
			echo "Successfully transferred: ${file}" | tee --append $SUMMARY_FILE
			echo "Deleting file..."
			rm --verbose ${file}
		else
            # If file not transferred successfully, don't delete and try again next time
			echo "Transfer failed: ${file}" | tee --append $SUMMARY_FILE
			echo "File not deleted."
		fi
	done
else
	printf "\nNot transferring any dmap files"
fi

# Check if this site is transferring HDF5 to campus
if [[ " ${HDF5_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
	# Find all hdf5 files to transfer
	files=`find ${ARRAY_SOURCE} -name '*rawacf.hdf5' -printf '%p\n'`

	if [[ -n $files ]]; then
		printf "\nPlacing following hdf5 files in ${SDCOPY}:${DEST}:\n"
		printf '%s\n' "${files[@]}"
	else
		printf "\nNo hdf5 files found to be transferred.\n"
	fi

	# Transfer all files found
	for file in $files; do
		printf "\nTransferring: ${file}\n"
		rsync -av --partial --partial-dir=$TEMPDEST --timeout=180 --rsh=ssh $file $SDCOPY:$DEST

		# check if transfer was okay using the md5sum program
		verify_transfer "${file}" "${DEST}/$(basename $file)" "${SDCOPY}"
		return_value=$?
		if [[ $return_value -eq 0 ]]; then
			echo "Successfully transferred: ${file}" | tee --append $SUMMARY_FILE
			echo "Deleting file..."
			rm --verbose ${file}
		else
            # If file not transferred successfully, don't delete and try again next time
			echo "Transfer failed: ${file}" | tee --append $SUMMARY_FILE
			echo "File not deleted."
		fi
	done
else
	printf "\nNot transferring any HDF5 files\n"
fi

printf "\nTriggering next script via inotify...\n"
# Remove "flag" sent by convert_and_restructure to reset flag
# touch $FLAG_IN #Testing
rm --verbose "${FLAG_IN}"
# Send "flag" file to notify mrcopy to start next script
touch $FLAG_OUT
rsync -av --rsh=ssh "${FLAG_OUT}" "${SDCOPY}:${FLAG_DEST}"

printf "\nFinished transferring. End time: $(date --utc "+%Y%m%d %H:%M:%S UTC")\n\n" | tee --append $SUMMARY_FILE

exit
