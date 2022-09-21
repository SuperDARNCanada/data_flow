#!/bin/bash
# rsync_to_sas
# Author: Dieter Andre
# Modification: August 6th 2019 
# Simplified and changed to a loop over all files instead of acting on all files at once.
#
# Modification: November 22 2019
# Removed SDCOPY details from file
#
# Modification: September 2022
# Refactored for inotify usage
#
# Should be called from crontab like so:
# */5 * * * * ${HOME}/data_flow/site-linux/rsync_to_sas.sh >> ${HOME}/rsync_to_sas.log 2>&1

##############################################################################

source "$HOME/.bashrc" # source the RADARID, SDCOPY and other things

readonly DMAP_SOURCE="/borealis_nfs/borealis_data/rawacf_dmap/"
readonly ARRAY_SOURCE="/borealis_nfs/borealis_data/rawacf_array/"

# move to holding dir to convert dmaps on SDCOPY
if [ "${RADARID}" == "cly" ] || [ "${RADARID}" == "rkn" ]; then
	readonly DEST="/sddata/${RADARID}_holding_dir"
else
	readonly DEST="/sddata/${RADARID}_data/"
fi

# A temp directory for rsync to use in case rsync is killed, it will start up where it left off
readonly TEMPDEST=".rsync_partial"

# Location of md5sum file to verify rsync transfer
readonly MD5="${HOME}/md5"

# Specify which sites will transfer each file type
readonly DMAP_SITES=("sas" "pgr" "inv")
readonly HDF5_SITES=("sas" "pgr" "inv" "cly" "rkn")

##############################################################################

# Date in UTC format for logging
basename "$0"
date --utc

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
		echo "Placing following dmap files in ${SDCOPY}:${DEST}:" 
		printf '%s\n' "${files[@]}"
	else
		echo "No files found to be transferred."
	fi

	# Transfer all files found
	for file in $files
	do
		rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${file} ${SDCOPY}:${DEST}

		# Check if transfer was okay using the md5sum program
		ssh ${SDCOPY} "md5sum --binary ${DEST}$(basename ${file})" > ${MD5}
		md5sum --check ${MD5}
		mdstat=$?
		if [[ ${mdstat} -eq 0 ]]; then
			echo "Deleting file: "${file}
			rm --verbose ${file}
        fi
	done
else
	echo "Not transferring any dmaps, only HDF5"
fi

# Check if this site is transferring dmaps to campus
if [[ " ${HDF5_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
	# Find all hdf5 files to transfer
	files=`find ${ARRAY_SOURCE} -name '*rawacf.hdf5' -printf '%p\n'`

	if [[ -n $files ]]; then
		echo "Placing following dmap files in ${SDCOPY}:${DEST}:" 
		printf '%s\n' "${files[@]}"
	else
		echo "No files to be transferred."
	fi

	for file in $files
	do
		rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${file} ${SDCOPY}:${DEST}

		# check if transfer was okay using the md5sum program
		ssh ${SDCOPY} "md5sum --binary ${DEST}$(basename ${file})" > ${MD5}
		md5sum --check ${MD5}
		mdstat=$?
		if [[ ${mdstat} -eq 0 ]] ; then
			echo "Deleting file: "${file}
			rm -v ${file}
		else
			echo "File not deleted ${file}"
		fi
	done
else
	echo "Not transferring any dmaps, only HDF5"
fi

exit
