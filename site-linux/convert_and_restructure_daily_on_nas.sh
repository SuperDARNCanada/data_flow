#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller
#
# Last Edited: September 2022 by Theo Kolkman
# Refactored for inotify usage
#
# A script that uses pydarnio to convert Borealis files to SDARN DMap files
# as well as restructures the hdf5 files to be multidimensional arrays
# for better file readability. Backs up the source site files before it
# begins.
#
# Dependencies:
#	- pydarnio installed in a virtualenv at $HOME/pydarnio-env
# 	- RADARNAME and RADARID set as environment variables in $HOME/.bashrc
#
# TODO: Update when inotify is working
# The script should be run via crontab like so:
# 10,45 0,2,4,6,8,10,12,14,16,18,20,22 * * * ${HOME}/data_flow/site-linux/convert_and_restructure_daily.sh >> ${HOME}/convert_and_restructure_borealis_log.txt 2>&1

##############################################################################

# Specify error behaviour
set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

readonly HOME_DIR="/home/transfer"  # ${HOME} doesn't work since script is run by root

# source the RADARID, SDCOPY and other things
source "${HOME_DIR}/.bashrc"
# Load in function library
source "${HOME_DIR}/data_flow/library/data_flow_functions.sh"

##############################################################################

# Define directories
readonly DATA_DIR="/borealis_nfs/borealis_data"
readonly SOURCE="${DATA_DIR}/daily" # this is the source
readonly DMAP_DEST="${DATA_DIR}/rawacf_dmap"
readonly RAWACF_ARRAY_DEST="${DATA_DIR}/rawacf_array"
readonly BFIQ_ARRAY_DEST="${DATA_DIR}/bfiq_array"
readonly ANTENNAS_IQ_ARRAY_DEST="${DATA_DIR}/antennas_iq_array"
readonly BACKUP_DEST="${DATA_DIR}/backup"
readonly PROBLEM_FILES_DEST="${DATA_DIR}/conversion_failure"

# Specify which sites will convert each file type
readonly RAWACF_SITES=("sas" "pgr" "inv" "cly" "rkn")
readonly BFIQ_SITES=("sas" "pgr" "inv" "cly" "rkn")
readonly ANTENNAS_IQ_SITES=("sas" "cly")

# Location of inotify watch directory for flags on site linux
# readonly FLAG_DEST="${HOME_DIR}/logging/.dataflow_flags"
readonly FLAG_DEST="/home/radar/testing/data_flow_testing/.dataflow_flags"

# Flag received from rsync_to_nas script to trigger this script
readonly FLAG_IN="${FLAG_DEST}/.rsync_to_nas_flag"

# Flag sent out to trigger rsync_to_campus script
readonly FLAG_OUT="${HOME_DIR}/data_flow/.convert_flag"

# Create log file. New file created daily
readonly LOGGING_DIR="${HOME_DIR}/logs/convert_and_restructure/$(date +%Y)/$(date +%m)"
mkdir --parents --verbose ${LOGGING_DIR}
readonly LOGFILE="${LOGGING_DIR}/$(date +%Y%m%d).convert_and_restructure.log"
readonly  SUMMARY_DIR="${HOME_DIR}/logs/convert_and_restructure/summary/$(date +%Y)/$(date +%m)"
mkdir --parents --verbose "${SUMMARY_DIR}"
readonly SUMMARY_FILE="${SUMMARY_DIR}/$(date -u +%Y%m%d).convert_summary.log"

# Redirect all stdout and sterr in this script to $LOGFILE
exec &>> $LOGFILE

##############################################################################


printf "################################################################################\n\n" | tee --append $SUMMARY_FILE

printf "Executing $(basename "$0") on $(hostname)\n" | tee --append $SUMMARY_FILE
date --utc "+%Y%m%d %H:%M:%S UTC" | tee --append $SUMMARY_FILE

# Ensure that only a single instance of this script runs.
if pidof -o %PPID -x -- "$(basename -- $0)" > /dev/null; then
	echo "Error: Script $0 is already running. Exiting..."
	exit 1
fi

# Copy the source rawacf file to backup.
printf "\nBacking up rawacf and bfiq .site files:\n"
cp --verbose ${SOURCE}/*rawacf.hdf5.site $BACKUP_DEST
cp --verbose ${SOURCE}/*bfiq.hdf5.site $BACKUP_DEST

RAWACF_CONVERT_FILES=$(find "${SOURCE}" -name "*rawacf.hdf5.site" -type f)
BFIQ_CONVERT_FILES=$(find "${SOURCE}" -name "*bfiq.hdf5.site" -type f)
ANTENNAS_IQ_CONVERT_FILES=$(find "${SOURCE}" -name "*antennas_iq.hdf5.site" -type f)
source "${HOME_DIR}/pydarnio-env/bin/activate"

if [[ -n $RAWACF_CONVERT_FILES ]]; then
	printf "\n\nConverting the following rawacf files:\n"
	printf '%s\n' "${RAWACF_CONVERT_FILES[@]}"
else
	printf "\nNo rawacf files to be converted.\n"
fi

# Convert rawacf files to array and dmap format
for f in $RAWACF_CONVERT_FILES; do
    if [[ " ${RAWACF_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
        printf "\nConverting ${f}\n"
        printf "python3 remove_record.py $(basename ${f})\n"
        remove_record_output=$(python3 ${HOME_DIR}/data_flow/site-linux/remove_record.py ${f})
        if [[ -n "$remove_record_output" ]]; then
            printf "Removed records from ${f}:\n${remove_records_output}\n" | tee --append $SUMMARY_FILE
        fi
        printf "python3 borealis_convert_file.py --dmap $(basename ${f})\n"
        python3 "${HOME_DIR}/data_flow/site-linux/borealis_convert_file.py" --dmap "${f}"
        ret=$?
        if [[ $ret -eq 0 ]]; then
            # move the resulting files if all was successful
            # then remove the source site file.
            dmap_file_start="${f%.rawacf.hdf5.site}"

            # remove last character(s) (slice_id)
            slice_id="${dmap_file_start##*.}"
            dmap_file_wo_slice_id="${dmap_file_start%${slice_id}}"

            ordinal_id="$(($slice_id + 97))"
            file_character=$(chr $ordinal_id)
            dmap_file="${dmap_file_wo_slice_id}${file_character}.rawacf.bz2"
            mv --verbose "${dmap_file}" "${DMAP_DEST}"
            array_file="${f%.site}"
            mv --verbose "${array_file}" "${RAWACF_ARRAY_DEST}"
            rm --verbose "${f}"
            printf "Successfully converted: ${f}\n" | tee --append $SUMMARY_FILE
        else
            printf "File failed to convert: ${f}\n" | tee --append $SUMMARY_FILE
            mv --verbose "${f}" "${PROBLEM_FILES_DEST}"
        fi
    else
        printf "Not converting: ${f}\n" | tee --append $SUMMARY_FILE
        mv --verbose "${f}" "${RAWACF_ARRAY_DEST}"
    fi
done


if [[ -n $BFIQ_CONVERT_FILES ]]; then
	printf "\nConverting the following bfiq files:\n"
	printf '%s\n' "${BFIQ_CONVERT_FILES[@]}"
else
	printf "\nNo bfiq files to be converted.\n"
fi

# Convert bfiq files to array format
for f in $BFIQ_CONVERT_FILES; do
    if [[ " ${BFIQ_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
        printf "\nConverting ${f}\n"
        printf "python3 remove_record.py $(basename ${f})\n"
        remove_record_output=$(python3 ${HOME_DIR}/data_flow/site-linux/remove_record.py ${f})
        if [[ -n "$remove_record_output" ]]; then
            printf "Removed records from ${f}:\n${remove_records_output}\n" | tee --append $SUMMARY_FILE
        fi
        printf "python3 borealis_convert_file.py --dmap $(basename ${f})\n"
        python3 "${HOME_DIR}/data_flow/site-linux/borealis_convert_file.py" "${f}"
        ret=$?
        if [[ $ret -eq 0 ]]; then
            # Only converting array file
            array_file="${f%.site}"
            mv --verbose "${array_file}" "${BFIQ_ARRAY_DEST}"
            rm --verbose "${f}"
            printf "Successfully converted: ${f}\n" | tee --append $SUMMARY_FILE
        else
            printf "File failed to convert: ${f}\n" | tee --append $SUMMARY_FILE
            mv --verbose "${f}" "${PROBLEM_FILES_DEST}"
        fi
    else
        printf "Not converting: ${f}\n" | tee --append $SUMMARY_FILE
        mv --verbose "${f}"  "${BFIQ_ARRAY_DEST}"
    fi
done


if [[ -n $ANTENNAS_IQ_CONVERT_FILES ]]; then
	printf "\nConverting the following antennas_iq files:\n"
	printf '%s\n' "${ANTENNAS_IQ_CONVERT_FILES[@]}"
else
	printf "\nNo antennas_iq files to be converted.\n"
fi

# Convert antennas_iq files to array format
for f in $ANTENNAS_IQ_CONVERT_FILES; do
    if [[ " ${ANTENNAS_IQ_SITES[*]} " =~ " ${RADAR_ID} " ]]; then
        printf "\nConverting ${f}\n"
        printf "python3 remove_record.py $(basename ${f})\n"
        remove_record_output=$(python3 ${HOME_DIR}/data_flow/site-linux/remove_record.py ${f})
        if [[ -n "$remove_record_output" ]]; then
            printf "Removed records from ${f}:\n${remove_records_output}\n" | tee --append $SUMMARY_FILE
        fi
        printf "python3 borealis_convert_file.py --dmap $(basename ${f})\n"
        python3 "${HOME_DIR}/data_flow/site-linux/borealis_convert_file.py" "${f}"
        ret=$?
        if [ $ret -eq 0 ]; then
            # then remove source site file.
            array_file="${f%.site}"
            mv --verbose "${array_file}" "${ANTENNAS_IQ_ARRAY_DEST}"
            rm --verbose "${f}"
            printf "Successfully converted: ${f}\n" | tee --append $SUMMARY_FILE
        else
            printf "File failed to convert: ${f}\n" | tee --append $SUMMARY_FILE
            mv --verbose "${f}" "${PROBLEM_FILES_DEST}"
        fi
    else
        printf "Not converting: ${f}\n" | tee --append $SUMMARY_FILE
        mv --verbose "${f}"  "${ANTENNAS_IQ_ARRAY_DEST}"
    fi
done

# TODO: Send error file to Engineering dashboard
printf "\nTriggering next script via inotify...\n"
# Remove "flag" sent by convert_and_restructure to reset flag
# and allow inotify to see the next flag sent in
touch $FLAG_IN
rm --verbose "${FLAG_IN}"

# Send out "flag" to trigger next script with inotify
touch "${FLAG_OUT}"
rsync -av --rsh=ssh "${FLAG_OUT}" "${FLAG_DEST}"

printf "\nFinished file conversion. End time: $(date --utc "+%Y/%m/%d %H:%M:%S UTC")\n\n" | tee --append $SUMMARY_FILE

exit