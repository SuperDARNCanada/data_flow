#!/bin/bash
# Copyright 2021 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller
#
# Last Edited: September 2022 by Theo Kolkman
# Refactored for inotify usage
#
# A script that uses pydarnio to convert Borealis files to SDARN DMap files
# Uses borealis_array_to_dmap.py script to convert already-restructured array files
# to dmap. 
# 
# to be run on SDCOPY for when bandwidth limits exist at site and both hdf5 and 
# dmap files will not be transferred 
#
# Dependencies:
#	- pydarnio installed in a virtualenv at $HOME/pydarnio-env
#
# Usage: ./convert_on_campus RADAR_ID
#
# TODO: Update when inotify is working

##############################################################################

# Specify error behaviour
set -o errexit   # abort on nonzero exitstatus
set -o nounset   # abort on unbound variable
set -o pipefail  # don't hide errors within pipes

readonly HOME_DIR="/home/mrcopy"  # ${HOME} doesn't work since script is run by root

# source the RADAR_ID, SDCOPY and other things
source "${HOME_DIR}/.bashrc"
# Load in function library
source "${HOME_DIR}/data_flow/library/data_flow_functions.sh"

##############################################################################

RADAR_ID=$1

# Sites that have to convert dmap files on campus
# If a site isn't specified here, then this script is skipped and the flag
# for the next script is sent immediately
readonly SITES=("cly" "rkn")

# Define directories
DATA_DIR="/sddata"
SOURCE="${DATA_DIR}/${RADAR_ID}_holding_dir" # this is the source
RAWACF_DMAP_DEST="${DATA_DIR}/${RADAR_ID}_data"
RAWACF_ARRAY_DEST="${DATA_DIR}/${RADAR_ID}_data"

# Flag received from rsync_to_nas script to trigger this script
readonly FLAG_IN="${HOME_DIR}/data_flow/.inotify_watchdir/.rsync_to_campus_flag_${RADAR_ID}"

# Location of inotify watch directory for flags on site linux
readonly FLAG_DEST="${HOME_DIR}/data_flow/.inotify_watchdir"

# Flag sent out to trigger rsync_to_campus script
readonly FLAG_OUT="${HOME_DIR}/data_flow/.inotify_flags/.convert_on_campus_flag_${RADAR_ID}"

# Create log file. New file created daily
readonly LOGGING_DIR="${HOME_DIR}/logs/convert_on_campus/$(date +%Y)/$(date +%m)"
mkdir --parents --verbose $LOGGING_DIR
readonly LOGFILE="${LOGGING_DIR}/${RADAR_ID}.$(date +%Y%m%d).convert_on_campus.log"
readonly  SUMMARY_DIR="${HOME_DIR}/logs/convert_and_restructure/summary/$(date +%Y)/$(date +%m)"
mkdir --parents --verbose $SUMMARY_DIR
readonly SUMMARY_FILE="${SUMMARY_DIR}/$(date -u +%Y%m%d).convert_summary.log"

##############################################################################

# Redirect all stdout and sterr in this script to $LOGFILE
exec &>> $LOGFILE

printf "################################################################################\n\n" | tee --append $SUMMARY_FILE

printf "Executing $(basename "$0") on $(hostname)\n" | tee --append $SUMMARY_FILE
date --utc "+%Y%m%d %H:%M:%S UTC" | tee --append $SUMMARY_FILE

if [[ " ${SITES[*]} " =~ " ${RADAR_ID} " ]]; then

    #### TODO: Do we need a file lock? Multiple instances of this script may be
    #### running at same time for each site. Is this okay? Should we prevent this
    #### or add some safety checks?
    # Ensure that only a single instance of this script runs for this site
    # if pidof -o %PPID -x -- "$(basename -- $0) ${RADAR_ID}" > /dev/null; then
    #     echo "Error: Script $0 is already running. Exiting..."
    #     exit 1
    # fi

    echo "Restructuring files in ${DAILY_DIR}" >> ${LOGFILE} 2>&1

    RAWACF_CONVERT_FILES=`find "${SOURCE}" -maxdepth 1 -name "*rawacf.hdf5" -type f`
    source "${HOME}/pydarnio-env/bin/activate"

    if [[ -n $RAWACF_CONVERT_FILES ]]; then
        printf "\n\nConverting the following files:\n"
        printf '%s\n' "${RAWACF_CONVERT_FILES[@]}"
    else
        printf "\nNo files to be converted.\n"
    fi

    # EMAILBODY=""

    for f in ${RAWACF_CONVERT_FILES}
    do
        printf "\nConverting ${f}\n"
        printf "python3 borealis_array_to_dmap.py $(basename ${f})\n"
        python3 "${HOME}/data_flow/script_archive/borealis_array_to_dmap.py" $f
        ret=$?
        if [[ $ret -eq 0 ]]; then
            # move the resulting files if all was successful
            # then remove the source site file.
            dmap_file_start="${f%.rawacf.hdf5}"

            # remove last character(s) (slice_id)
            slice_id=${dmap_file_start##*.}
            dmap_file_wo_slice_id=${dmap_file_start%${slice_id}}

            ordinal_id="$(($slice_id + 97))"
            file_character=$(chr $ordinal_id)
            dmap_file="${dmap_file_wo_slice_id}${file_character}.rawacf.bz2"
            mv -v $dmap_file $RAWACF_DMAP_DEST
            mv -v $f $RAWACF_ARRAY_DEST
            printf "Successfully converted: ${f}\n" | tee --append $SUMMARY_FILE
        else
            printf "File failed to convert: ${f}\n" | tee --append $SUMMARY_FILE
            mv --verbose $f $PROBLEM_FILES_DEST
    done
fi

# TODO: Send error file to Engineering dashboard
printf "\nTriggering next script via inotify...\n"
# Remove "flag" sent by convert_and_restructure to reset flag
# and allow inotify to see the next flag sent in
rm --verbose $FLAG_IN

# Send out "flag" to trigger next script with inotify
touch $FLAG_OUT
rsync -av --rsh=ssh $FLAG_OUT $FLAG_DEST

printf "\nFinished file conversion. End time: $(date --utc "+%Y%m%d %H:%M:%S UTC")\n\n" | tee --append $SUMMARY_FILE

exit
