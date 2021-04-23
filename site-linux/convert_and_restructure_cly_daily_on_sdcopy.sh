#!/bin/bash
# Copyright 2021 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller

# A script that uses pydarnio to convert Borealis files to SDARN DMap files
# Uses borealis_array_to_dmap.py script to convert already-restructured array files
# to dmap. 

# to be run on SDCOPY for when bandwidth limits exist at site and both hdf5 and 
# dmap files will not be transferred 

#
# Dependencies include pydarnio being installed in a virtualenv at $HOME/pydarnio-env

# Date, time and other stuff
DATE=`date +%Y%m%d`
DATE_UTC=`date -u`
CURYEAR=`date +%Y`
CURMONTH=`date +%m`
HOSTNAME=`hostname`

# What directories?
DAILY_DIR=/sddata/cly_holding_dir # this is the source
DMAP_DEST=/sddata/cly_data
RAWACF_ARRAY_DEST=/sddata/cly_data

LOGGINGDIR=${HOME}/logs/file_conversions/${CURYEAR}/${CURMONTH}
mkdir -p ${LOGGINGDIR}
LOGFILE=${LOGGINGDIR}/${DATE}.log

# Character and ordinal functions for '0' -> 'a' etc. in dmap file names
chr() {
  [ "$1" -lt 256 ] || return 1
  printf "\\$(printf '%03o' "$1")"
}

# Print decimal (%d) value of an ascii character (example 'ord c' would 
# print 99). Using C-style character set
ord() {
  LC_CTYPE=C printf '%d' "'$1"
}

##############################################################################
# Email function. Called if any files fail conversion. 
# Argument 1 should be the subject
# Argument 2 should be the body
##############################################################################
send_email () {
        # Argument 1 should be the subject
        # Argument 2 should be the body
        # What email address to send to?
        EMAILADDRESS="kevin.krieger@usask.ca"
        echo -e "${2}" | mutt -s "${1}" -- ${EMAILADDRESS}
}

# Copy the source rawacf file to backup.
#cp -v ${DAILY_DIR}/*rawacf.hdf5.site $BACKUP_DEST
#cp -v ${DAILY_DIR}/*bfiq.hdf5.site $BACKUP_DEST
##############################################################################
# Convert the files to SDARN format and to array format for storage.
##############################################################################

# What file pattern should be converted?
RAWACF_FILE_PATTERN_TO_CONVERT=*rawacf.hdf5

echo "" >> ${LOGFILE} 2>&1
echo ${DATE_UTC} >> ${LOGFILE} 2>&1
echo "Restructuring files in ${DAILY_DIR}" >> ${LOGFILE} 2>&1

RAWACF_CONVERT_FILES=`find "${DAILY_DIR}" -name "${RAWACF_FILE_PATTERN_TO_CONVERT}" -type f`
source ${HOME}/pydarnio-env/bin/activate

EMAILBODY=""

for f in ${RAWACF_CONVERT_FILES}
do
    echo "" >> ${LOGFILE} 2>&1
    echo "python3 ${HOME}/data_flow/script_archive/borealis_array_to_dmap.py ${f}" >> ${LOGFILE} 2>&1
    python3 ${HOME}/data_flow/script_archive/borealis_array_to_dmap.py ${f} >> ${LOGFILE} 2>&1
    ret=$?
    if [ $ret -eq 0 ]; then
        # move the resulting files if all was successful
        # then remove the source site file.
        dmap_file_start="${f%.rawacf.hdf5}"

        # remove last character(s) (slice_id)
        slice_id=${dmap_file_start##*.}
        dmap_file_wo_slice_id=${dmap_file_start%${slice_id}}

        ordinal_id="$(($slice_id + 97))"
        file_character=`chr $ordinal_id`
        dmap_file="${dmap_file_wo_slice_id}${file_character}.rawacf.bz2"
        mv -v ${dmap_file} ${DMAP_DEST}/ >> ${LOGFILE} 2>&1
        mv -v ${f} ${RAWACF_ARRAY_DEST} >> ${LOGFILE} 2>&1
    else
        EMAILBODY="${EMAILBODY}\nFile failed to convert: ${f}"
    fi
done

if [ ! -z "$EMAILBODY" ]; then # check if not empty
    EMAILSUBJECT="[Conversions CLY on SDCOPY] ${DATE}: Files failed conversion"
    echo -e ${EMAILBODY} >> ${LOGFILE} 2>&1
    send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi
