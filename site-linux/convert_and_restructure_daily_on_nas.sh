#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller

# A script that uses pydarnio to convert Borealis files to SDARN DMap files
# as well as restructures the hdf5 files to be multidimensional arrays
# for better file readability. Backs up the source site files before it 
# begins.
#
# Dependencies include pydarnio being installed in a virtualenv at $HOME/pydarnio-env
# and RADARNAME being in environment variable
#
# The script should be run via crontab like so:
# 10,45 0,2,4,6,8,10,12,14,16,18,20,22 * * * ${HOME}/data_flow/site-linux/convert_and_restructure_daily.sh >> ${HOME}/convert_and_restructure_borealis_log.txt 2>&1

# Date, time and other stuff
DATE=`date +%Y%m%d`
DATE_UTC=`date -u`
CURYEAR=`date +%Y`
CURMONTH=`date +%m`
HOSTNAME=`hostname`

# What directories?
DAILY_DIR=/borealis_nfs/borealis_data/daily # this is the source
DMAP_DEST=/borealis_nfs/borealis_data/rawacf_dmap
RAWACF_ARRAY_DEST=/borealis_nfs/borealis_data/rawacf_array
BFIQ_ARRAY_DEST=/borealis_nfs/borealis_data/bfiq_array
ANTENNAS_IQ_ARRAY_DEST=/borealis_nfs/borealis_data/antennas_iq_array
BACKUP_DEST=/borealis_nfs/borealis_data/backup

LOGGINGDIR=${HOME}/logs/file_conversions/${CURYEAR}/${CURMONTH}
mkdir -p ${LOGGINGDIR}
LOGFILE=${LOGGINGDIR}/${DATE}.log

# Character and ordinal functions for '0' -> 'a' etc. in dmap file names
chr() {
  [ "$1" -lt 256 ] || return 1
  printf "\\$(printf '%03o' "$1")"
}

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
        EMAILADDRESS="kevin.krieger@usask.ca, marci.detwiller@usask.ca"
        echo -e "${2}" | mutt -s "${1}" -- ${EMAILADDRESS}
}

# Copy the source rawacf file to backup.
cp -v ${DAILY_DIR}/*rawacf.hdf5.site $BACKUP_DEST
cp -v ${DAILY_DIR}/*bfiq.hdf5.site $BACKUP_DEST
##############################################################################
# Convert the files to SDARN format and to array format for storage.
##############################################################################

# What file pattern should be converted?
RAWACF_FILE_PATTERN_TO_CONVERT=*rawacf.hdf5.site
BFIQ_FILE_PATTERN_TO_CONVERT=*bfiq.hdf5.site
ANTENNAS_IQ_FILE_PATTERN_TO_CONVERT=*antennas_iq.hdf5.site

echo "" >> ${LOGFILE} 2>&1
echo ${DATE_UTC} >> ${LOGFILE} 2>&1
echo "Restructuring files in ${DAILY_DIR}" >> ${LOGFILE} 2>&1

RAWACF_CONVERT_FILES=`find "${DAILY_DIR}" -name "${RAWACF_FILE_PATTERN_TO_CONVERT}" -type f`
BFIQ_CONVERT_FILES=`find "${DAILY_DIR}" -name "${BFIQ_FILE_PATTERN_TO_CONVERT}" -type f`
ANTENNAS_IQ_CONVERT_FILES=`find "${DAILY_DIR}" -name "${ANTENNAS_IQ_FILE_PATTERN_TO_CONVERT}" -type f`
source ${HOME}/pydarnio-env/bin/activate

EMAILBODY=""

for f in ${RAWACF_CONVERT_FILES}
do
    echo "" >> ${LOGFILE} 2>&1
    echo "python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f}" >> ${LOGFILE} 2>&1
    python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f} >> ${LOGFILE} 2>&1
    ret=$?
    if [ $ret -eq 0 ]; then
        # move the resulting files if all was successful
        # then remove the source site file.
        dmap_file_start="${f%.rawacf.hdf5.site}"

        # remove last character (slice_id)
        dmap_file_wo_slice_id=${dmap_file_start%?}
        
        check_char=${dmap_wo_slice_id: -1}
        if [ $check_char != "." ]; then
            # will be last two chars, >9
            dmap_file_wo_slice_id=${dmap_file_start%??}
            slice_id=${dmap_file_start: -2}
        else
            # last char is slice id, keep dmap_file_wo_slice_id as is
            slice_id=${dmap_file_start: -1}
        fi
        echo $dmap_file_wo_slice_id
        ordinal_id="$(($slice_id + 97))"
        file_character=`chr $ordinal_id`
        dmap_file="${dmap_file_wo_slice_id}${file_character}.rawacf.bz2"
        mv -v ${dmap_file} ${DMAP_DEST}/ >> ${LOGFILE} 2>&1
        array_file="${f%.site}"
        mv -v ${array_file} ${RAWACF_ARRAY_DEST} >> ${LOGFILE} 2>&1
        rm -v ${f} >> ${LOGFILE} 2>&1
    else
        EMAILBODY="${EMAILBODY}\nFile failed to convert: ${f}"
    fi
done

for f in ${BFIQ_CONVERT_FILES}
do
    echo "" >> ${LOGFILE} 2>&1
    echo "python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f}" >> ${LOGFILE} 2>&1
    python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f} >> ${LOGFILE} 2>&1
    ret=$?
    if [ $ret -eq 0 ]; then
        # remove iqdat and move bfiq array file if successful.
        # then remove source site file.
        dmap_file="${f%bfiq.hdf5.site}iqdat.bz2"
        rm -v ${dmap_file} >> ${LOGFILE} 2>&1
        array_file="${f%.site}"
        mv -v ${array_file} ${BFIQ_ARRAY_DEST} >> ${LOGFILE} 2>&1
        rm -v ${f} >> ${LOGFILE} 2>&1
    else
        EMAILBODY="${EMAILBODY}\nFile failed to convert: ${f}"
    fi
done

for f in ${ANTENNAS_IQ_CONVERT_FILES}
do
    echo "" >> ${LOGFILE} 2>&1
    echo "python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f}" >> ${LOGFILE} 2>&1
    python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f} >> ${LOGFILE} 2>&1
    ret=$?
    if [ $ret -eq 0 ]; then
        # remove iqdat and move bfiq array file if successful.
        # then remove source site file.
        array_file="${f%.site}"
        mv -v ${array_file} ${ANTENNAS_IQ_ARRAY_DEST} >> ${LOGFILE} 2>&1
        rm -v ${f} >> ${LOGFILE} 2>&1
    else
        EMAILBODY="${EMAILBODY}\nFile failed to convert: ${f}"
    fi
done

if [ ! -z "$EMAILBODY" ]; then # check if not empty
    EMAILSUBJECT="[Conversions ${RADARNAME}] ${DATE}: Files failed conversion"
    echo -e ${EMAILBODY} >> ${LOGFILE} 2>&1
    send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi
