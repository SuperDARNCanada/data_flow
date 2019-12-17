#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller

# A script that uses pydarn to convert Borealis files to SDARN DMap files
# as well as restructures the hdf5 files to be multidimensional arrays
# for better file readability. Backs up the source site files before it
# begins.
#
# Dependencies include pydarn being installed in a virtualenv at $HOME/pydarn-env
# and RADARNAME being in environment variable
#
# The script should be run via crontab like so:
# 10,45 0,2,4,6,8,10,12,14,16,18,20,22 * * * ${HOME}/data_flow/site-linux/convert_and_restructure_daily.sh >> ${HOME}/convert_and_restructure_borealis_log.txt 2>&1


# prevent copying of files
echo 1 > ${HOME}/convert_daily_borealis_running

# Date, time and other stuff
DATE=`date +%Y%m%d`
DATE_UTC=`date -u`
CURYEAR=`date +%Y`
CURMONTH=`date +%m`
HOSTNAME=`hostname`

# What directories?
DAILY_DIR=/data/daily # this is the source
DMAP_DEST=/data/rawacf_dmap
ARRAY_DEST=/data/rawacf_array
BFIQ_ARRAY_DEST=/data/bfiq_array

LOGGINGDIR=${HOME}/logs/file_conversions/${CURYEAR}/${CURMONTH}
mkdir -p ${LOGGINGDIR}
LOGFILE=${LOGGINGDIR}/${DATE}.log

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
cp -v ${DAILY_DIR}/*rawacf.hdf5.site /data/backup
cp -v ${DAILY_DIR}/*bfiq.hdf5.site /data/backup
##############################################################################
# Convert the files to SDARN format and to array format for storage.
##############################################################################

# What file pattern should be converted?
RAWACF_FILE_PATTERN_TO_CONVERT=*rawacf.hdf5.site
BFIQ_FILE_PATTERN_TO_CONVERT=*bfiq.hdf5.site

echo "" >> ${LOGFILE} 2>&1
echo ${DATE_UTC} >> ${LOGFILE} 2>&1
echo "Restructuring files in ${DAILY_DIR}" >> ${LOGFILE} 2>&1

RAWACF_CONVERT_FILES=`find "${DAILY_DIR}" -name "${RAWACF_FILE_PATTERN_TO_CONVERT}" -type f`
BFIQ_CONVERT_FILES=`find "${DAILY_DIR}" -name "${BFIQ_FILE_PATTERN_TO_CONVERT}" -type f`
source ${HOME}/pydarn-env/bin/activate

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
        dmap_file="${f%hdf5.site}bz2"
        mv -v ${dmap_file} ${DMAP_DEST}/ >> ${LOGFILE} 2>&1
        array_file="${f%.site}"
        mv -v ${array_file} ${ARRAY_DEST} >> ${LOGFILE} 2>&1
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

if [ ! -z "$EMAILBODY" ]; then # check if not empty
    EMAILSUBJECT="[Conversions ${RADARNAME}] ${DATE}: Files failed conversion"
    echo -e ${EMAILBODY} >> ${LOGFILE} 2>&1
    send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi

rm -v ${HOME}/convert_daily_borealis_running
