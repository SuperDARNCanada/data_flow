#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marci Detwiller

# A singleton script to rotate borealis data files that follow a specific pattern
# on a filesystem. It will search for and remove the oldest files
# in a loop to keep making room for new files, if the filesystem usage is above 
# a certain threshold (example: if usage is above 90%, 
# delete 12 files on the /data partition)
#
# Dependencies include pydarn being installed in a virtualenv at $HOME/pydarn-env
# and RADARNAME being in environment variable
#
# The script should be run via crontab like so:
# 32 5,17 * * * . $HOME/.profile; $HOME/data_flow/borealis/rotate_borealis_files.sh >> $HOME/rotate_borealis_files.log 2>&1

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
HOME=/home/transfer

LOGGINGDIR=/home/transfer/logs/file_conversions/${CURYEAR}/${CURMONTH}
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
        EMAILADDRESS=kevin.krieger@usask.ca,marci.detwiller@usask.ca
        echo -e "${2}" | mutt -s "${1}" ${EMAILADDRESS}
}

# Copy the source rawacf file to backup.
cp -v ${DAILY_DIR}/*rawacf.hdf5* /data/backup

##############################################################################
# Convert the files to SDARN format and to array format for storage.
##############################################################################

# What file pattern should be converted?
RAWACF_FILE_PATTERN_TO_CONVERT=*rawacf.hdf5.site
BFIQ_FILE_PATTERN_TO_CONVERT=*bfiq.hdf5.site

# What files patterns need to be moved?
RAWACF_DMAP_PATTERN=*.rawacf.dmap.bz2
RAWACF_ARRAY_PATTERN=*.rawacf.hdf5
BFIQ_ARRAY_PATTERN=*.bfiq.hdf5
BFIQ_DMAP_PATTERN=*.iqdat.bz2

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
    	dmap_file="${f%hdf5.site}dmap.bz2"
    	mv -v ${DAILY_DIR}/${dmap_file} ${DMAP_DEST}/ >> ${LOGFILE} 2>&1
    	array_file="${f%.site}"
    	mv -v ${DAILY_DIR}/${array_file} ${ARRAY_DEST} >> ${LOGFILE} 2>&1
        echo "rm -v ${f}" >> ${LOGFILE} 2>&1 
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
    	dmap_file="${f%bfiq.hdf5.site}iqdat.dmap.bz2"
    	rm -v ${dmap_file} >> ${LOGFILE} 2>&1
    	array_file="${f%.site}"
    	mv -v ${DAILY_DIR}/${array_file} ${BFIQ_ARRAY_DEST}
        echo "rm -v ${f}" >> ${LOGFILE} 2>&1 
        rm -v ${f} >> ${LOGFILE} 2>&1
    else
        EMAILBODY="${EMAILBODY}\nFile failed to convert: ${f}"
    fi
done

if [ ! -z "$EMAILBODY" ]; then # check if not empty
    EMAILSUBJECT="${DATE_UTC} ${RADARNAME} Files failed conversion"
    echo -e ${EMAILBODY} >> ${LOGFILE} 2>&1
    send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi

echo Rawdata directory has `du -k $DEST |cut -f1` KBytes
rm -v ${HOME}/convert_daily_borealis_running
