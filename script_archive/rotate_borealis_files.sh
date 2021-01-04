#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Kevin Krieger

# A singleton script to rotate borealis data files that follow a specific pattern
# on a filesystem. It will search for and remove the oldest files
# in a loop to keep making room for new files, if the filesystem usage is above 
# a certain threshold (example: if usage is above 90%, 
# delete 12 files on the /data partition)
#
# Dependencies include BOREALISPATH being in the environment variables in $HOME/.profile
# and pydarn being installed in a virtualenv at $HOME/pydarn-env
#
# The script should be run via crontab like so:
# 32 5,17 * * * . $HOME/.profile; $HOME/data_flow/borealis/rotate_borealis_files.sh >> $HOME/rotate_borealis_files.log 2>&1


# What filesystem are we interested in?
FILESYSTEM=`cat ${BOREALISPATH}/config.ini | jq -r '.data_directory'` 
# Where should files be moved to?
ARCHIVE_LOCATION=/borealis_nfs/archive/
CONV_FAIL_LOCATION=${ARCHIVE_LOCATION}/fail/
# Delete files if filesystem usage is over this threshold
CAPACITY_LIMIT=90
# How many files should be deleted at a time in the loop?
DELETE_X_FILES=12
# What file pattern should be deleted?
FILE_PATTERN_TO_DELETE=*hdf5*

# Date, time and other stuff
DATE=`date +%Y%m%d`
DATE_UTC=`date -u`
CURYEAR=`date +%Y`
CURMONTH=`date +%m`
HOSTNAME=`hostname`

# What directory should be used for logging?
LOGGINGDIR=/home/radar/logs/file_rotations/${CURYEAR}/${CURMONTH}
mkdir -p ${LOGGINGDIR}
LOGFILE=${LOGGINGDIR}/${DATE}.log
EMAILFLAG=0
EMAILBODY=
EMAILSUBJECT="File Rotations ${HOSTNAME} borealis: [${DATE}]"

##############################################################################
# Email function. Called before any abnormal exit, or at the end of the
# script if the email flag was set.
# Argument 1 should be the subject
# Argument 2 should be the body
##############################################################################
send_email () {
        # Argument 1 should be the subject
        # Argument 2 should be the body
        # What email address to send to?
        EMAILADDRESS=kevin.krieger@usask.ca
        echo -e "${2}" | mutt -s "${1}" ${EMAILADDRESS}
}

##############################################################################
# Do some error checking on the arguments
##############################################################################
# Echo the date for logging purposes
echo "" >> ${LOGFILE} 2>&1
echo ${DATE_UTC} >> ${LOGFILE} 2>&1
echo "Checking arguments..." >> ${LOGFILE} 2>&1
# Check to make sure the local holding directory exists
if [ ! -d "${FILESYSTEM}" ];
then
        EMAILBODY="Error: Input directory ${FILESYSTEM} doesn't exist! Exiting\n"
        EMAILSUBJECT="${EMAILSUBJECT} Input directory error"
        echo -e ${EMAILBODY} >> ${LOGFILE} 2>&1
        send_email "${EMAILSUBJECT}" "${EMAILBODY}"
        exit
fi

# Check to see that the rotations directory exist
if [ ! -d "${LOGGINGDIR}" ];
then
        EMAILBODY="Error: Logging directory ${LOGGINGDIR} doesn't exist! Exiting\n"
        EMAILSUBJECT="${EMAILSUBJECT} Logging directory error"
        echo -e ${EMAILBODY} >> ${LOGFILE} 2>&1
        send_email "${EMAILSUBJECT}" "${EMAILBODY}"
        exit
fi

##############################################################################
# First convert all antennas_iq site files from previous days (excluding today)
# array restructured to save space before checking for the files to delete.
##############################################################################

# What file pattern should be converted?
FILE_PATTERN_TO_CONVERT=*antennas_iq.hdf5.site

echo "Restructuring all antennas_iq except from today ${DATE}" >> ${LOGFILE} 2>&1

# find all site files, remove the files from today from the list and reverse the list order
# so that we convert the most recent files first (older files may get removed right away anyway)
CONVERT_FILES=`find "${FILESYSTEM}" -name "${FILE_PATTERN_TO_CONVERT}" -type f | grep -v "${DATE}" | tac`
source ${HOME}/pydarn-env/bin/activate

MAX_FILES_TO_CONVERT=24
converted_files_count=0

for f in ${CONVERT_FILES}
do
    if [[ $converted_files_count -gt ${MAX_FILES_TO_CONVERT} ]]
    then
        echo "Converted MAX NUMBER OF FILES (${MAX_FILES_TO_CONVERT})" >> ${LOGFILE} 2>&1
        break
    fi

    convert_cmd="python3 ${HOME}/data_flow/site-linux/borealis_convert_file.py ${f}"
    echo ${convert_cmd} >> ${LOGFILE} 2>&1
    ${convert_cmd} >> ${LOGFILE} 2>&1
    ret=$?
    if [ $ret -eq 0 ]; then
	# In case it succeeds in conversion, then remove the .site file
	rm -v ${f} >> ${LOGFILE} 2>&1
    else
	# In case it fails to convert, move the .site file to the archive fail location
	mv -v ${f} ${CONV_FAIL_LOCATION} >> ${LOGFILE} 2>&1
    fi
    converted_files_count=$((converted_files_count+1))
done

##############################################################################
# Proceed if filesystem capacity is over the value of CAPACITY (using 
# df POSIX syntax).
##############################################################################

# If each loop is over capacity, how many times should we loop and 
# delete files before exiting? And a variable to count loops
MAX_LOOPS=5
safety_count=0

while true
do
    if [[ $safety_count -gt ${MAX_LOOPS} ]]
    then
        EMAILFLAG=1
        EMAILBODY="${EMAILBODY}\nReached maximum (${MAX_LOOPS}) number of loops, exiting!"
        EMAILSUBJECT="${EMAILSUBJECT} Max loops reached"
        break
    fi
    safety_count=$((safety_count+1))

    CAP=`df -P ${FILESYSTEM} | awk '{ gsub("%",""); capacity = $5 }; END { print capacity }'` 2>> ${LOGFILE}
    echo "${FILESYSTEM} utilization: ${CAP}% of ${CAPACITY_LIMIT}% limit" >> ${LOGFILE} 2>&1
    if [ $CAP -gt $CAPACITY_LIMIT ]
    then
        EMAILFLAG=1
        # Find the oldest files for deletion 
        DEL_FILES=`find "${FILESYSTEM}" -name "${FILE_PATTERN_TO_DELETE}" -type f -printf '%T+ %p\n' | sort | head -n "${DELETE_X_FILES}" | awk '{print $2}'` 2>> ${LOGFILE}
        if [[ "${DEL_FILES}" =~ ^\ +$ ]]
        then
            echo "DEL FILES is just whitespace, breaking"
            break
        elif [[ "${DEL_FILES}" == "" ]]
        then
            echo "DEL FILES is null, breaking"
            break
        fi
        
        echo "${DEL_FILES}" >>  ${LOGFILE} 2>&1
        
        for f in ${DEL_FILES}
        do
            mv -v ${f} ${ARCHIVE_LOCATION}  >> ${LOGFILE} 2>&1
        done
        EMAILBODY="${EMAILBODY}\nFiles moved:\n${DEL_FILES}"
    else
        # Not above the threshold, so break and do nothing.
        break
    fi
done

if [[ ${EMAILFLAG} -eq 1 ]];
then
        echo "Sending email..." >> ${LOGFILE} 2>&1
        send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi

exit
