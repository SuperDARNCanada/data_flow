#!/bin/bash
# Copyright 2019 SuperDARN Canada, University of Saskatchewan
# Author: Kevin Krieger

# A singleton script to copy the data FILES to the on-site linux computer, removing them locally after
# a successful copy. Dependencies include 'jq' (installed via zypper), 
# as well as BOREALISPATH and SITE_LINUX being in the environment variables in $HOME/.profile
#
# The script should be run via crontab like so:
# 8,45 */2 * * * . $HOME/.profile; $HOME/data_flow/rsync_to_linux.sh >> $HOME/rsync_to_linux.log 2>&1

# What is this script called?
SCRIPT_NAME=`basename "$0"`
# Linux computer's user and hostname/IP address as well as the destination directory
LINUX=${SITE_LINUX}
DEST=/data/daily/
# Use jq with the raw output option and the Borealis directory to get the source for the data
SOURCE=`cat ${BOREALISPATH}/config.ini | jq -r '.data_directory'`
# The following constant is how many minutes threshold the script will use to find FILES
# to move to the site linux computer. This is so that the script doesn't try to move the current data
# file being written to.
CUR_FILE_THRESHOLD_MINUTES=5
# A temp directory for rsync to use in case rsync is killed, it will start up where it left off
TEMPDEST=.rsync_partial
# Where should the md5sum file live to verify the rsync transfer?
MD5=/tmp/md5

echo ${SCRIPT_NAME}

# Date in UTC format for logging
date -u
echo "Placing FILES in ${LINUX}:${DEST}"

# Check to make sure this is the only instance running.
# If only this one is running, will be ${HOME}/${SCRIPT_NAME} ${HOME}/${SCRIPT_NAME}
RSYNCRUNNING="`ps aux | grep rsync_to_linux.sh | awk '$11 !~ /grep/ {print $12}'`" 

#must be three times because the first two will be this instance of ${SCRIPT_NAME}
if [[ "$RSYNCRUNNING" == *"${HOME}/${SCRIPT_NAME}"*"${HOME}${SCRIPT_NAME}"*"${HOME}${SCRIPT_NAME}"* ]]
then 
	echo "Another instance running, exiting"
	exit
fi

FILES=`find ${SOURCE} \( -name '*rawacf.hdf5.site' -o -name '*bfiq.hdf5.*' \) -cmin +${CUR_FILE_THRESHOLD_MINUTES} -printf '%p\n'`
echo $FILES
for file in $FILES
do
	# Get the file name, directory it's in and rsync it
	datafile=`basename $file`	
	path=`dirname $file`
	cd $path
	rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${datafile} ${LINUX}:${DEST}
        
	# check if transfer was okay using the md5sum program, then remove the file if it matches
	ssh ${LINUX} "cd ${DEST}; md5sum -b ${datafile}" > ${MD5}
	md5sum -c ${MD5}
	mdstat=$?
	if [ ${mdstat} -eq 0 ]
       	then
		echo "Deleting file: ${file}"
		rm -v ${file}
	fi
done

exit
