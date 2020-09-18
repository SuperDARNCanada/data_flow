#!/bin/bash
# rsync_to_sas
# Author: Dieter Andre
# Modification: August 6th 2019 
# Simplified and changed to a loop over all files instead of acting on all files at once.
# Modification: November 22 2019
# Removed SDCOPY details from file
#
# Should be called from crontab like so:
# */5 * * * * ${HOME}/data_flow/site-linux/rsync_to_sas.sh >> ${HOME}/rsync_to_sas.log 2>&1
#
#

source /home/radar/.bashrc # source the RADARID, SDCOPY and other things
HOME=/home/radar/
DMAP_SOURCE=/borealis_nfs/borealis_data/rawacf_dmap/
ARRAY_SOURCE=/borealis_nfs/borealis_data/rawacf_array/
DEST=/data/${RADARID}_data/

echo ""
date
echo "Placing files in sdcopy.usask.ca:$DEST"

TEMPDEST=.rsync_partial
MD5=${HOME}md5

# do not start while files are converted and transferred from /data/daily
if [ -a ${HOME}convert_daily_borealis_running ] ; then
  exit
fi

RSYNCRUNNING="`ps aux | grep rsync_to_sas | awk '$11 !~ /grep/ {print $12}'`" #if only this one running, will be /home/transfer/rsync_to_sas /home/transfer/rsync_to_sas

if [[ "$RSYNCRUNNING" == *"data_flow/site-linux/rsync_to_sas"*"data_flow/site-linux/rsync_to_sas"*"data_flow/site-linux/rsync_to_sas"* ]] ; then #must be three times because the first two will be this instance of rsync_to_sas
  exit
fi

files=`find ${DMAP_SOURCE} -name '*rawacf.bz2' -printf '%p\n'`
for file in $files
do
        datafile=`basename $file`
        path=`dirname $file`
        cd $path
        rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${datafile} ${SDCOPY}:${DEST}
        # check if transfer was okay using the md5sum program
        ssh ${SDCOPY} "cd ${DEST}; md5sum -b ${datafile}" > ${MD5}
        md5sum -c ${MD5}
        mdstat=$?
        if [ ${mdstat} -eq 0 ] ; then
                echo "Deleting file: "${file}
                rm -v ${file}
        fi
done

files=`find ${ARRAY_SOURCE} -name '*rawacf.hdf5' -printf '%p\n'`
for file in $files
do
        datafile=`basename $file`
        path=`dirname $file`
        cd $path
        rsync -av --partial --partial-dir=${TEMPDEST} --timeout=180 --rsh=ssh ${datafile} ${SDCOPY}:${DEST}
        # check if transfer was okay using the md5sum program
        ssh ${SDCOPY} "cd ${DEST}; md5sum -b ${datafile}" > ${MD5}
        md5sum -c ${MD5}
        mdstat=$?
        if [ ${mdstat} -eq 0 ] ; then
                echo "Deleting file: "${file}
                rm -v ${file}
        fi
done

exit
