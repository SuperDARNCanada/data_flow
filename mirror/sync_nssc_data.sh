#!/bin/bash
# Last modification 20231201 by Saif Marei
# Modified paths for raw hashes and blocklisted/failed files on Cedar
#
# Modified from auto_make_fitacf_all_globus 
#
# This script is designed to log on to the NSSC  
# SuperDARN mirror in order to sync to the usask mirror
#
# Call the script like so with the following arguments:
# /path/to/script/sync_from_nssc /path/to/holding/directory YYYYMM
# Argument 1 is the data holding directory.
# Argument 2 is Year and Month (YYYYMM) to get data for, default current YYYYMM
# 
# The script will check the arguments and if there are errors with the
# arguments then it will fail with an email sent out.

# The logic is as follows:
# 1) Download the hashes files from the usask mirror for the specified dates
#        also download the blocklist and previously failed files
# 2) Compare with nssc versions of hashes files
# 3) Find out which data files have been updated or added
# 4) Remove any blocked files from the files to download 
# 5) Remove any previously failed files from the files to download
# 6) Download remaining files not on usask, blocked or previously failed
#
#  Emails are sent to indicate files that are different, files that are 
# missing on NSSC, files that are blocked or files that failed checks
#
##############################################################################
# Initialize some variables
##############################################################################
# What is the local holding directory to store data?
HOLDINGDIR=$1
# What year and month are we getting data for?
YYYYMM=$2

# EDIT 2023-04-28 TK. Makes file names specify which mirror they are for
#YYYYMM="${YYYYMM}_nssc"

# Variables for Cedar user and paths
CEDAR_USER=saifm@robot.cedar.alliancecan.ca
CEDAR_HASHES=/home/saifm/projects/rrg-kam136-ad/sdarn/chroot/sddata/raw/
CEDAR_BLOCKLIST=/home/saifm/projects/rrg-kam136-ad/sdarn/chroot/sddata/.config/blocklist/
CEDAR_FAILED=/home/saifm/projects/rrg-kam136-ad/sdarn/chroot/sddata/.config/all_failed.txt

# What hash program are we using?
HASHPROG=/usr/bin/sha1sum
# Date, time and other stuff
STARTTIME=`date +%s`
DATE_TIME=`date +%Y%m%d.%H%M`
DATE_UTC=`date -u`
CURDAY=`date +%d`
CURHOUR=`date +%H`
CURMIN=`date +%M`
CURYEAR=`date +%Y`
CURMONTHNAME=`date +%B`
CURMONTH=`date +%m`
MONTHNAME=`date --date=${CURYEAR}${CURMONTH}01 +%B`
# What is our logging file directory?
LOGGINGDIR=/home/dataman/logs/nssc/${CURYEAR}/${CURMONTH}/
# What is our log file name?
LOGFILE=${LOGGINGDIR}${DATE_TIME}_nssc.log
# Make the log file directory if it doesn't exist
mkdir -p ${LOGGINGDIR}
# Check that year and month were supplied or not
if [[ "" == ${YYYYMM} ]]
then
	YYYYMM=${CURYEAR}${CURMONTH}
fi
# Should we be sending an email?
EMAILFLAG=0
# What is the email body?
EMAILBODY=
# What is the email Subject?
EMAILSUBJECT="sync_nssc_data - [${YYYYMM}]"
# What sftp program are we using?
SFTP=/usr/bin/sftp
# What is the script to get hashes files?
GLOBUS_GET_HASHES=/home/dataman/globus_mirror_scripts/get_hashes_globus.py
# What is the script to get the blocked files?
GLOBUS_GET_BLOCKLIST=/home/dataman/globus_mirror_scripts/get_blocklist_globus.py
# What is the script to get the previously failed files?
GLOBUS_GET_FAILED=/home/dataman/globus_mirror_scripts/get_failed_globus.py
# What is the globus mirror directory root? 
GLOBUS_MIRROR_ROOT="~/chroot/sddata"
# What is the username to connect with on the sftp server?
USER=dataman
# What is the hostname to connect to (the sftp server)?
REMOTEHOST=superdarn.mirror.nssdc.ac.cn
# What is the nssc directory name on the sftp server?
#REMOTEDIRBASE=/data/ftproot/raw
REMOTEDIRBASE=/sddata/raw
# What is the full directory name on the sftp server 
REMOTEDIR=${REMOTEDIRBASE}
# What is our holding directory for hashes files?
LOCALHASHDIR=/home/dataman/tmp_hashes_usask_nssc_cmp/$DATE_TIME
NSSCHASHDIR=/home/dataman/tmp_hashes_nssc/$DATE_TIME
# What is our holding directory for blocked files?
LOCALBLDIR=/home/dataman/tmp_blocklist/$DATE_TIME
# What is our holding directory for previously failed files?
LOCALFAILEDDIR=/home/dataman/tmp_failed/$DATE_TIME
# Make sure they exist
mkdir -p ${LOCALHASHDIR}
mkdir -p ${NSSCHASHDIR}
mkdir -p ${LOCALBLDIR}
mkdir -p ${LOCALFAILEDDIR}

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
        EMAILADDRESS="saif.marei@usask.ca"
        echo -e "${2}" | /usr/bin/mutt -s "${1}" ${EMAILADDRESS}
}

##############################################################################
# Do some error checking on the arguments 
##############################################################################
# Echo the date for logging purposes
echo "" >> ${LOGFILE}
echo ${DATE_UTC} >> ${LOGFILE}
echo "Checking arguments..." >> ${LOGFILE}
# Check the local holding directory
if [ ! -d "${HOLDINGDIR}" ];
then
	EMAILBODY="Error: Holding directory: ${HOLDINGDIR} invalid. Exiting\n"
	EMAILSUBJECT="${EMAILSUBJECT} holding directory error"
 	echo -e ${EMAILBODY} >> ${LOGFILE}
        send_email "${EMAILSUBJECT}" "${EMAILBODY}"
        exit
fi

#############################################################################
# Execute step 1) Download the hashes file(s) from the servers
# as well as the blocklist and previously failed files
##############################################################################
# First we need to rename any older hashes files in this directory, since we
# may have run this script already this day
for file in `ls -1 ${LOCALHASHDIR}/*hashes 2>/dev/null`
do
	mv -v ${file} ${file}.old >> ${LOGFILE} 2>&1 
done
for file in `ls -1 ${NSSCHASHDIR}/*hashes 2>/dev/null`
do
	mv -v ${file} ${file}.old >> ${LOGFILE} 2>&1
done
#for file in `ls -1 ${LOCALBLDIR}/*txt`
#do
#	mv -v ${file} ${file}.old >> ${LOGFILE} 2>&1 
#done
#for file in `ls -1 ${LOCALFAILEDDIR}/*txt`
#do
#	mv -v ${file} ${file}.old >> ${LOGFILE} 2>&1 
#done

yyyy=`echo $YYYYMM | cut -b1-4`
mm=`echo $YYYYMM | cut -b5-6`
rsync -avL --timeout=15 -e "ssh -i ~/.ssh/id_rsa" ${CEDAR_USER}:${CEDAR_HASHES}/$yyyy/$mm/$yyyy$mm.hashes ${LOCALHASHDIR}/ >> ${LOGFILE} 2>&1
RETURN_VALUE=$?
echo "rsync get hashes returned: ${RETURN_VALUE}" >> ${LOGFILE}
if [[ ${RETURN_VALUE} -ne 0 ]]
then
        echo "Rsync retrieval of hashes failed" >> ${LOGFILE}
        EMAILBODY="Error: Could not download hash file. Exiting\n"
	EMAILSUBJECT="${EMAILSUBJECT} hashes error"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT}" "${EMAILBODY}"
	exit

fi

rsync -avL --timeout=20 -e "ssh -i ~/.ssh/id_rsa" ${CEDAR_USER}:${CEDAR_BLOCKLIST} ${LOCALBLDIR}/ >> ${LOGFILE} 2>&1
RETURN_VALUE=$?
echo "rsync get blocklist returned: ${RETURN_VALUE}" >> ${LOGFILE}
if [[ ${RETURN_VALUE} -ne 0 ]]
then
	echo "Rsync retrieval of blocklist failed" >> ${LOGFILE}
	EMAILBODY="Error: Could not download blocklist. Exiting\n"
	EMAILSUBJECT="${EMAILSUBJECT} blocklist error"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT}" "${EMAILBODY}"
	exit
fi

rsync -avL --timeout=15 -e "ssh -i ~/.ssh/id_rsa" ${CEDAR_USER}:${CEDAR_FAILED} ${LOCALFAILEDDIR}/ >> ${LOGFILE} 2>&1
RETURN_VALUE=$?
echo "rsync get failed returned: ${RETURN_VALUE}" >> ${LOGFILE}
if [[ ${RETURN_VALUE} -ne 0 ]]
then
	echo "Rsync retrieval of previously failed files failed" >> ${LOGFILE}
	EMAILBODY="Error: Could not download failed file list. Exiting\n"
	EMAILSUBJECT="${EMAILSUBJECT} hashes error"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT}" "${EMAILBODY}"
	exit
fi

SFTPBATCH=${NSSCHASHDIR}/sftp.batch
SFTPERRORS=${NSSCHASHDIR}/sftp.errors
echo -n > ${SFTPBATCH}
cd ${NSSCHASHDIR}
echo "-get ${REMOTEDIR}/${yyyy}/${mm}/${yyyy}${mm}.hashes ${NSSCHASHDIR}" >> ${SFTPBATCH}
echo "exit" >> ${SFTPBATCH}
${SFTP} -b ${SFTPBATCH} ${USER}@${REMOTEHOST} >> ${LOGFILE} 2> ${SFTPERRORS} 

if [[ -s $SFTPERRORS ]]
then
	ERRORS_STRING=`cat $SFTPERRORS`
	EMAILBODY="${EMAILBODY}\nSFTP errors\n$ERRORS_STRING"
	EMAILSUBJECT="${EMAILSUBJECT} ERROR"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi

if [ ! -e "${NSSCHASHDIR}/${yyyy}${mm}.hashes" ];
then
	EMAILBODY="Error: NSSC hash file error. Exiting\n"
	EMAILSUBJECT="${EMAILSUBJECT} NSSC hash file error"
 	echo -e ${EMAILBODY} >> ${LOGFILE}
        send_email "${EMAILSUBJECT}" "${EMAILBODY}"
        exit
fi

HASHTIMEEND=`date +%s`
##############################################################################
# Sort the files, because otherwise we can't properly diff them :/
##############################################################################
localhash=${LOCALHASHDIR}/${yyyy}${mm}.hashes
nsschash=${NSSCHASHDIR}/${yyyy}${mm}.hashes
localsorted=${localhash}.usask.sorted
nsscsorted=${nsschash}.nssc.sorted
sort -k2  ${localhash} > ${localsorted}
sort -k2 ${nsschash} > ${nsscsorted}

##############################################################################
# 2) Go through the hashes file, comparing with our current copy to see
# which data files we need to download. 
##############################################################################
PREFIX=${LOCALHASHDIR}/${YYYYMM}_nssc_data.
UPDATED_NSSC=${PREFIX}updated_nssc
UPDATED_USASK=${PREFIX}updated_usask
DIFFERENT_FILES=${PREFIX}different
NOT_AT_USASK=${PREFIX}not_at_usask
HASHES_NOT_AT_USASK=${PREFIX}hashes_not_at_usask
NOT_AT_NSSC=${PREFIX}not_at_nssc
BLOCKED_FILES=${PREFIX}blocked
FAILED_HASHES=${PREFIX}failed_hashes
FAILED_FILES=${PREFIX}failed
TO_DOWNLOAD=${PREFIX}to_download
FAILED_UNIQ=${PREFIX}failed_uniq

# This will contain files that are either different, or exist on the nssc mirror but not usask
diff -n ${localsorted} ${nsscsorted} | grep -a rawacf > ${UPDATED_NSSC} 2>> ${LOGFILE}
# This will contain files that are either different, or exist on the usask mirror but not nssc
diff -n ${nsscsorted} ${localsorted} | grep -a rawacf > ${UPDATED_USASK} 2>> ${LOGFILE}
# This will contain files that are different but exist at both sites. It contains the NSSC versions (NSSC's hashes)
cat ${UPDATED_NSSC} ${UPDATED_USASK} | sort -s -k 2,2 | uniq -d -f1 > ${DIFFERENT_FILES} 2>> ${LOGFILE}
# This will contain files that nssc has, but usask doesn't **** NO HASHES IN THE LIST ****
cat ${UPDATED_NSSC} ${DIFFERENT_FILES} | sort -s -k 2,2 | uniq -u -f1 | awk -F' ' '{print $2}' > ${NOT_AT_USASK} 2>> ${LOGFILE}
# This will contain files that nssc has, but usask doesn't 
cat ${UPDATED_NSSC} ${DIFFERENT_FILES} | sort -s -k 2,2 | uniq -u -f1 > ${HASHES_NOT_AT_USASK} 2>> ${LOGFILE}
# This will contain files that usask has, but nssc doesn't
cat ${UPDATED_USASK} ${DIFFERENT_FILES} | sort -s -k 2,2 | uniq -u -f1 > ${NOT_AT_NSSC} 2>> ${LOGFILE}
# This will contain files that nssc has, that usask doesn't, that are blocked ** NO HASHES IN THE LIST **
cat ${LOCALBLDIR}/*.txt | sort | uniq > ${BLOCKED_FILES}.tmp 2>> ${LOGFILE}
cat ${NOT_AT_USASK} ${BLOCKED_FILES}.tmp | sort | uniq -d > ${BLOCKED_FILES} 2>> ${LOGFILE}
# This will contain files that nssc has, that usask doesn't, or that are different, that have failed dmap, bzip2, etc checks
# First, get unique set of failed files in the failed files list. 
# The -w70 flag is to only compare the first 70 characters of the hashes_not_at_usask and failed_uniq file 
# concatenation, since the failed_uniq file also contains the error message. The sha1sum is 160 bits or 40 hex 
# characters long, then  there are two spaces and then the filename which is either 31 or 33 characters long. 
# It's enough to compare up to the 70th character which would be into the 'rawacf.bz2' part of the filename.
cat ${LOCALFAILEDDIR}/*txt | sort -k2 | uniq > ${FAILED_UNIQ} 2>> ${LOGFILE}
cat ${UPDATED_NSSC} ${FAILED_UNIQ} | sort -k2 | uniq -d -w70  > ${FAILED_HASHES} 2>> ${LOGFILE}
# This will contain the failed files without hashes
cat ${FAILED_HASHES} | awk -F' ' '{print $2}' > ${FAILED_FILES} 2>> ${LOGFILE}
# This will contain files that nssc has, that usask doesn't, that are not blocked or failed ** NO HASHES IN THE LIST **
cat ${NOT_AT_USASK} ${BLOCKED_FILES} ${FAILED_FILES} | sort | uniq -u > ${TO_DOWNLOAD} 2>> ${LOGFILE}

totalUpdated=`wc -l ${NOT_AT_USASK} | awk -F' ' '{print $1}'`
totalMissing_all=`wc -l ${NOT_AT_NSSC} | awk -F' ' '{print $1}'`
totalDifferent=`wc -l ${DIFFERENT_FILES} | awk -F' ' '{print $1}'`
totalBlocked=`wc -l ${BLOCKED_FILES} | awk -F' ' '{print $1}'`
totalFailed=`wc -l ${FAILED_FILES} | awk -F' ' '{print $1}'`
totalToDownload=`wc -l ${TO_DOWNLOAD} | awk -F' ' '{print $1}'`

day_threshold=3
totalOldMissingFiles=0

if [[ ${totalMissing_all} -gt 0 ]] 
then
	missing_files_string=""
	DEFAULT_IFS=${IFS}
	IFS=$'\n'
	for f in $(cat ${NOT_AT_NSSC})
	do 
		file_date=`echo $f | awk -F' ' '{print $2}' | awk -F'.' '{print $1}'`
		date_threshold=`date --date="$day_threshold days ago" +%Y%m%d`
		if [[ $date_threshold -ge $file_date ]]
		then
			missing_files_string="$missing_files_string\n$f"
			totalOldMissingFiles=`expr ${totalOldMissingFiles} + 1`
		fi
	done
	IFS=${DEFAULT_IFS}
	if [[ ${totalOldMissingFiles} -gt 0 ]]
	then
		EMAILBODY="NSSC is missing files older than $day_threshold days:\n${missing_files_string}"
		EMAILSUBJECT2="${EMAILSUBJECT} nssc missing files older than $day_threshold days"
		echo -e ${EMAILBODY} >> ${LOGFILE}
		send_email "${EMAILSUBJECT2}" "${EMAILBODY}"
	fi
fi
if [[ ${totalDifferent} -gt 0 ]]
then
	different_files_string=`cat ${DIFFERENT_FILES}`
	EMAILBODY="NSSC has different files:\n${different_files_string}"
	EMAILSUBJECT3="${EMAILSUBJECT} nssc different files"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT3}" "${EMAILBODY}"
fi
if [[ ${totalBlocked} -gt 0 ]]
then
	blocked_files_string=`cat ${BLOCKED_FILES}`
	EMAILBODY="NSSC has blocked files:\n${blocked_files_string}"
	EMAILSUBJECT4="${EMAILSUBJECT} nssc blocked files"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT4}" "${EMAILBODY}"
fi
if [[ ${totalFailed} -gt 0 ]]
then
	failed_files_string=`cat ${FAILED_HASHES}`
	EMAILBODY="NSSC has failed files:\n${failed_files_string}"
	EMAILSUBJECT5="${EMAILSUBJECT} nssc failed files"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT5}" "${EMAILBODY}"
fi
if [[ ${totalToDownload} -eq 0 ]] 
then
	EMAILBODY="No files to download. Exiting\n"
	EMAILSUBJECT="${EMAILSUBJECT} no files to download"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	#send_email "${EMAILSUBJECT}" "${EMAILBODY}"
	exit
fi

##############################################################################
# 3) Now that we have the files required to download, let's put together
# an sftp batch command file, ".batch" and an rsync one
##############################################################################
SFTPBATCH=${HOLDINGDIR}/sftp.batch
RSYNCBATCH=${HOLDINGDIR}/rsync_${yyyy}${mm}.batch
echo "Building sftp/rsync batch files to download files..." >> ${LOGFILE}
# Empty any previous batch file first.
echo -n > ${SFTPBATCH}
echo -n > ${RSYNCBATCH}
# We need to place get commands for each file required
totalFiles=0
for ITEM in `cat ${TO_DOWNLOAD}`
do
	echo "-get ${REMOTEDIR}/${yyyy}/${mm}/${ITEM} ${HOLDINGDIR}" >> ${SFTPBATCH}
	echo "${REMOTEDIR}/${yyyy}/${mm}/${ITEM}" >> ${RSYNCBATCH}
	totalFiles=`expr ${totalFiles} + 1`
done
# Finally, we exit from sftp
echo "exit" >> ${SFTPBATCH}
echo "Total new files: ${totalFiles}" >> ${LOGFILE}


##############################################################################
# Finally, we are ready to execute the sftp batch file and download the
# required files. Redirect errors to a file ".errors". Do not execute
# if there were zero files that required downloading.
##############################################################################
SFTPERRORS=${HOLDINGDIR}/sftp.errors
echo -n > $SFTPERRORS
if [[ ${totalFiles} -gt 0 ]]
then
	echo "Getting files..." >> ${LOGFILE}
	${SFTP} -b ${SFTPBATCH} ${USER}@${REMOTEHOST} >> ${LOGFILE} 2> ${SFTPERRORS} 
else
	EMAILBODY="${EMAILBODY}\n NO NEW FILES - YOU SHOULD NEVER SEE THIS"
	EMAILSUBJECT="${EMAILSUBJECT} ERROR"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT}" "${EMAILBODY}"
	exit
fi
if [[ -s $SFTPERRORS ]]
then
	ERRORS_STRING=`cat $SFTPERRORS`
	EMAILBODY="${EMAILBODY}\nSFTP errors\n$ERRORS_STRING"
	EMAILSUBJECT="${EMAILSUBJECT} ERROR"
	echo -e ${EMAILBODY} >> ${LOGFILE}
	send_email "${EMAILSUBJECT}" "${EMAILBODY}"
fi
SYNCTIMEEND=`date +%s`

##############################################################################
# Cleanup and information 
##############################################################################
ENDTIME=`date +%s`
HASHSYNCTIME=`expr ${HASHTIMEEND} - ${STARTTIME}`
SYNCTIME=`expr ${SYNCTIMEEND} - ${HASHTIMEEND}`
TOTALTIME=`expr ${ENDTIME} - ${STARTTIME}`

echo "Time to download hashes files: 	${HASHSYNCTIME} seconds" >> ${LOGFILE}
echo "Time to download ${totalFiles} rawacf files: 	${SYNCTIME} seconds" >> ${LOGFILE}
echo "Total time to execute script : 	${TOTALTIME} seconds" >> ${LOGFILE}


exit

