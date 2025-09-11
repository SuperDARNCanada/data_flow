#!/bin/bash

# This script is designed to log on to the BAS/NSSC
# SuperDARN mirror in order to sync to the usask mirror
#
# Call the script like so with the following arguments:
# /path/to/script/sync_from_mirror /path/to/holding/directory MIRROR YYYYMM
# Argument 1: Holding directory. (/data/holding/BAS or /data/holding/NSSC)
# Argument 2: Specified mirror (BAS or NSSC)
# Argument 3: Year and Month (YYYYMM) to get data for, default current YYYYMM
#
# The script will check the arguments and if there are errors with the
# arguments then it will fail with an email sent out.
#
# Logical flow of the script
# 1) Download the hashes files from the usask mirror for the specified dates
#        also download the blocklist and previously failed files
# 2) Compare with bas/nssc versions of hashes files
# 3) Find out which data files have been updated or added
# 4) Remove any blocked files from the files to download
# 5) Remove any previously failed files from the files to download
# 6) Download remaining files not on usask, blocked or previously failed
#
# Emails are sent to indicate files that are different, files that are
# missing on BAS/NSSC, files that are blocked or files that failed checks
#

##############################################################################
# Initialize some variables
##############################################################################

# Valid HOLDINGDIR and MIRROR values
readonly VALID_DIRS=("/data/holding/BAS" "/data/holding/NSSC")  # Add /test/ for testing purposes
readonly VALID_MIRRORS=("BAS" "NSSC")

readonly NEW_NSSC_RADARS=("hje" "hjw" "lje" "ljw" "sze" "szw")

# Assign input arguments
HOLDINGDIR=$1
MIRROR=$2
YYYYMM=$3

# Check validity of input arguments
if [[ $# -ne 2 ]] && [[ $# -ne 3 ]]; then
    printf "Usage: sync_mirror_data.sh HOLDINGDIR MIRROR YYYYMM\n"
    exit 1
fi

if [[ ! " ${VALID_DIRS[*]} " =~ " ${HOLDINGDIR} " ]]; then
    printf "\"$HOLDINGDIR\" is not a valid holding directory\n"
    exit 1
fi

if [[ ! " ${VALID_MIRRORS[*]} " =~ " ${MIRROR} " ]]; then
    printf "\"$MIRROR\" is not a valid mirror\n"
    exit 1
fi

# Variables for Cedar user and paths
# readonly cedar_user=saifm@robot.cedar.alliancecan.ca
readonly cedar_user=rar129
readonly cedar_login="${cedar_user}@robot.fir.alliancecan.ca"

# Add _test to Cedar paths for testing purposes
#readonly CEDAR_HASHES=/home/saifm/projects/rrg-kam136-ad/sdarn/chroot/sddata/raw/
#readonly CEDAR_BLOCKLIST=/home/saifm/projects/rrg-kam136-ad/sdarn/chroot/sddata/.config/blocklist/
#readonly CEDAR_FAILED=/home/saifm/projects/rrg-kam136-ad/sdarn/chroot/sddata/.config/all_failed.txt
readonly CEDAR_HASHES="/home/${cedar_user}/projects/rrg-kam136-ad/sdarn/chroot/sddata/raw/"
readonly CEDAR_BLOCKLIST="/home/${cedar_user}/projects/rrg-kam136-ad/sdarn/chroot/sddata/.config/blocklist/"
readonly CEDAR_FAILED="/home/${cedar_user}/projects/rrg-kam136-ad/sdarn/chroot/sddata/.config/all_failed.txt"

# Date/time variables
STARTTIME=$(date +%s)
DATE_TIME=$(date +%Y%m%d.%H%M)
DATE_UTC=$(date -u)
CURYEAR=$(date +%Y)
CURMONTH=$(date +%m)

# Get lowercase MIRROR
mirror="${MIRROR,,}"

# Set username, hostname, and path for sftp usage given the provided mirror
if [[ "${MIRROR}" == "BAS" ]]
then
  USER=superdarn
  REMOTEHOST=bslsuperdarnb.nerc-bas.ac.uk
  REMOTEDIR=/data/superdarn/data/raw
elif [[ "${MIRROR}" == "NSSC" ]]
then
  USER=dataman
  REMOTEHOST=superdarn.mirror.nssdc.ac.cn
  REMOTEDIR=/sddata/raw
fi

# Create shortcuts for sha1sum and sftp programs
HASHPROG=/usr/bin/sha1sum
SFTP=/usr/bin/sftp

# Setup logfile
LOGDIR=/home/dataman/logs/${mirror}/${CURYEAR}/${CURMONTH}/  # Add _test for testing purposes
LOGFILE=${LOGDIR}${DATE_TIME}_${mirror}.log
# Make the log file directory if it doesn't exist
mkdir -p "${LOGDIR}"
# Redirect STDOUT and STDERR to $LOGFILE
exec &>> ${LOGFILE}
# Check if year and month were provided. If not, use current YYYYMM.
if [[ "" == "${YYYYMM}" ]]
then
  YYYYMM=${CURYEAR}${CURMONTH}
fi

EMAILBODY=
EMAILSUBJECT="sync_${mirror}_data - [${YYYYMM}]"
# What is our holding directory for hashes files?
# Double check that these directories are consistent between sync_mirror_data.sh and monthly batch_sync_mirror (for bas)
# Add /test_mirror/ in all 4 of the below paths for testing purposes
LOCALHASHDIR=/home/dataman/tmp_hashes_usask_${mirror}_cmp/$DATE_TIME
MIRRORHASHDIR=/home/dataman/tmp_hashes_${mirror}/$DATE_TIME
# What is our holding directory for blocked files?
LOCALBLDIR=/home/dataman/tmp_blocklist/$DATE_TIME
# What is our holding directory for previously failed files?
LOCALFAILEDDIR=/home/dataman/tmp_failed/$DATE_TIME
# Make sure above paths exist
mkdir -p "${LOCALHASHDIR}" "${MIRRORHASHDIR}" "${LOCALBLDIR}" "${LOCALFAILEDDIR}"

##############################################################################
# Email function. Called before any abnormal exit.
# Argument 1: email subject
# Argument 2: email body
##############################################################################
send_email () {
  # What email address to send to?
  EMAILADDRESS="superdarn_engineers@usask.ca"
  echo -e "${2}" | /usr/bin/mutt -s "${1}" ${EMAILADDRESS}
}

##############################################################################
# Do some error checking on the arguments
##############################################################################
echo -e "\n${DATE_UTC}\nChecking arguments..."
# Check the local holding directory
if [ ! -d "${HOLDINGDIR}" ];
then
  EMAILBODY="Error: Holding directory: ${HOLDINGDIR} invalid. Exiting\n"
  EMAILSUBJECT="${EMAILSUBJECT} holding directory error"
  echo -e "${EMAILBODY}"
  send_email "${EMAILSUBJECT}" "${EMAILBODY}"
  exit
fi

#############################################################################
# Step 1) Download the hashes file(s) from the servers
# as well as the blocklist and previously failed files
# If any downloads fail: log the error, send an email, and exit the script
##############################################################################
# First we need to rename any older hashes files in this directory, since we
# may have run this script already this day
for file in $(ls -1 ${LOCALHASHDIR}/*hashes 2>/dev/null)
do
  mv -v "${file}" "${file}.old"
done
for file in $(ls -1 ${MIRRORHASHDIR}/*hashes 2>/dev/null)
do
  mv -v "${file}" "${file}.old"
done

# Get year and month by parsing YYYYMM
yyyy=$(echo "${YYYYMM}" | cut -b1-4)
mm=$(echo "${YYYYMM}" | cut -b5-6)
# Download hashes from Cedar via rsync. Note the use of an ssh key and automated MFA through Digital Research Alliance.
rsync -avL --timeout=15 -e "ssh -i ~/.ssh/id_ed25519" ${cedar_login}:${CEDAR_HASHES}/"${yyyy}"/"${mm}"/"${YYYYMM}".hashes "${LOCALHASHDIR}"/
RETURN_VALUE=$?
echo "rsync get hashes returned: ${RETURN_VALUE}"
if [[ ${RETURN_VALUE} -ne 0 ]]
then
  echo "Rsync retrieval of hashes failed. Exiting."
  exit
fi
# Download blocklist directory from Cedar via rsync
rsync -avL --timeout=20 -e "ssh -i ~/.ssh/id_ed25519" ${cedar_login}:${CEDAR_BLOCKLIST} "${LOCALBLDIR}"/
RETURN_VALUE=$?
echo "rsync get blocklist returned: ${RETURN_VALUE}"
if [[ ${RETURN_VALUE} -ne 0 ]]
then
  echo "Rsync retrieval of blocklist failed. Exiting."
  exit
fi
# Download failed files list "all_failed.txt" from Cedar via rsync
rsync -avL --timeout=15 -e "ssh -i ~/.ssh/id_ed25519" ${cedar_login}:${CEDAR_FAILED} "${LOCALFAILEDDIR}"/
RETURN_VALUE=$?
echo "rsync get failed returned: ${RETURN_VALUE}"
if [[ ${RETURN_VALUE} -ne 0 ]]
then
  echo "Rsync retrieval of previously failed files failed. Exiting."
  exit
fi

# Create file to store sftp commands to be performed on mirror
SFTPBATCH=${MIRRORHASHDIR}/sftp.batch
# Create file to write any errors raised by sftp commands
SFTPERRORS=${MIRRORHASHDIR}/sftp.errors
# Ensure file is empty
echo -n > "${SFTPBATCH}"

cd "${MIRRORHASHDIR}" || exit
# Write commands to get hashes from mirror and exit to sftp batch file
echo "-get ${REMOTEDIR}/${yyyy}/${mm}/${YYYYMM}.hashes ${MIRRORHASHDIR}" >> "${SFTPBATCH}"
echo "exit" >> "${SFTPBATCH}"
# Execute sftp batch file, logging errors to sftp error file
# Add use of key for NSSC connection for migration
if [[ "${MIRROR}" == "BAS" ]]; then
  ${SFTP} -b "${SFTPBATCH}" ${USER}@${REMOTEHOST} >> "${LOGFILE}" 2> "${SFTPERRORS}"
elif [[ "${MIRROR}" == "NSSC" ]]; then
  ${SFTP} -i "~/.ssh/id_NSSC" -b "${SFTPBATCH}" ${USER}@${REMOTEHOST} >> "${LOGFILE}" 2> "${SFTPERRORS}"
fi

if [[ -s $SFTPERRORS ]]
then
  ERRORS_STRING=$(cat "${SFTPERRORS}")
  EMAILBODY="${EMAILBODY}\nSFTP errors\n$ERRORS_STRING"
  echo -e "${EMAILBODY}"
fi
# Check if hash file successfully transferred from server. If not: log, email, exit
if [ ! -e "${MIRRORHASHDIR}/${YYYYMM}.hashes" ]; then
  EMAILBODY="Error: ${MIRROR} hash file error. Exiting\n"
  echo -e "${EMAILBODY}"
  exit
fi

# Date in seconds after epoch for timing calculations at the end of the script
HASHTIMEEND=$(date +%s)
##############################################################################
# Sort the files, because otherwise we can't properly diff them :/
##############################################################################
localhash=${LOCALHASHDIR}/${YYYYMM}.hashes
mirrorhash=${MIRRORHASHDIR}/${YYYYMM}.hashes
localsorted=${localhash}.usask.sorted
mirrorsorted=${mirrorhash}.${mirror}.sorted
sort -k2  "${localhash}" > "${localsorted}"
sort -k2 "${mirrorhash}" > "${mirrorsorted}"

##############################################################################
# Step 2) Go through the hashes file, comparing with our current copy to see
# which data files we need to download.
##############################################################################
PREFIX=${LOCALHASHDIR}/${YYYYMM}_${mirror}_data.
UPDATED_MIRROR=${PREFIX}updated_${mirror}
UPDATED_USASK=${PREFIX}updated_usask
DIFFERENT_FILES=${PREFIX}different
NOT_AT_USASK=${PREFIX}not_at_usask
HASHES_NOT_AT_USASK=${PREFIX}hashes_not_at_usask
NOT_AT_MIRROR=${PREFIX}not_at_${mirror}
BLOCKED_FILES=${PREFIX}blocked
FAILED_HASHES=${PREFIX}failed_hashes
FAILED_FILES=${PREFIX}failed
TO_DOWNLOAD=${PREFIX}to_download
FAILED_UNIQ=${PREFIX}failed_uniq

# This will contain files that are either different, or exist on the external mirror but not usask
diff -n ${localsorted} ${mirrorsorted} | grep rawacf > ${UPDATED_MIRROR}
# This will contain files that are either different, or exist on the usask mirror but not ext mirror
diff -n ${mirrorsorted} ${localsorted} | grep rawacf > ${UPDATED_USASK}
# This will contain files that are different but exist at both sites
cat ${UPDATED_MIRROR} ${UPDATED_USASK} | sort -s -k2,2 | uniq -d -f1 > ${DIFFERENT_FILES}
# This will contain files that ext mirror has, but usask doesn't **** NO HASHES IN THE LIST ****
cat ${UPDATED_MIRROR} ${DIFFERENT_FILES} | sort -s -k2,2 | uniq -u -f1 | awk -F' ' '{print $2}' > ${NOT_AT_USASK}
# This will contain files that ext mirror has, but usask doesn't
cat ${UPDATED_MIRROR} ${DIFFERENT_FILES} | sort -s -k2,2 | uniq -u -f1 > ${HASHES_NOT_AT_USASK}
# This will contain files that usask has, but ext mirror doesn't
cat ${UPDATED_USASK} ${DIFFERENT_FILES} | sort -s -k2,2 | uniq -u -f1 > ${NOT_AT_MIRROR}
# This will contain files that ext mirror has, that usask doesn't, that are blocked ** NO HASHES IN THE LIST **
cat ${LOCALBLDIR}/*.txt | sort | uniq > ${BLOCKED_FILES}.tmp
cat ${NOT_AT_USASK} ${BLOCKED_FILES}.tmp | sort | uniq -d > ${BLOCKED_FILES}
# This will contain files that ext mirror has, that usask doesn't, or that are different, that have failed dmap, bzip2, etc checks
# First, get unique set of failed files in the failed files list.
# The -w70 flag is to only compare the first 70 characters of the hashes_not_at_usask and failed_uniq file
# concatenation, since the failed_uniq file also contains the error message. The sha1sum is 160 bits or 40 hex
# characters long, then  there are two spaces and then the filename which is either 31 or 33 characters long.
# It's enough to compare up to the 70th character which would be into the 'rawacf.bz2' part of the filename.
cat ${LOCALFAILEDDIR}/*txt | sort -k2 | uniq > ${FAILED_UNIQ}
cat ${UPDATED_MIRROR} ${FAILED_UNIQ} | sort -k2 | uniq -d -w70  > ${FAILED_HASHES}
# This will contain the failed files without hashes
cat ${FAILED_HASHES} | awk -F' ' '{print $2}' > ${FAILED_FILES}
# This will contain files that bas has, that usask doesn't, that are not blocked or failed ** NO HASHES IN THE LIST **
cat ${NOT_AT_USASK} ${BLOCKED_FILES} ${FAILED_FILES} | sort | uniq -u > ${TO_DOWNLOAD}

# Variables to store the number of rawacfs in each file defined above
# wc -l outputs # of lines and filepath separated by a space
totalUpdated=$(wc -l ${NOT_AT_USASK} | awk -F' ' '{print $1}')
totalMissing_all=$(wc -l ${NOT_AT_MIRROR} | awk -F' ' '{print $1}')
totalDifferent=$(wc -l ${DIFFERENT_FILES} | awk -F' ' '{print $1}')
totalBlocked=$(wc -l ${BLOCKED_FILES} | awk -F' ' '{print $1}')
totalFailed=$(wc -l ${FAILED_FILES} | awk -F' ' '{print $1}')
totalToDownload=$(wc -l ${TO_DOWNLOAD} | awk -F' ' '{print $1}')

# Only compare files within the last 3 days
day_threshold=3
totalOldMissingFiles=0

# Only execute this block if there is at least one file that mirror is missing
if [[ ${totalMissing_all} -gt 0 ]]
then
  missing_files_string=""
  # Store the default IFS value, then change IFS to separate strings by new line
  DEFAULT_IFS=${IFS}
  IFS=$'\n'
  # New IFS allows loop over [hash1 file1, hash2 file2, etc.] rather than [hash1, file1, hash2, file2, etc.]
  for f in $(cat ${NOT_AT_MIRROR})
  do
    # Parse date from filename (second entry in line), split filename by '.' and first entry is yyyymmdd
    file_date=$(echo "$f" | awk -F' ' '{print $2}' | awk -F'.' '{print $1}')
    date_threshold=$(date --date="$day_threshold days ago" +%Y%m%d)
    # Check if the missing file is more than 3 days old
    if [[ $date_threshold -ge $file_date ]]
    then
      # Append missing file [hash filename] to missing_files_string and increment missing files count
      missing_files_string="$missing_files_string\n$f"
      totalOldMissingFiles=$((totalOldMissingFiles + 1))
    fi
  done
  # Reset IFS back to default
  IFS=${DEFAULT_IFS}
  # Check if any of the missing files are more than 3 days old. If so, log and email
  if [[ ${totalOldMissingFiles} -gt 0 ]]
  then
    EMAILBODY="${MIRROR} is missing files older than $day_threshold days:\n${missing_files_string}"
    echo -e ${EMAILBODY}
  fi
fi
# Log and send an email if mirror has any different files
if [[ ${totalDifferent} -gt 0 ]]
then
  different_files_string=$(cat ${DIFFERENT_FILES})
  EMAILBODY="${MIRROR} has different files:\n${different_files_string}"
  EMAILSUBJECT3="${EMAILSUBJECT} ${mirror} different files"
  echo -e "${EMAILBODY}"
  send_email "${EMAILSUBJECT3}" "${EMAILBODY}"
fi
# Log and send an email if mirror has any blocked files
if [[ ${totalBlocked} -gt 0 ]]
then
  blocked_files_string=$(cat ${BLOCKED_FILES})
  EMAILBODY="${MIRROR} has blocked files:\n${blocked_files_string}"
  EMAILSUBJECT4="${EMAILSUBJECT} ${mirror} blocked files"
  echo -e "${EMAILBODY}"
  send_email "${EMAILSUBJECT4}" "${EMAILBODY}"
fi
# Log and send an email if mirror has any failed files
if [[ ${totalFailed} -gt 0 ]]
then
  failed_files_string=$(cat ${FAILED_HASHES})
  EMAILBODY="${MIRROR} has failed files:\n${failed_files_string}"
  EMAILSUBJECT5="${EMAILSUBJECT} ${mirror} failed files"
  echo -e "${EMAILBODY}"
  send_email "${EMAILSUBJECT5}" "${EMAILBODY}"
fi
# Log and exit script if there are no files to download from mirror
if [[ ${totalToDownload} -eq 0 ]]
then
  EMAILBODY="No files to download. Exiting\n"
  echo -e "${EMAILBODY}"
  exit
fi

##############################################################################
# Step 3) Now that we have the files required to download, let's put together
# an sftp batch command file, ".batch" and an rsync one
## NOTE: rsync batch is never executed
##############################################################################
SFTPBATCH=${HOLDINGDIR}/sftp.batch
RSYNCBATCH=${HOLDINGDIR}/rsync_${yyyy}${mm}.batch
echo "Building sftp/rsync batch files to download files..."
# Empty any previous batch file first.
echo -n > "${SFTPBATCH}"
echo -n > "${RSYNCBATCH}"
# We need to place 'get' commands for each file required
totalFiles=0
for ITEM in $(cat ${TO_DOWNLOAD})
do
  DEFAULT_IFS=${IFS}
  IFS='.'
  read -ra filename <<< "$ITEM"
  radar_id=${filename[3]}
  IFS=${DEFAULT_IFS}
  if [[ " ${NEW_NSSC_RADARS[*]} " =~ " ${radar_id} " ]]; then
    continue
  fi

  echo "-get ${REMOTEDIR}/${yyyy}/${mm}/${ITEM} ${HOLDINGDIR}" >> "${SFTPBATCH}"
  echo "${REMOTEDIR}/${yyyy}/${mm}/${ITEM}" >> "${RSYNCBATCH}"
  # Increment files to download counter
  totalFiles=$((totalFiles + 1))
done
# Finally, we exit from sftp
echo "exit" >> "${SFTPBATCH}"
echo "Total new files: ${totalFiles}"

##############################################################################
# Step 4) Finally, we are ready to execute the sftp batch file and download the
# required files. Redirect errors to a file ".errors". Do not execute
# if there were zero files that required downloading.
##############################################################################
SFTPERRORS=${HOLDINGDIR}/sftp.errors
echo -n > "$SFTPERRORS"
# Make sure there is at least one file in sftp batch before executing
if [[ ${totalFiles} -gt 0 ]]
then
  echo "Getting files..."
  # Add use of key for NSSC connection for migration
  if [[ "${MIRROR}" == "BAS" ]]; then
    ${SFTP} -b "${SFTPBATCH}" ${USER}@${REMOTEHOST} >> "${LOGFILE}" 2> "${SFTPERRORS}"
  elif [[ "${MIRROR}" == "NSSC" ]]; then
    ${SFTP} -i "~/.ssh/id_NSSC" -b "${SFTPBATCH}" ${USER}@${REMOTEHOST} >> "${LOGFILE}" 2> "${SFTPERRORS}"
  fi
else
  # The script already checked and exited if totalToDownload -eq 0
  # Should never hit this condition
  EMAILBODY="NO NEW FILES - YOU SHOULD NEVER SEE THIS"
  echo -e "${EMAILBODY}"
  exit
fi
if [[ -s $SFTPERRORS ]]
then
  ERRORS_STRING=$(cat "${SFTPERRORS}")
  EMAILBODY="${EMAILBODY}\nSFTP errors\n$ERRORS_STRING"
  echo -e "${EMAILBODY}"
fi
SYNCTIMEEND=$(date +%s)

##############################################################################
# Cleanup and information
##############################################################################
ENDTIME=$(date +%s)
HASHSYNCTIME=$((HASHTIMEEND - STARTTIME))
SYNCTIME=$((SYNCTIMEEND - HASHTIMEEND))
TOTALTIME=$((ENDTIME - STARTTIME))

echo "Time to download hashes files: 	${HASHSYNCTIME} seconds"
echo "Time to download ${totalFiles} rawacf files: 	${SYNCTIME} seconds"
echo "Total time to execute script : 	${TOTALTIME} seconds"
