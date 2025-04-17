# Copyright 2022 SuperDARN Canada, University of Saskatchewan
# Author: Theodore Kolkman, Draven Galeschuk
#
# This file contains functions used by various data flow scripts
# Functions have been moved here from within other scripts to clean up code
#
# To use this library:  `source $HOME/data_flow/library/data_flow_functions.sh`


###################################################################################################
# Convert decimal value to corresponding ascii character for use in dmap file names. Example: 
# `chr 99` would print 'c'.
# Argument 1: Ascii decimal value between 1 and 256
###################################################################################################
chr() {
	[ "$1" -lt 256 ] || return 1
	printf "\\$(printf '%03o' "$1")"
}

###################################################################################################
# Convert ascii character to corresponding decimal value. Example: `ord c` would print 99. Using 
# C-style character set
# Argument 1: Ascii character
###################################################################################################
ord() {
	LC_CTYPE=C printf '%d' "'$1"
}

###################################################################################################
# Get rawacf dmap filename from corresponding borealis filename.
#
# Example: `get_dmap_name 20231004.1400.00.pgr.1.rawacf.h5` will print
#          20231004.1400.00.pgr.b.rawacf.bz2 to standard output.
#
# Argument 1: array filename
# Returns:    0 on successful name translation
###################################################################################################
get_dmap_name() {
	# Check function was called correctly
	if [[ $# -ne 1 ]]; then
		printf "get_dmap_name(): Invalid number of arguments\n" 
		return 1
	fi

	borealis_filename=$(basename $1)
	borealis_directory=$(dirname $1)
	# Check that the filename given is a valid rawacf file name
	if [[ ! "$borealis_filename" =~ ^[0-9]{8}.[0-9]{4}.[0-9]{2}.[[:lower:]]{3}.[0-9]+.rawacf.h5$   ]]; then
		printf "get_dmap_name(): Invalid filename - $borealis_filename isn't a valid borealis file name\n"
		return 1
	fi

	file_start="${borealis_filename%.rawacf.h5}"

	# Remove last character(s) (slice_id)
	slice_id=$(echo $file_start | rev | cut --delimiter='.' --fields=1 | rev)
	file_start_wo_slice_id="${file_start%${slice_id}}"

	ordinal_id="$(($slice_id + 97))"
	file_character=$(chr $ordinal_id)

	dmap_file="${file_start_wo_slice_id}${file_character}.rawacf.bz2"
	printf "${borealis_directory}/${dmap_file}"

	return 0
}


###################################################################################################
# Get rawacf borealis filename from corresponding dmap filename.
#
# Example: `get_borealis_name 20231004.1400.00.pgr.b.rawacf.bz2` will print
#          20231004.1400.00.pgr.1.rawacf.h5 to standard output.
#
# Argument 1: dmap filename
# Returns:    0 on successful name translation
###################################################################################################
get_borealis_name() {
	# Check function was called correctly
	if [[ $# -ne 1 ]]; then
		printf "get_borealis_name(): Invalid number of arguments\n"
		return 1
	fi

	dmap_filename=$(basename $1)
	dmap_directory=$(dirname $1)
	
	# Check that the filename given is a valid dmap file name
	if [[ "$dmap_filename" =~ ^[0-9]{8}.[0-9]{4}.[0-9]{2}.[[:lower:]]{3}.[[:lower:]]+.rawacf.bz2$ ]]; then
		file_start="${dmap_filename%.rawacf.bz2}"
	elif [[ "$dmap_filename" =~ ^[0-9]{8}.[0-9]{4}.[0-9]{2}.[[:lower:]]{3}.[[:lower:]]+.rawacf$ ]]; then
		file_start="${dmap_filename%.rawacf}"
	else
		printf "get_borealis_name(): Invalid filename - $dmap_filename isn't a valid dmap file name\n"
		return 1
	fi

	# Remove last character (the file character)
	file_character=$(echo $file_start | rev | cut --delimiter='.' --fields=1 | rev)
	file_start_wo_character="${file_start%${file_character}}"

	# Convert file character to slice ID
	ordinal_id="$(ord $file_character)"
	slice_id=$(($ordinal_id - 97))

	# Put it all together
	borealis_file="${file_start_wo_character}${slice_id}.rawacf.h5"
	printf "${dmap_directory}/${borealis_file}"

	return 0
}

###################################################################################################
# Verify transfer of a file using md5sum
# 
# Calculates the md5sum of the sent file, and compares it to the md5sum of the source file. If the 
# destination file is on a different computer, specify the ssh address of the destination computer 
# (Ex. transfer@192.168.1.204)
# 
# Example: Transferring between local directories (i.e. as in rsync_to_nas)
#       verify_transfer /data/borealis_data/[FILE] /borealis_nfs/borealis_data/backup/[FILE]
# Example: Transferring to a different computer (i.e. as in rsync_to_campus)
#       verify_transfer /borealis_nfs/borealis_data/rawacf_array/[FILE] /sddata/sas_holding_dir/[FILE] dataman@sdc-serv.usask.ca
#
# Argument 1: Source file
# Argument 2: Destination file (if on remote computer, specify in argument 3)
# Argument 3: ssh address of destination computer (leave empty if local)
###################################################################################################
verify_transfer () {
	local source_file=$1
	local dest_file=$2
	local dest_ssh=${3-""}  # Default to empty string

	TMP_FILE="/tmp/transfer_$(basename ${source_file}).md5" # Unique tmp file for this transfer

	if [[ -n $dest_ssh ]]; then
		ssh $dest_ssh "md5sum --binary $dest_file" > $TMP_FILE
	else
		md5sum --binary $dest_file > $TMP_FILE
	fi

	# Convert md5sum to look at source file
	sed -i "s~$dest_file~$source_file~g" $TMP_FILE
	# Check md5sum of destination file is same as source
	md5sum --check --status $TMP_FILE
	retval=$?
 	rm $TMP_FILE
	return $retval
}

###################################################################################################
# Sends an alert to a slack channel - generally the #data-flow-alerts channel
#
# Takes in a message to send to slack and a slack webhook and attempts to send that message to
# the associated slack channel through the Borealis Alerts incoming webhooks app. The message
# should contain useful information such as: timestamp on computer, file/files affected,
# error message or description of problem, device error occurred on.
#
# Argument 1: message to send
# Argument 2: slack webhook. likely loaded into the script calling this function from .profile
###################################################################################################
alert_slack() {
  message=$1
  webhook=$2
  LOGFILE_SLACKALERT="${HOME}/logs/slack_dataflow_notif.log"

  NOW=$(date +'%Y%m%d %H:%M:%S')
  if [[ -z ${message} ]]; then
    echo "${NOW} dataflow slack message error: Empty message attempted to be sent." | tee -a "${LOGFILE_SLACKALERT}"
  elif [[ -z ${webhook} ]]; then
    echo "${NOW} dataflow webhook error: No webhook was found. Attempted message was ${message}" | tee -a "${LOGFILE_SLACKALERT}"
  fi

  curl --silent --header "Content-type: application/json" --data "{'text':'${message}'}" "${webhook}"
  result=$?
  if [[ ${result} -ne 0 ]]; then
    echo "${NOW} attempt to curl to webhook ${webhook} failed with error: ${result} (see https://curl.se/libcurl/c/libcurl-errors.html)" | tee -a "${LOGFILE_SLACKALERT}"
  fi
}

###################################################################################################
# Verify that the timestamp and last modification time of a Borealis file are consistent
#
# Compares the timestamp in a Borealis file's name (i.e. 20220617.2200.00) to the last modification
# time of the file (found with the 'stat' command). If the difference in these times is greater
# than a specified threshold, the function returns an error code.
#
# Argument 1: Path to a Borealis file to check.
#
# Returns 0: Success: difference between timestamp and modification time are consistent.
#         1: Error: Function used incorrectly
#         2: Failure: difference between timestamp and modification time is greater than the 
#         specified threshold
###################################################################################################
check_timestamp() {
	# Check function was called correctly
	if [[ $# -ne 1 ]]; then
		printf "check_timestamp(): Invalid number of arguments\n" 
		printf "Usage: check_timestamp filename"
		return 1
	fi

	local file=$1						# Borealis file to check
	local filename=$(basename $file)	# The name of the file (no path)
	local threshold=86400				# Threshold is 1 day (86400 seconds)

	# Check that the filename given is a valid file name
	if [[ ! "$filename" =~ ^[0-9]{8}\.[0-9]{4}\.[0-9]{2}\.[[:lower:]]{3}\.[a-z0-9]+\..+(\.h5)?$   ]]; then
		printf "check_timestamp(): Invalid filename - $filename isn't a valid file name\n"
		return 1
	fi

	# Get timestamp time in seconds since epoch from the filename timestamp. 
	# Must replace '.' with ' ' to get a string that can be interpreted by `date` command
	local timestamp_string=$(echo $filename | cut --fields 1-2 --delimiter '.')
	local timestamp_time=$(date --utc --date "${timestamp_string//./ }" +%s)		

	# Get file modification time in seconds since epoch	
	local modification_time=$(stat --format=%Y "$file")

	# Get time difference
	local time_diff=$(($modification_time - $timestamp_time))
	local absolute_diff="${time_diff#-}" # Remove '-' sign to ensure difference is positive
	if [[ $absolute_diff -gt $threshold ]]; then
		# File timestamp and modification time is inconsistent - return error code 2
		return 2
	fi

	# If end of function is reached, the file timestamp and modification times are consistent.
	return 0
}
