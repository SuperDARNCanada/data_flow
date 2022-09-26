# Copyright 2022 SuperDARN Canada, University of Saskatchewan
# Author: Theodore Kolkman
#
# This file contains functions used by various data flow scripts
# Functions have been moved here from within scripts to clean up code a bit
#
# To use this library:  source $HOME/data_flow/lib/data_flow_functions.sh


##############################################################################
# Convert decimal value to corresponding ascii character for use in dmap file
# names. Example: `chr 99` would print 'c'.
# Argument 1: Ascii decimal value between 1 and 256
##############################################################################
chr() {
	[ "$1" -lt 256 ] || return 1
	printf "\\$(printf '%03o' "$1")"
}

##############################################################################
# Convert ascii character to corresponding decimal value. Example: `ord c` 
# would print 99). Using C-style character set
# Argument 1: Ascii character
##############################################################################
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

##############################################################################
# Verify transfer of a file using md5sum
# Calculated the md5sum of the sent file, and compares it to the md5sum of 
# the source file. If the destination file is on a different computer, specify
# the ssh address of the destination computer (Ex. radar@10.65.0.32)
# Argument 1: Source file
# Argument 2: Destination file (if on remote computer, specify in argument 3)
# Argument 3: ssh address of destination computer (leave empty if local)
##############################################################################
verify_transfer () {
	local source_file=$1
	local dest_file=$2
	local dest_ssh=${3-""}	# Default to empty string
	if [[ -n $dest_ssh ]]; then
		ssh $dest_ssh "md5sum --binary $dest_file" > tmp.md5
	else
		md5sum --binary $dest_file > tmp.md5
	fi

	# Convert md5sum to look at source file
	sed -i "s~$dest_file~$source_file~g" tmp.md5
	# Check md5sum of destination file is same as source
	md5sum --check --status tmp.md5
	return $?
}