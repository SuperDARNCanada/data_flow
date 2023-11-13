# Copyright 2022 SuperDARN Canada, University of Saskatchewan
# Author: Theodore Kolkman
#
# This file contains functions used by various data flow scripts
# Functions have been moved here from within other scripts to clean up code
#
# To use this library:  source $HOME/data_flow/lib/data_flow_functions.sh


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
# Get rawacf dmap filename from corresponding array filename. 
#
# Example: `get_dmap_name 20231004.1400.00.pgr.1.rawacf.hdf5` will print 
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

	array_filename=$(basename $1)
	array_directory=$(dirname $1)
	# Check that the filename given is a valid dmap file name
	if [[ ! "$array_filename" =~ ^[0-9]{8}.[0-9]{4}.[0-9]{2}.[[:lower:]]{3}.[0-9]+.rawacf.hdf5$   ]]; then
		printf "get_dmap_name(): Invalid filename - $array_filename isn't a valid array file name\n"
		return 1
	fi

	file_start="${array_filename%.rawacf.hdf5}"

	# Remove last character(s) (slice_id)
	slice_id=$(echo $file_start | rev | cut --delimiter='.' --fields=1 | rev)
	file_start_wo_slice_id="${file_start%${slice_id}}"

	ordinal_id="$(($slice_id + 97))"
	file_character=$(chr $ordinal_id)

	dmap_file="${file_start_wo_slice_id}${file_character}.rawacf.bz2"
	printf "${array_directory}/${dmap_file}"

	return 0
}


###################################################################################################
# Get rawacf array filename from corresponding dmap filename. 
#
# Example: `get_array_name 20231004.1400.00.pgr.b.rawacf.bz2` will print 
#          20231004.1400.00.pgr.1.rawacf.hdf5 to standard output.
#
# Argument 1: dmap filename
# Returns:    0 on successful name translation
###################################################################################################
get_array_name() {
	# Check function was called correctly
	if [[ $# -ne 1 ]]; then
		printf "get_array_name(): Invalid number of arguments\n" 
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
		printf "get_array_name(): Invalid filename - $dmap_filename isn't a valid dmap file name\n"
		return 1
	fi

	# Remove last character (the file character)
	file_character=$(echo $file_start | rev | cut --delimiter='.' --fields=1 | rev)
	file_start_wo_character="${file_start%${file_character}}"

	# Convert file character to slice ID
	ordinal_id="$(ord $file_character)"
	slice_id=$(($ordinal_id - 97))

	# Put it all together
	array_file="${file_start_wo_character}${slice_id}.rawacf.hdf5"
	printf "${dmap_directory}/${array_file}"

	return 0
}

###################################################################################################
# Verify transfer of a file using md5sum
# 
# Calculates the md5sum of the sent file, and compares it to the md5sum of the source file. If the 
# destination file is on a different computer, specify the ssh address of the destination computer 
# (Ex. radar@10.65.0.32)
# 
# Example: Transferring between local directories (i.e. Borealis to NAS)
# 	verify_transfer /data/borealis_data/[FILE] /borealis_nfs/borealis_data/backup/[FILE]
# Example: Transferring to a different computer (i.e. NAS to SuperDARN-CSSDP)
# 	verify_transfer /borealis_nfs/borealis_data/rawacf_array/[FILE] /sddata/inotify_data_flow/sas_data/[FILE] mrcopy@128.233.224.39
#
# Argument 1: Source file
# Argument 2: Destination file (if on remote computer, specify in argument 3)
# Argument 3: ssh address of destination computer (leave empty if local)
###################################################################################################
verify_transfer () {
	local source_file=$1
	local dest_file=$2
	local dest_ssh=${3-""}	# Default to empty string
	if [[ -n $dest_ssh ]]; then
		ssh $dest_ssh "md5sum --binary $dest_file" > $HOME/tmp.md5
	else
		md5sum --binary $dest_file > $HOME/tmp.md5
	fi

	# Convert md5sum to look at source file
	sed -i "s~$dest_file~$source_file~g" $HOME/tmp.md5
	# Check md5sum of destination file is same as source
	md5sum --check --status $HOME/tmp.md5
	return $?
}