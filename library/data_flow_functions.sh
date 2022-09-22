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