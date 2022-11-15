#!/bin/bash
# Copyright 2022 SuperDARN Canada, University of Saskatchewan
# Author: Theodore Kolkman
#
# Configuration file for data flow repository

###################################################################################################

# Valid site RADARID values
readonly VALID_RADARIDS=("sas" "pgr" "inv" "cly" "rkn")

###################################################################################################

# Transfer to NAS flag. If not true, transfer to $SITE_LINUX computer instead
# Changes what data is transferred and converted, and where the data flows through
readonly NAS_SITES=("sas" "pgr" "inv" "cly" "rkn")

###################################################################################################

# Files each site converts. If a site isn't specified they are not converting/restructuring that 
# type of file
readonly RAWACF_SITES=("sas" "pgr" "inv" "cly" "rkn")
readonly BFIQ_SITES=("pgr")
readonly ANTENNAS_IQ_SITES=("sas" "pgr" "inv" "cly" "rkn")

# Special case sites. If a site is specified here, the data flow will be slightly altered to 
# accomodate the site
readonly LOW_MEMORY_SITES=("pgr" "inv" "rkn")   # Sites that can't restructure antennas_iq files normally
readonly LOW_BANDWIDTH_SITES=("cly" "rkn")      # Sites that don't transfer dmap files to campus

###################################################################################################

# Define variables needed for telemetry
readonly TELEMETRY_DIR="/home/telemetry/data_flow_logs"
readonly TELEMETRY="telemetry@chapman.usask.ca"
readonly RSH="ssh -p 2222"

###################################################################################################
