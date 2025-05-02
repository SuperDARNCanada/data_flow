#!/bin/bash
# Copyright 2022 SuperDARN Canada, University of Saskatchewan
# Author: Theodore Kolkman
#
# Configuration file for data flow repository

source "${HOME}/.profile"   # Get $SDC_SERV_IP for telemetry uses

###################################################################################################

# Valid site RADAR_ID values
readonly VALID_IDS=("sas" "pgr" "inv" "cly" "rkn")

###################################################################################################

# Transfer to NAS flag. If not true, transfer to $SITE_LINUX computer instead
# Changes what data is transferred and converted, and where the data flows through
readonly NAS_SITES=("sas" "pgr" "inv" "cly" "rkn")

###################################################################################################

# Files each site produces. If a site isn't specified they are not converting/restructuring that 
# type of file
readonly RAWACF_SITES=("sas" "pgr" "inv" "cly" "rkn")
readonly BFIQ_SITES=()
readonly ANTENNAS_IQ_SITES=("sas" "pgr" "inv" "cly" "rkn")

# Special case sites. If a site is specified here, the data flow will be slightly altered to 
# accomodate the site
readonly LOW_MEMORY_SITES=("pgr" "inv" "cly" "rkn" "sas")

###################################################################################################

# Define variables needed for telemetry
readonly TELEMETRY_DIR="/home/logman/data_flow_logs"
readonly TELEMETRY="logman@${SDC_SERV_IP}"
readonly TELEMETRY_RSH="ssh"

###################################################################################################
