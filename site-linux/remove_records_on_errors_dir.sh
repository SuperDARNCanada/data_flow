#!/bin/bash

DAILY_DIR=/borealis_nfs/borealis_data/daily/
ERRORS_DIR=/borealis_nfs/borealis_data/hdf5_errors/
BACKUP_DIR=/borealis_nfs/borealis_data/hdf5_errors_backup/
FILE_PATTERN_FIX=*.site
FILES_TO_FIX=`find "${ERRORS_DIR}" -name "${FILE_PATTERN_FIX}" -type f`

for f in ${FILES_TO_FIX}
do
    cp ${f} ${BACKUP_DIR}
    python3 ${HOME}/data_flow/site-linux/remove_record.py ${f} 
    ret=$?
    if [ $ret -eq 0 ]; then
        mv -v ${f} ${DAILY_DIR}
    fi
done
