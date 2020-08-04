#!/bin/bash

DAILY_DIR=/data/daily/
ERRORS_DIR=/data/hdf5_errors/
FILE_PATTERN_FIX=*.site
FILES_TO_FIX=`find "${ERRORS_DIR}" -name "${FILE_PATTERN_FIX}" -type f`

for f in ${FILES_TO_FIX}
do
    python3 ${HOME}/data_flow/site-linux/remove_record.py ${f} 
    ret=$?
    if [ $ret -eq 0 ]; then
        mv -v ${f} ${DAILY_DIR}
    fi
done

