#!/bin/bash
# Copyrght (C) SuperDARN Canada, University of Saskatchewan 
# Author(s): Marina Schmidt 
#
# Description: finds which fitacf files does not have a corresponding 
# summary plot and creates a list. Then the list is used in parallel 
# to generate summary plots. 
#
# Param: Normally date and year are passed in on cedar but for chapman 
#        we get the current month and year
#        On chapman we will pass in the number of processors to use, cedar we use nproc 
#        as the number of processes are set in the job submission. 
# Post: temprorary summary plot folder is created for generating summary plots
#       

PROC=$1
if [[ ${PROC} -eq 0 ]]
then
    PROC=1 # default will be one to be safe
fi

YEAR=$(date +%Y)
MONTH=$(date +%m)
# since this script will run at the beginning of each day, 
# firsts of the year/month we need to go back to get the 
# previous months data as we won't have data for the firsts 
# yet. 
if [[ $(date +%d) -eq 1 ]]
then
    MONTH=$(date --date='-1 month' +%m) # gets the previous month
    YEAR=$(date --date='-1 month' +%Y) # gets the previous year
fi

# setup directory structure these are not parameters 
# because of how cedar setup jobs 
# summary plot directories
TMPSUMMARYDIR=/home/superdarn/tmp_summary/${YEAR}/${MONTH}/
SUMMARYDIR=/sddata/summary_plots/${YEAR}/${MONTH}/

# fitacf dirs, need a sepparate one for borealis 
FITACFDIR=/sddata/fitacf_30/${YEAR}/${MONTH}/
BOREALISFITACFDIR=/sddata/borealis_fitacf_30/${YEAR}/${MONTH}

mkdir -p ${TMPSUMMARYDIR}

# This long command does the following: 
#   1. find all summary plots 
#      i) remove everything except <yyyymmdd>_<radar><slice/channel> in the filename
#      ii) pipe to sort and uniq
#   2. concatenate that with find of all fitacf files
#      i) remove everything in the filename except <yyyymmdd>_<radar><slice/channel>
#      ii) pipe to sort and uniq - uniq is needed due files being 2-hour files
#   3. sort outputs from 1. and 2. - this should put all duplicates together
#   4. pipe to uniq -u - this removes all non unique lines leaving the dates and radars 
#      that don't have a matching summary plot 
need_to_generate=$(sort <(find ${SUMMARYDIR} -type f -exec bash -c 'basename "${0%.*}"' {} \; | sed 's/pydarn_\(.*\)_\(.*\)_.*/\1 \2/' | sort | uniq) <(find ${FITACFDIR} -type f -exec bash -c 'basename "${0%.*}"' {} \; | sed -e 's/\([0-9]\+\)\.[0-9]\+\.[0-9]\+\.\([a-z]\+.*\)\..*/\1 \2/' | sort | uniq) | uniq -u)

echo ${need_to_generate}
# commented out as it is needed for job submission in cedar 
#/opt/software/slurm/bin/srun /cvmfs/soft.computecanada.ca/nix/var/nix/profiles/16.09/bin/parallel --joblog /home/mschmidt/logs/summary_logs.log -j$(nproc) --max-args=2 python3 pydarn_generate_summary_plots.py {} ${FITACFDIR} ::: ${need_to_generate}

parallel -j${PROC} --max-args=2 python3 pydarn_generate_summary_plots.py {} ${FITACFDIR} ${TMPSUMMARYDIR} ::: ${need_to_generate}
# again needed for cedar
#mv -v ${TMPSUMMARYDIR}/*.png ${SUMMARYDIR}/
#chgrp -R rpp-kam136 ${SUMMARYDIR}
#chmod -R 664 ${SUMMARYDIR} 

# see comment above, same command but now with borealis fitacf data
need_to_generate=$(sort <(find ${SUMMARYDIR} -type f -exec bash -c 'basename "${0%.*}"' {} \; | sed 's/pydarn_\(.*\)_\(.*\)_.*/\1 \2/' | sort | uniq) <(find ${BOREALISFITACFDIR} -type f -exec bash -c 'basename "${0%.*}"' {} \; | sed -e 's/\([0-9]\+\)\.[0-9]\+\.[0-9]\+\.\([a-z]\+.*\)\..*/\1 \2/' | sort | uniq) | uniq -u)
parallel -j${PROC} --max-args=2 python3 pydarn_generate_summary_plots.py {} ${BOREALISFITACFDIR} ${TMPSUMMARYDIR} ::: ${need_to_generate}

echo ${need_to_generate}
# cedar code
#/opt/software/slurm/bin/srun /cvmfs/soft.computecanada.ca/nix/var/nix/profiles/16.09/bin/parallel --joblog /home/mschmidt/logs/summary_logs.log -j$(nproc) --max-args=2 python3 pydarn_generate_summary_plots.py {} ${BOREALISFITACFDIR} ::: ${need_to_generate}
#mv -v ${TMPSUMMARYDIR}/*.png ${BOREALISSUMMARYPLOTSDIR}/
#chgrp -R rpp-kam136 ${BOREALISSUMMARYPLOTSDIR}
#chmod -R 664 ${BOREALISSUMMARYPLOTSDIR}

# since chapman doesn't have write permissions to the data nas we need to rsync the files over to mrcopy
# the chmod command sets the permissions of directories as 755 and files 644
rsync -azvh --chown=mrcopy:users --chmod=Du=rwx,Dg=rx,Do=rx,Fu=rw,Fg=r,Fo=r ${TMPSUMMARYDIR} mrcopy@sdcopy:/sddata/summary_plots/${YEAR}/${MONTH}/

exit 0

