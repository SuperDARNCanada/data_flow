# Data Flow
This repo contains scripts related to data flow and management, from file generation to distribution.
Each server uses different scripts, but all are contained in this repo. Directories are separated by server

borealis - The server that creates the data files (currently rawacf, antennas\_iq and bfiq)
site-linux - The linux server on SuperDARN Canada sites that does further backup/processing/moving of data files
sdcopy - The linux server at the UofS that currently handles data checking, backup and staging for VT
cssdp - The linux server at the UofS that currently handles the temporary mirror, as well as uploads to cedar via globus
cedar - The Compute Canada server that houses SuperDARN Canada's storage allocation, accessible via globus.


# Typical Crontab for Borealis Machine to Sync All Files to Nas and Rotate Antennas IQ files
```
8,43 */2 * * * . $HOME/.profile; /home/radar/data_flow/borealis/rsync_to_nas.sh >> $HOME/rsync_to_nas.log 2>&1
```

# Typical Crontab for Site Linux Machine (OR Borealis Machine) to Convert Files on NAS and sync to Off-Site
```
10,45 0,2,4,6,8,10,12,14,16,18,20,22 * * * ${HOME}/data_flow/site-linux/convert_and_restructure_daily_on_nas.sh >> ${HOME}/convert_and_restructure_borealis_log.txt 2>&1
*/5 * * * * ${HOME}/data_flow/site-linux/rsync_to_sas_from_nas.sh >> ${HOME}/rsync_to_sas.log 2>&1
```
