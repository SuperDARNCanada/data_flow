# Data Flow
This repo contains scripts related to data flow and management, from file generation to distribution.
Each server uses different scripts, but all are contained in this repo. Directories are separated by server

borealis - The server that creates the data files (currently rawacf, antennas\_iq and bfiq)
site-linux - The linux server on SuperDARN Canada sites that does further backup/processing/moving of data files
sdcopy - The linux server at the UofS that currently handles data checking, backup and staging for VT
cssdp - The linux server at the UofS that currently handles the temporary mirror, as well as uploads to cedar via globus
cedar - The Compute Canada server that houses SuperDARN Canada's storage allocation, accessible via globus.

