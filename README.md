## Data Flow
This repo contains scripts related to data flow and management, from file generation to distribution.
Each server uses different scripts, but all are contained in this repo. Directories are separated by server and usage

- borealis - The server that creates the Borealis data files (currently rawacf, antennas\_iq and bfiq). 
- site-linux - The linux server on SuperDARN Canada sites that does further backup/processing and moving of data files
to the university campus
- superdarn-cssdp - The linux server at the UofS that currently handles the temporary mirror, uploads to cedar via globus, 
stages SuperDARN files for other institutions, and backs up data to the campus' long term storage.
- inotify_daemons - Scripts that use `inotifywait` to monitor the data flow and trigger scripts once the preceding script 
has finished execution. These scripts are to be set up as a systemd service.
- .inotify_flags - Directory where scripts will create flag files for the inotify daemon to watch
- .inotify_watchdir - Directory that the inotify daemon monitors for flag files
- library - Bash functions used by data flow scripts
- script_archive - Old unused data flow scripts

### How it works
The SuperDARN data flow scripts execute in the following order:
1. borealis/rsync_to_nas: Move 2-hour blocks of Borealis files (rawacf, antennas\_iq, bfiq) to the site storage (default
is site NAS). Triggered when next 2-hour Borealis block starts writing
2. site-linux/convert_and_restructure: Convert rawacf files to SuperDARN DMAP format, and restructure all site files to
array format. Files are converted and restructured on the site storage and organized in respective directories. Triggered
by flag sent at completion of rsync_to_nas.
3. site-linux/rsync_to_campus: Moves rawacf DMAP and array files from the site storage to the university campus server at
superdarn-cssdp. Triggered by flag sent at completion of convert_and_restructure.
4. superdarn-cssdp/convert_on_campus: Converts rawacf array files to DMAP files for sites that only have bandwidth to 
transfer array files to campus. Sites that don't need to convert anything just skip to end of script. Triggered by flag
sent at completion of rsync_to_campus
5. superdarn-cssdp/distribute_borealis_files: Copy DMAP files to respective directories to distribution to other institutions,
the Globus mirror, and Cedar. Backs up DMAP and array files to the campus NAS. Triggered by flag sent at completion of
convert_on_campus.


Each script is triggered by the inotify daemon at the completion of the previous script (or when files are ready to be 
transferred for rsync_to_nas). The inotify daemon watches a specific directory for a specific file change (i.e. a flag 
file being written there), and when this occurs it starts its respective data flow script.

### Setting up the inotify daemon
To make a daemon with `systemd`, create a `.service` file within `/usr/lib/systemd/system/` (must be super user). For 
example, the `rsync_to_nas.daemon` is run with the following `rsync_to_nas.service` file:

```
[Unit]
Description=Data flow daemon: rsync_to_nas

[Service]
User=radar
ExecStart=/home/radar/data_flow/inotify_daemons/rsync_to_nas.daemon
Restart=always

[Install]
WantedBy=multi-user.target
```

To start the service, execute the following commands as superuser:
- `systemctl daemon-reload`
- `systemctl enable rsync_to_nas.service`
- `systemctl start rsync_to_nas.service`

To verify it is running:
- `systemctl status rsync_to_nas.service`