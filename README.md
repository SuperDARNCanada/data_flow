## Data Flow
This repo contains scripts related to data flow and management, from file generation to distribution.
Each server uses different scripts, but all are contained in this repo. Directories are separated by server and usage

- borealis - The server that creates the Borealis data files (currently rawacf, antennas\_iq and bfiq). 
- site-linux - The linux server on SuperDARN Canada sites that does further backup/processing and moving of data files
to the university campus
- superdarn-cssdp - The linux server at the UofS that currently handles uploads to cedar via globus, 
stages SuperDARN files for other institutions, and backs up data to the campus' long term storage.
- inotify_daemons - Scripts that use `inotifywait` to monitor the data flow and trigger scripts once the preceding script 
has finished execution. These scripts are to be set up as a `systemd` service.
- .inotify_flags - Directory where scripts will create flag files for the inotify daemon to watch
- .inotify_watchdir - Directory that the inotify daemon monitors for flag files
- library - Bash functions used by data flow scripts
- script_archive - Old unused data flow scripts

### How it works
The SuperDARN data flow scripts execute in the following order:

1. borealis/rsync_to_nas: Move 2-hour blocks of Borealis files (rawacf, antennas\_iq, bfiq) to the site storage (default
is site NAS). Triggered via borealis.daemon when next 2-hour Borealis block starts writing
2. site-linux/convert_and_restructure: Convert rawacf files to SuperDARN DMAP format, and restructure all site files to
array format. Files are converted and restructured on the site storage and organized in respective directories. Triggered via
site-linux.daemon when rsync_to_nas finishes executing.
3. site-linux/rsync_to_campus: Moves rawacf DMAP and array files from the site storage to the university campus server at
superdarn-cssdp. Triggered via site-linux.daemon when convert_and_restructure finishes executing.
4. superdarn-cssdp/convert_on_campus: Converts rawacf array files to DMAP files for sites that only have bandwidth to 
transfer array files to campus. Sites that don't need to convert anything just skip to end of script. Triggered via 
campus.daemon when rsync_to_campus finishes executing
5. superdarn-cssdp/distribute_borealis_files: Copy DMAP files to respective directories to distribution to other institutions,
the Globus mirror, and Cedar. Backs up DMAP and array files to the campus NAS. Triggered via campus.daemon when 
convert_on_campus finishes executing.


Each script is triggered by an inotify daemon unique to each computer. These daemons run on each of the data flow 
computers (borealis, site-linux, and superdarn-cssdp) and run sequentially using inotify to trigger each daemon script
in order. The daemon scripts are as follows:

- borealis.daemon: Runs on the Borealis computer. Executes rsync_to_nas when inotify sees a new 2-hour borealis file
get created. When rsync_to_nas finishes, the daemon sends a "flag" file to the site-linux computer to trigger the next
data flow daemon
- site-linux.daemon: Runs on the Site-Linux computer. Executes convert_and_restructure and rsync_to_campus sequentially
as soon as rsync_to_nas finishes. Triggered by flag sent by borealis.daemon.
- campus.daemon: Runs on SuperDARN-CSSDP. Executes convert_on_campus and distribute_borealis_data sequentially as soon
as rsync_to_campus finishes. Triggered by flag sent by site-linux.daemon.

Each of these daemons are configured through systemd, as described below. Example `.service.` files are given in 
`inotify_daemons/services/`.

### Setting up the inotify daemon
To make a daemon with `systemd`, create a `.service` file within `/usr/lib/systemd/system/` (must be super user). For 
example, the `borealis.daemon` is run with the following `borealis_dataflow.service` file:

```
[Unit]
Description=Borealis data flow inotify daemon

[Service]
User=radar
ExecStart=/home/radar/data_flow/inotify_daemons/borealis.daemon
Restart=always

[Install]
WantedBy=multi-user.target
```

Install `inotifywait` via zypper with `sudo zypper in inotify-tools` if it is not already installed.

To start the service, execute the following commands as superuser:
- `systemctl daemon-reload`
- `systemctl enable rsync_to_nas.service`
- `systemctl start rsync_to_nas.service`

To verify it is running:
- `systemctl status rsync_to_nas.service`