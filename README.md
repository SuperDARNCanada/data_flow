## Data Flow
This repo contains scripts related to data flow and management, from file generation to
distribution. Each server uses different scripts, but all are contained in this repo. Directories
are separated by server and usage.

- borealis - The server that creates the Borealis data files (currently rawacf, antennas\_iq and 
bfiq). 
- site-linux - The linux server on SuperDARN Canada sites that does further backup/processing and
moving of data files to the university campus.
- campus - The linux server sdc-serv at the UofS that currently handles uploads to cedar via globus,
stages SuperDARN files for other institutions, and backs up data to the campus' long term storage.
Currently in process of moving these functions from superdarn-cssdp to sdc-serv.
- inotify_daemons - Scripts that use `inotifywait` to monitor the data flow and trigger scripts once
the preceding script has finished execution. These scripts are to be set up as a `systemd` service.
Example service files are within the `inotify_daemons/services/` directory.
- library - Bash functions used by data flow scripts.
- tools - Various scripts used alongside the data flow. Ex: log parsing scripts
- script_archive - Old unused data flow scripts.

### How it works
The SuperDARN data flow scripts execute in the following order:

1. borealis/rsync_to_nas: Move 2-hour blocks of Borealis files (rawacf, antennas\_iq, bfiq) to the 
site storage (default is site NAS). Triggered via borealis.daemon when next 2-hour Borealis block 
starts writing
2. site-linux/convert_and_restructure: Convert rawacf files to SuperDARN DMAP format, and 
restructure all site files to array format. Files are converted and restructured on the site 
storage and organized in respective directories. Triggered via site-linux.daemon when rsync_to_nas 
finishes executing.
3. site-linux/rsync_to_campus: Moves rawacf DMAP and array files from the site storage to the 
university campus server at sdc-serv. Triggered via site-linux.daemon when 
convert_and_restructure finishes executing.
4. campus/convert_on_campus: Converts rawacf array files to DMAP files for sites that only 
have bandwidth to transfer array files to campus. Sites that don't need to convert anything just 
skip to end of script. Triggered via campus.daemon when rsync_to_campus finishes executing
5. campus/distribute_borealis_files: Copy DMAP files to respective directories to 
distribution to other institutions, the Globus mirror, and Cedar. Backs up DMAP and array files to 
the campus NAS. Triggered via campus.daemon when convert_on_campus finishes executing.


Each script is triggered by an inotify daemon unique to each computer. These daemons run on each of 
the data flow computers (borealis, site-linux, and sdc-serv) and run sequentially using 
inotify to trigger each daemon script in order. Hidden directories `.inotify_flags/` and 
`inotify_watchdir/` are created and used to manage inotify flags. The daemon scripts are as follows:

- borealis.daemon: Runs on the Borealis computer. Executes rsync_to_nas when inotify sees a new 
2-hour borealis file get created. When rsync_to_nas finishes, the daemon sends a "flag" file to the 
site-linux computer to trigger the next data flow daemon
- site-linux.daemon: Runs on the Site-Linux computer. Executes convert_and_restructure and 
rsync_to_campus sequentially as soon as rsync_to_nas finishes. Triggered by flag sent by 
borealis.daemon.
- campus.daemon: Runs on sdc-serv.usask.ca. Executes convert_on_campus and distribute_borealis_data 
sequentially as soon as rsync_to_campus finishes. Triggered by flag sent by site-linux.daemon.

Each of these daemons are configured through `systemd`, as described below. Example `.service` 
files are given in `inotify_daemons/services/`.

### Setting up the inotify daemon
To make a daemon with `systemd`, create a `.service` file within `/usr/lib/systemd/system/` (must 
be super user). For example, the `borealis.daemon` is run with the following 
`borealis_dataflow.service` file:

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

Useful systemctl commands for operating `systemd` daemons:
- `systemctl daemon-reload`
- `systemctl enable borealis_dataflow.service`
- `systemctl start borealis_dataflow.service`
- `systemctl status borealis_dataflow.service`
- `systemctl restart borealis_dataflow.service`
- `systemctl stop borealis_dataflow.service`
- `systemctl disable borealis_dataflow.service`


### Installing data flow

To use this data flow repository, follow the following steps:

1. Clone data flow repository
2. Install `inotifywait` via zypper with `sudo zypper in inotify-tools` if it is not already 
installed.
3. Set up ssh between current computer and next computer in dataflow to work without password. This
is required for the sending of inotify flags between computers.
    - As the user running the dataflow, create a key (if no key already created):
    `ssh-keygen -t ecdsa -b 521`
    - Copy the public key to the destination computer: `ssh-copy-id user@host`
    - Computers that must be linked: Borealis -> Site-Linux, Site-Linux -> sdc-serv
    - For telemetry purposes, each data flow computer must also be linked to Chapman, so copy the ssh keys to Chapman as well
4. Install the inotify daemon for the respective computer (for example, install borealis.daemon 
with borealis_dataflow.service on the Borealis computer). As super user, do the following:
    - Copy the correct `.service` file from `inotify_daemons/services/` to 
    `/usr/lib/systemd/system/`
    - Reload the daemons: `systemctl daemon-reload`
    - Enable the daemon: `systemctl enable [dataflow].service`
    - Start the daemon: `systemctl start [dataflow].service`
    - Check that the daemon is running: `systemctl status [dataflow].service`
    - To specify the radar for `campus_dataflow@.service` (using sas as an example): 
    `systemctl [command] campus_dataflow@sas.service`
5. Ensure the pydarnio-env virtual environment is set up in home directory and configured correctly.
    - To link pydarnio-env/ to the current branch in the ~/pyDARNio local repo, do the following 
    commands:
        - `source ~/pydarnio-env/bin/activate`
        - `pip install -e ~/pyDARNio`
    - If the `-e` is omitted, the pydarnio-env will just be installed with the current branch of
    ~/pyDARNio, and won't be updated if the branch changes.
6. Check the logs to ensure the data flow is working correctly
    - The inotify daemon logs are available in the `~/logs/inotify_daemons/` directory
    - The data flow script logs are available in the `~/logs/[script name]` directory
7. For telemetry purposes, summary logs are availabe for each script in the 
`~/logs/[script name]/summary/` directory. These logs contain the status of all operations on each
file and easily parseable to monitor data flow operation. Each script rsyncs the summary files to 
Chapman for uploading to the Engineering dashboard. SSH password-free connection must be setup 
between each computer and Chapman for this to work correctly. 
8. To modify the data flow easily, a `config.sh` file is provided. This file specifies:
    - If the data flow can use the NAS at a site
    - What Borealis filetypes are to be converted and restructured
    - Which sites have bandwidth / memory limitations
    - Where logs should be synched for telemetry
