## Data Flow
This repo contains scripts related to data flow and management, from file generation to
distribution. Each server uses different scripts, but all are contained in this repo. Directories
are separated by server and usage.

- borealis - Scripts run on the site Borealis computers (bore206, main207). These scripts transfer
  Borealis rawacf and antennas\_iq data files from the computer to the site NAS (nas203).
- site-linux - Scripts run on the site distribution computers (dist204, dist205). These computers
  perform extra data processing and transfer files from the site NAS to campus.
- campus - Scripts run on the on-campus data server (sdc-serv). These scripts verify files received
  from site and move them to respective directories for data backup, staging for other institutions,
  and staging for the SuperDARN data mirror.
- mirror - Scripts run on the on-campus data server (sdc-serv). These scripts control the USask
  SuperDARN Mirror using Globus scripts, and syncs data with the other SuperDARN mirrors.
- inotify_daemons - Scripts that use `inotifywait` to monitor the data flow and trigger scripts once
the preceding script has finished execution. These scripts are to be set up as a `systemd` service.
Example service files are within the `inotify_daemons/services/` directory.
- library - Bash and python functions and tools used by data flow scripts.

### How it works
The SuperDARN data flow scripts execute in the following order:

1. borealis/rsync_to_nas: Move 2-hour blocks of Borealis files (rawacf, antennas\_iq) to the site
storage (default is the site NAS). Triggered via borealis.daemon when the next 2-hour Borealis file
is first written
2. site-linux/plot_antennas_iq: reads in the antennas iq files after the previous script has run.
Creates plots of the iq data for each rx path using the last generated file. Triggered via
site-linux.daemon once borealis.daemon finishes running.
3. site-linux/rsync_to_campus: Moves rawacf files and iq plots from the site NAS to the university
campus server at sdc-serv. Triggered via site-linux.daemon when plot_antennas_iq finishes executing. 
4. campus/convert_on_campus: Converts rawacf array files to DMAP files for sites specified in
`config.sh`. Sites that don't need to convert anything just skip to end of script. Triggered via
campus.daemon when site-linux.daemon finishes running. 
5. campus/distribute_borealis_data: Copy DMAP files to respective directories for distribution to
other institutions, the Globus mirror, and CEDAR. Backs up DMAP and array files to the campus NAS.
Triggered via campus.daemon when convert_on_campus finishes executing.
6. campus/archive_iq_plots: Archives any iq plots on campus that are over 24 hours old by moving
them to an archive directory. Triggered via campus.daemon when distribute_borealis_data finishes
executing. 


Each script is triggered by an inotify daemon unique to each computer. These daemons run on each of 
the data flow computers (borealis, site-linux, and sdc-serv) and run sequentially using 
inotify to trigger each daemon script in order. Hidden directories `.inotify_flags/` and 
`inotify_watchdir/` are created and used to manage inotify flags. The daemon scripts are as follows:

- borealis.daemon: Runs on the Borealis computer. Executes rsync_to_nas when inotify sees a new
2-hour borealis file get created. When rsync_to_nas finishes, the daemon sends a "flag" file to the
site-linux computer to trigger the next data flow daemon
- site-linux.daemon: Runs on the Site-Linux computer. Executes plot_antennas_iq and rsync_to_campus
sequentially as soon as rsync_to_nas finishes. Triggered on flag sent by borealis.daemon.
- campus.daemon: Runs on sdc-serv. Executes convert_on_campus, distribute_borealis_data, and
archive_iq_plots sequentially as soon as rsync_to_campus finishes. Triggered by flag sent by
site-linux.daemon.

Each of these daemons are configured through `systemd`, as described below. Example `.service` files
are provided in `inotify_daemons/services/`.

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

### Setting up SSH multiplexing
The University of Saskatchewan has a firewall which blocks an IP address from connecting to the
campus network if 50 connection requests are made within one minute. The `rsync_to_campus` script
can easily hit this mark when sending `antennas_iq` plots, so SSH multiplexing is recommended. To
configure this:

1. log into the remote computer that will be running the `rsync_to_campus` script (i.e. a site-linux
computer).
2. `cd ~/.ssh`
3. `mkdir controlmasters`
4. Edit the file called `config`, adding the following (`[xxx]` indicates to fill in the value
   intelligently):
```
HOST [address of the campus computer, either hostname or IP]
    User [username]
    ControlPath ~/.ssh/controlmasters/%C
    ControlMaster auto
    ControlPersist 10m
```
5. Verify that the settings are working by running `ssh -N -f [username]@[address]; ssh -O check [username]@[address]`,
   where username and address are the same as those set in the config file. The output will be something like:
```
Success:
transfer@pgrdist205:~> ssh -N -f dataman@sdc-serv.usask.ca; ssh -O check dataman@sdc-serv.usask.ca
Master running (pid=15739)

Failure:
transfer@pgrdist205:~> ssh -N -f dataman@sdc-serv.usask.ca; ssh -O check dataman@sdc-serv.usask.ca
Control socket connect(/home/transfer/.ssh/controlmasters/3354587955ba492d0d5f595f8619d902ac0192a7): No such file or directory
```

### Installing data flow

To use this data flow repository, follow the following steps:

1. Clone data flow repository: `git clone https://github.com/SuperDARNCanada/data_flow.git`
2. Install `inotifywait` via zypper with `sudo zypper in inotify-tools` if it is not already 
installed.
3. Set up ssh between current computer and next computer in dataflow to work without password. This
is required for the sending of inotify flags between computers.
    - As the user running the dataflow, create a key (if no key already created):
    `ssh-keygen -t ecdsa -b 521`
    - Copy the public key to the destination computer: `ssh-copy-id user@host`
    - Computers that must be linked: Borealis -> Site-Linux, Site-Linux -> sdc-serv
    - For telemetry purposes, each data flow computer must also be linked to the logman user on
      sdc-serv, so copy the ssh keys to logman@sdc-serv as well
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
logman@sdc-serv for uploading to the Engineering dashboard. SSH password-free connection must be
setup between each computer and logman@sdc-serv for this to work correctly. 
8. To modify the data flow easily, a `config.sh` file is provided. This file specifies:
    - If the data flow can use the NAS at a site
    - What Borealis filetypes are to be converted and restructured
    - Which sites have bandwidth / memory limitations
    - Where logs should be synched for telemetry
