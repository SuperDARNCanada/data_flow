This directory contains files that are sent by data flow scripts to 
trigger the next script to execute via inotify. These files are watched
by inotify, and when they are transferred to the .inotify_Watchdir directory
on the respective computers, inotify starts the next data flow script.

Flags in this directory:
.rsync_to_nas_flag
    Sent by rsync_to_nas on site Borealis computers to trigger 
    convert_and_restructure once the 2-hour site files are finished
    writing
.convert_and_restructure_flag
    Sent by convert_and_restructure on the site linux computer running
    data flow to trigger the rsync_to_campus script. Flag sends once 
    conversion is finished
.rsync_to_campus_flag
    Sent by rsync_to_campus to the superdarn-cssdp computer to trigger
    the convert_on_campus script once all files have finished transferring
    to campus
.convert_on_campus_flag
    Sent by convert_on_campus to trigger the distribute_borealis_data
    script
.distribute_borealis_data_flag
    Sent by the distribute_borealis_data script once it has finished execution.
    Next script in the data flow can use this flag to start up.