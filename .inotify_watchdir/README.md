This directory is watched by inotify for any files written to it. The dataflow
scripts will send a flag file here when they finish running, and the respective
computer will see this through inotify and start the next script up. 

When the script triggered by a flag here finished, it deletes the file so that 
inotify can correctly see the next time the flag is sent. As a result, this directory
will be empty unless data flow scripts are executing