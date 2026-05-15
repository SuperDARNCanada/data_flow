## Mirror
This section of the Data Flow repo contains scripts related to the campus to mirror rawacf data flow. These scripts
are run via crontab on sdc-serv. The scripts that are scheduled via crontab are located in the top level directory.
The remaining scripts are located in the `tools/` subdirectory and are utility scripts for the data flow operations.

### Main Scripts
- **download_vt_data** - This script logs onto the Virginia Tech (VT) server and downloads all rawacfs for a given
radar from the holding directory on the VT server to our holding directory on campus. Run the script like:
    - `download_vt_data holding_dir radar`
- **sync_mirror_data.sh** - This script logs onto one of the SuperDARN mirrors and downloads all rawacfs that our
mirror is missing. Any rawacfs that are in the failed list or blocklist will not be downloaded from the external mirror.
This script is designed to run for a given mirror and yyyymm. The yyyymm argument is optional and if no yyyymm is given,
the script will only sync with the given mirror for the current yyyymm. Run the script like:
  - `flock nssc_filelock -c 'sync_mirror_data.sh NSSC_holding NSSC yyyymm'` to sync with the NSSC server and
  - `flock bas_filelock -c 'sync_mirror_data.sh BAS_holding BAS yyyymm'` to sync with the BAS server
- **gatekeeper_globus.py** - This script acts on one of the three holding directories to transfer the rawacfs to the
SuperDARN Canada mirror via the globus_sdk python library. Below is a brief overview of this process, see the
SuperDARN Canada Wiki for details:
  - Remove any rawacfs in holding_dir if they are in the failed list or blocklist.
  - Hash the holding_dir and compare to the hash files on the mirror. Remove matching files from holding_dir and move
  nonmatching files to `nomatch/` subdirectory in holding_dir.
  - Check that all remaining rawacfs can be successfully unzipped and are not empty. Files who fail any of these tests
  are moved to `holding_dir/failed/`.
  - Update the failed files list `all_failed.txt` on the mirror and transfer these failed files to `local_data/failed/`
  on the mirror.
  - Transfer the remaining files in holding_dir to the mirror at `raw/yyyy/mm/` as they have passed all checks
  - Update yyyymm.hashes and master.hashes

  Run the script like:

  - python `-u gatekeeper_globus.py -d holding_dir -m mirror_dir`

- **batch_sync_mirror** - 

### Tools
