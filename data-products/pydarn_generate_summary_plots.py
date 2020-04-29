# Copyright (C) 2019 SuperDARN Canada, University of Saskatchewan
# Author: Marina Schmidt
#
# Description: This script is used to generate summary plots for all
#              SuperDARN radars and for each beam for a given date
from glob import glob
import pydarn
import matplotlib.pyplot as plt
from collections import deque
import sys
import bz2
import warnings
import os


def read_files(date, radar, data_path):
    '''
    reads multiple compressed 2-hour fitacf files
    for the data and the radar

    :param: date - string format of yyyymmdd
    :param: radar - 3 letter radar abbreviation
    :param: data_path - absolute folder path to fitacf files

    :post: finds all fitacf files using glob for the given data
        and radar abbreviation then sorts the list. This ensures the
        times in the filenames are in order. Concatenates the data
        together.

    :return: data list of all records from the files read in
    '''
    year = date[0:4]
    month = date[4:6]
    data = deque()

    # get all fitacf files for the given data and radar
    fitacf_files = glob('{path}/{date}*{radar}*.fitacf.bz2'.format(path=data_path,date=date, radar=radar,year=year,month=month))

    # check if there was any files obtained
    if fitacf_files == []:
        raise Exception('No data for radar {}'.format(radar))

    # sort the files to ensure we are concatenating the records in order
    fitacf_files.sort()

    # sanity printing
    print("Reading in {radar} for {date} ...".format(radar=radar, date=date))
    # read in the compressed fitacf files using pyDARN
    for fitacf_file in fitacf_files:
        with bz2.open(fitacf_file) as fp:
            fitacf_stream = fp.read()

        reader = pydarn.SDarnRead(fitacf_stream, True)
        records = reader.read_fitacf()
        data += records
    print("Reading complete...")
    return data


def plot_files(data, date, radar, plot_path):
    """
    Plots each beam for the data from read_files
    into a summary plot and
    saves the plot as a png with the name:
        pydarn_<yyyymmdd>_<radar abbrev>.<if there is slice/channle>_bm<beam number>.png

    :param: data - list of records returned from read_files
    :param: date - string yyyymm for the plots filename
    :param: radar - 3 letter radar abbreviation for the plots filename

    :post: creates a directory to save files in if not created. Creates
           a png per beam for that radar on the given date.
    """
    year=date[0:4]
    month=date[4:6]

    # makes a directory to save files in if it doesn't exist
    os.makedirs("{}/".format(plot_path), exist_ok=True)

    # get number of beams to iteratre over
    beams = pydarn.SuperDARNRadars.radars[data[0]['stid']].hardware_info.beams
    filename = ''
    for beam in range(0, beams):
        try:
            # summary plots raise warnings on resolution
            # size, this is annoying so lets catch them
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pydarn.RTP.plot_summary(data, beam_num=beam)
            filename = '{plot_path}/pydarn_{date}_{radar}_bm{beam}.png'\
                    ''.format(plot_path=plot_path, year=year, month=month,
                              date=date, radar=radar, beam=beam)
            plt.savefig(filename)
            plt.close()
            print("Saved {} Summary plots to {} ...".format(radar, filename))
        # pyDARN will raise an error if no data is found for
        # that beam. Not all beams are used on all radars
        # especially during special modes
        except pydarn.rtp_exceptions.RTPNoDataFoundError:
            plt.close()
            continue
        # TODO: check what may raise this error?
        except pydarn.rtp_exceptions.RTPUnknownParameterError:
            plt.close()
            continue
        # Any other errors needs to be documented
        except Exception as err:
            print("Error: {}".format(err))
            plt.close()

# check number of arguements
# if not print usage message
if len(sys.argv) is not 5:
    print("Must supply one command line arguement")
    print("Example: python3 pydarn_generate_summary_plot.py 20190102 sas /data/fitacf_30 /plot/path/")
    exit(1)

date = sys.argv[1]
radar = sys.argv[2]
data_path = sys.argv[3]
plot_path = sys.argv[4]
print("starting")
try:
    print("reading "+str(date))
    data = read_files(date, radar, data_path)
    print("plotting")
    plot_files(data, date, radar, plot_path)
    # to be memory efficient
    del data
except Exception as err:
        print("Error: {}".format(err))
        exit(-1)

exit(0)

