# Copyright 2024 SuperDARN Canada, University of Saskatchewan
# Copied from antennas_iq plotting from borealis-data-utils repo
"""
Usage:

iq_plotting.py [-h] [--plot-directory]=[destination] antennas_iq_file

Pass in the antennas iq file you wish to plot and specify the directory
you would like the plot to be saved. Default is the working directory.
The antennas iq file can be in either site or array format.

The script will :
1. Identify if the iq file is site or array format and load in the data
     accordingly.
2. Calculates power and snr for each sample in a sequence.
3. Plots the un-averaged samples of each sequence in the file in succession

"""

import argparse

import datetime
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os

from pydarnio import BorealisRead

matplotlib.use('Agg')

def usage_msg():
    """
    Return the usage message for this process.

    This is used if a -h flag or invalid arguments are provided.

    :returns: the usage message
    """

    usage_message = """iq_plotting.py [-h] [--plot-directory]=[destination] antennas_iq_file

    Pass in the antennas iq file you wish to plot and specify the directory
    you would like the plot to be saved. Default is the working directory.
    The antennas iq file can be in either site or array format.

    The script will :
    1. Identify if the iq file is site or array format and load in the data
         accordingly.
    2. Calculates power and snr for each sample in a sequence.
    3. Plots the un-averaged samples of each sequence in the file in succession
    """

    return usage_message


def iq_plotting_parser():
    parser = argparse.ArgumentParser(usage=usage_msg())
    parser.add_argument("antennas_iq_file", help="Name of the file to plot.")
    parser.add_argument("--antennas", help="Antenna indices to plot. Format as --antennas=0,2-4,8")
    parser.add_argument("--max-power", help="Maximum Power of color scale (dB).", default=40.0, type=float)
    parser.add_argument("--min-power", help="Minimum Power of color scale (dB).", default=10.0, type=float)
    parser.add_argument("--start-sample", help="Sample Number to start at.", default=0, type=int)
    parser.add_argument("--end-sample", help="Sample Number to end at.", default=70, type=int)
    parser.add_argument("--plot-directory", help="Directory to save plots.", default='', type=str)
    parser.add_argument("--figsize", help="Figure dimensions in inches. Format as --figsize=10,6", type=str)
    return parser


def build_list_from_input(str_in: str):
    """
    Takes a string formatted like 0-2,3,5,7,9-12 and parses into a list containing all numbers,
    with endpoints of ranges included.

    Parameters
    ----------
    str_in: str
        Numbers to include. Individual numbers separated by commas are included, as well as ranges like 6-10
        which will include numbers 6, 7, 8, 9, and 10.

    Returns
    -------
    list[int]
        List of integers. For the example in the docstring, list would include [0, 1, 2, 3, 5, 7, 9, 10, 11, 12].
    """
    nums = []
    entries = str_in.split(',')
    for entry in entries:
        if '-' in entry:
            small_num, big_num = entry.split('-')
            nums.extend(range(int(small_num), int(big_num) + 1))
        else:
            nums.append(int(entry))

    return nums


def plot_unaveraged_range_time_data(data_array, num_sequences_array, timestamps_array, dataset_descriptor,
                                    plot_filename, vmax, vmin, start_sample, end_sample, figsize):
    """
    Plots data as range time given an array with correct dimensions. Also
    plots SNR by finding the ratio of max power in the sequence to average
    power of the 10 weakest range gates.

    Note that this plots unaveraged data. All sequences available from the
    record will be plotted side by side. Uses plasma color map.

    Parameters
    ----------
    data_array: ndarray
        Array with shape num_records x max_num_sequences x num_samps for some
        dataset.
    num_sequences_array: ndarray
        Array with shape num_records with the number of sequences per record.
    timestamps_array: ndarray
        Array of timestamps with dimensions num_records x max_num_sequences.
    dataset_descriptor: str
        Name for dataset, to be included in plot title.
    plot_filename: str
        Where to save plot.
    vmax: float
        Max power for the color bar on the plot.
    vmin: float
        Min power for the color bar on the plot.
    start_sample: int
        The sample to start plotting at.
    end_sample: int
        The last sample in the sequence to plot.
    figsize: tuple (float, float)
        The desired size (in inches) of the plotted figure.
    """

    (num_records, max_num_sequences, num_samps) = data_array.shape

    power_list = []  # list of lists of power
    timestamps = []  # list of timestamps
    noise_list = []  # list of (average of ten weakest ranges in sample range)
    max_snr_list = []  # max power - sequence noise (ave of 10 weakest ranges)

    for record_num in range(num_records):
        num_sequences = int(num_sequences_array[record_num])

        # get data for all sequences up to num sequences for this record.
        voltage_samples = data_array[record_num, :num_sequences, :]

        for sequence in range(num_sequences):
            timestamp = float(timestamps_array[record_num, sequence])
            timestamps.append(timestamp)

            # Get the raw power from the voltage samples
            power = np.abs(voltage_samples)[sequence]
            power_db = 10 * np.log10(power)
            power_list.append(power_db)

            # Average the 10 lowest power samples for this sequence, and call this the noise level
            sequence_noise_db = 10 * np.log10(np.average(np.sort(power)[:10]))
            noise_list.append(sequence_noise_db)

            # Max SNR = maximum power - noise level
            max_snr_list.append(np.max(power_db[2:]) - sequence_noise_db)

    power_array = np.array(power_list)

    start_time = datetime.datetime.utcfromtimestamp(timestamps[0])
    end_time = datetime.datetime.utcfromtimestamp(timestamps[-1])

    # take the transpose to get sequences x samps for the dataset
    new_power_array = np.transpose(power_array)

    kw = {'width_ratios': [95, 5], 'height_ratios': [1, 3]}
    fig, ((ax1, cax1), (ax2, cax2)) = plt.subplots(2, 2, figsize=figsize, gridspec_kw=kw)
    fig.suptitle(f'{dataset_descriptor} Raw Power Sequence Time {start_time.strftime("%Y%m%d")} '
                 f'{start_time.strftime("%H:%M:%S")} to {end_time.strftime("%H:%M:%S")} UT vs Range')

    # plot SNR and noise
    ax1.plot(range(len(max_snr_list)), max_snr_list)
    ax1.set_title('Max SNR in sequence')
    ax1.set_ylabel('SNR (dB)')

    img = ax2.imshow(new_power_array, aspect='auto', origin='lower', cmap=plt.get_cmap('plasma'), vmax=vmax, vmin=vmin)
    ax2.set_title(f'Range-time based on samples {start_sample} to {end_sample}')
    ax2.set_ylabel('Sample number (Range)')
    ax2.set_xlabel('Sequence number (spans time)')
    ax2.set_ylim(start_sample, end_sample)

    fig.colorbar(img, cax=cax2, label='Raw Power (dB)')
    cax1.axis('off')

    ax2.sharex(ax1)
    print(plot_filename)
    plt.savefig(plot_filename)
    plt.close()


def plot_antennas_range_time(antennas_iq_file, antenna_nums=None, vmax=40.0, vmin=10.0, start_sample=0,
                             end_sample=70, plot_directory='', figsize=(12, 10)):
    """
    Reads in antennas iq file (can be site or array type) data from echoes received in every sequence
    for a single antenna. and calls a function to create the antennas iq plot for the read in data.

    Plots the samples between start_sample and end_sample for every
    sequence in the file.

    Parameters
    ----------
    antennas_iq_file: str
        The filename that you are plotting data from for plot title.
    antenna_nums: list[int]
        List of antennas you want to plot. This is the antenna number
        as listed in the antenna_arrays_order. The index into the data array
        is determined by finding the index of the antenna number into the
        antenna_arrays_order list. The data array is organized main antennas
        first consecutively, followed by interferometer antennas consecutively.
        Default None, which allows the algorithm to plot all antennas available
        in the dataset.
    vmax: float
        Max power for the color bar on the plot, in dB. Default 40 dB.
    vmin: float
        Min power for the color bar on the plot, in dB.  Default 10 dB.
    start_sample: int
        The sample to start plotting at. Default 0th range (first sample).
    end_sample: int
        The last sample in the sequence to plot. Default 70 so ranges 0-69
        will plot.
    plot_directory: str
        The directory that generated plots will be saved in. Default '', which
        will save plots in the same location as the input file.
    figsize: tuple (float, float)
        The size of the figure to create, in inches across by inches tall. Default (12, 10)
    """
    basename = os.path.basename(antennas_iq_file)

    if plot_directory == '':
        directory_name = os.path.dirname(antennas_iq_file)
    elif not os.path.exists(plot_directory):
        directory_name = os.path.dirname(antennas_iq_file)
        print(f"Plot directory {plot_directory} does not exist. Using directory {directory_name} instead.")
    else:
        directory_name = plot_directory

    time_of_plot = '.'.join(basename.split('.')[0:6])

    # Try to guess the correct file structure
    basename = os.path.basename(antennas_iq_file)
    is_site_file = 'site' in basename

    reader = BorealisRead(antennas_iq_file, 'antennas_iq', 'array')
    arrays = reader.arrays

    (num_records, num_antennas, max_num_sequences, num_samps) = arrays['data'].shape

    # typically, antenna names and antenna indices are the same except
    # where certain antennas were skipped in data writing for any reason.
    if antenna_nums is None or len(antenna_nums) == 0:
        antenna_indices = list(range(0, num_antennas))
        antenna_names = list(arrays['antenna_arrays_order'])
    else:
        antenna_indices = []
        antenna_names = [f'antenna_{a}' for a in antenna_nums]
        for antenna_num in antenna_nums:
            antenna_indices.append(list(arrays['antenna_arrays_order']).index('antenna_' + str(antenna_num)))

    sequences_data = arrays['num_sequences']
    timestamps_data = arrays['sqn_timestamps']

    print(antennas_iq_file)

    if is_site_file:
        iterable = enumerate(antenna_names)
    else:
        iterable = zip(antenna_indices, antenna_names)

    for antenna_num, antenna_name in iterable:
        antenna_data = arrays['data'][:, antenna_num, :, :]
        # Antenna name from iterable tends to returns as type bytes, convert to string
        if isinstance(antenna_name, bytes):
            name = antenna_name.decode("ASCII")
        else:
            name = antenna_name

        plot_filename = f'{directory_name}/{time_of_plot}.{name}_{start_sample}_{end_sample}.jpg'

        plot_unaveraged_range_time_data(antenna_data, sequences_data, timestamps_data, name, plot_filename,
                                        vmax, vmin, start_sample, end_sample, figsize)


def main():
    antennas_iq_parser = iq_plotting_parser()
    args = antennas_iq_parser.parse_args()

    filename = args.antennas_iq_file

    antenna_nums = []
    if args.antennas is not None:
        antenna_nums = build_list_from_input(args.antennas)

    sizes = (32, 16)    # Default figsize
    if args.figsize is not None:
        sizes = []
        for size in args.figsize.split(','):
            sizes.append(float(size))
        if len(sizes) == 1:
            sizes.append(sizes[0])  # If they only pass in one size, assume they want a square plot.
        else:
            if len(sizes) > 2:
                print(f'Warning: only keeping {sizes[:2]} from input figure size.')
            sizes = sizes[:2]

    plot_antennas_range_time(filename, antenna_nums=antenna_nums, vmax=args.max_power, vmin=args.min_power,
                             start_sample=args.start_sample, end_sample=args.end_sample,
                             plot_directory=args.plot_directory, figsize=sizes)


if __name__ == '__main__':
    main()
