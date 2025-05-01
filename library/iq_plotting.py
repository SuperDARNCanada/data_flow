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
import datetime as dt

import h5py
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pydarnio
import os

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
                                    plot_filename, vmax, vmin, start_sample, end_sample, figsize, experiment):
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
    experiment: str
        Name of the experiment that collected the data.
    """

    start_time = dt.datetime.utcfromtimestamp(timestamps_array.values[0].astype(int) * 1e-9)
    end_time = dt.datetime.utcfromtimestamp(timestamps_array.values[-1].astype(int) * 1e-9)

    kw = {'width_ratios': [97, 3], 'height_ratios': [1, 4]}
    fig, ((ax1, cax1), (ax2, cax2)) = plt.subplots(2, 2, figsize=figsize, gridspec_kw=kw, layout='constrained',
                                                   sharex='col')
    fig.suptitle(f'{experiment}: {dataset_descriptor} Power - {start_time.strftime("%Y%m%d")} '
                 f'{start_time.strftime("%H:%M:%S")} to {end_time.strftime("%H:%M:%S")} UTC')

    tstamp_values = timestamps_array.values
    tstamps = [dt.datetime.utcfromtimestamp(x.astype(int) * 1e-9) for x in tstamp_values]

    # Plot the number of sequences per averaging period
    tstamp_indices = np.array([0] + np.cumsum(num_sequences_array).values[:-1].tolist(), dtype=int)
    ax1.plot(np.array(tstamps)[tstamp_indices], num_sequences_array.values)
    ax1.set_ylabel('# sequences')
    ax1.set_ylim(0)

    last_tstamp = tstamps[-1] + dt.timedelta(seconds=0.1)
    tstamps.append(last_tstamp)
    power = 20 * np.log10(np.abs(data_array.values.T))
    img = ax2.pcolormesh(
            tstamps,
            np.arange(start_sample - 0.5, end_sample + 0.5),
            power[start_sample:end_sample],
            cmap=plt.get_cmap('plasma'), vmax=vmax, vmin=vmin)
    ax2.set_ylabel('Sample number (Range)')
    ax2.set_xlabel('Timestamp')

    fig.colorbar(img, cax=cax2, label='Power (dB)')
    cax1.axis('off')

    ax2.sharex(ax1)
    print(plot_filename)
    plt.savefig(plot_filename, bbox_inches='tight')
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

    # Get the experiment name
    with h5py.File(antennas_iq_file, 'r') as f:
        experiment = f['metadata']['experiment_name'][()].decode('utf-8')

    # Read in the data from the file
    dset = pydarnio.BorealisV1Read.arrays_as_xarray(antennas_iq_file)
    data = dset['antennas_iq_data']
    num_antennas = data.shape[0]

    # typically, antenna names and antenna indices are the same except
    # where certain antennas were skipped in data writing for any reason.
    if antenna_nums is None or len(antenna_nums) == 0:
        antenna_indices = list(range(0, num_antennas))
    else:
        antenna_indices = []
        for antenna_num in antenna_nums:
            antenna_indices.append(list(dset['rx_antenna']).index(antenna_num))

    antenna_names = [f'antenna_{x}' for x in antenna_nums]

    sequences_data = dset['num_sequences']
    timestamps_data = dset['sqn_timestamps']

    print(antennas_iq_file)

    for antenna_idx, antenna_name in zip(antenna_indices, antenna_names):
        antenna_data = data[antenna_idx, :, :]
        plot_filename = f'{directory_name}/{time_of_plot}.{antenna_name}_{start_sample}_{end_sample}.jpg'
        plot_unaveraged_range_time_data(antenna_data, sequences_data, timestamps_data, antenna_name, plot_filename,
                                        vmax, vmin, start_sample, end_sample, figsize, experiment)


def main():
    antennas_iq_parser = iq_plotting_parser()
    args = antennas_iq_parser.parse_args()

    filename = args.antennas_iq_file

    antenna_nums = []
    if args.antennas is not None:
        antenna_nums = build_list_from_input(args.antennas)

    sizes = (12, 10)    # Default figsize
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
