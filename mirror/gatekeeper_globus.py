# coding: utf-8
""" Last modified: 202410 by Saif Marei

 Last modification 202304 by Kevin Krieger
 ported from gatekeeper

 This script is designed to log on to the University of Saskatchewan globus
 SuperDARN mirror in order to upload rawacf files for a specific pattern.
 The script performs various checks on all specified rawacf files in a
 local holding directory and only transfers the files which pass all tests
 to the SuperDARN mirror. Files which fail any of the checks are then
 moved to an appropriate location, given the nature of the failure.

 Call the script like so with the following arguments:
 /path/to/script/gatekeeper_globus -d /path/to/local/holding/dir/ -m /path/to/mirror/root/ -p [pattern]
 Argument 1 is a path to a local holding directory with data you wish to put on the mirror
 Argument 2 is a path to the root of data mirror under which appear the directories for data type
 Argument 3 is the optional pattern, omit to sync all rawacf files
 Run
 python /path/to/script/gatekeeper_globus -h for more information on the usage of this script.

 The script needs to be run on the same machine that the globus personal endpoint is
 running on (i.e. the same machine where the local holding directory is located)
"""
from __future__ import print_function
import globus_sdk
from globus_sdk.scopes import TransferScopes
import inspect
from datetime import datetime, timedelta
from os.path import expanduser, isfile, getsize, isdir
from os import listdir, mkdir, remove, rename, stat
import shutil
import fnmatch
import sys
import subprocess
import time
# Import smtp library and email MIME function for email alerts
import smtplib
from email.mime.text import MIMEText
import pydarnio
import logging
import argparse

# Make sure there is only one instance running of this script
from tendo import singleton

me = singleton.SingleInstance()

HOME = expanduser("~")
TRANSFER_RT_FILENAME = f"{HOME}/.globus_transfer_rt"
PERSONAL_UUID_FILENAME = f"{HOME}/.globusonline/lta/client-id.txt"

if isfile(PERSONAL_UUID_FILENAME):
    with open(PERSONAL_UUID_FILENAME) as f:
        PERSONAL_UUID = f.readline().strip()

# Client ID retrieved from https://auth.globus.oorg/v2/web/developers
gatekeeper_app_CLIENT_ID = 'bc9d5b7a-6592-4156-bfb8-aeb0fc4fb07e'


def extendable_logger(log_name, file_name, level=logging.INFO):
    """ Will set up and format a logger referenced as such: logger.info(msg), logger.warning(msg), etc.

    :param log_name: The name of the log file to be written to
    :param file_name: The identifier by which to reference the log file, written in each log
    :param level: Base logging level, default to INFO
    :returns: Logger to be used
    """
    handler = logging.FileHandler(log_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    specified_logger = logging.getLogger(file_name)
    specified_logger.setLevel(level)
    specified_logger.addHandler(handler)
    return specified_logger


def month_year_iterator(start_month, start_year, end_month, end_year):
    """ Found on stackoverflow by user S.Lott.

    :param start_month: the month you wish to start your iterator on, integer
    :param start_year: the year you wish to start your iterator on, integer
    :param end_month: the month you wish to end your iterator on, integer
    :param end_year: the year you wish to end your iterator on, integer
    :returns: An iterator over a tuple of years and months
    """
    ym_start = 12 * start_year + start_month - 1
    ym_end = 12 * end_year + end_month - 1
    for ym in range(ym_start, ym_end):
        y, m = divmod(ym, 12)
        yield y, m + 1


def parse_data_filename(filename):
    """Parse a string representing a SuperDARN file name and return a tuple of the elements.
    (Year, Month, Day, Hour, Minute, Second, Radar abbreviation, data type, [channel])
    Note that the date/time elements are integers, the rest are strings.

    If there is a channel identifier, it is returned as the final element of the tuple,
    otherwise no channel identifier is returned.

    :param filename: String, example: "20200804.2200.01.mcm.a.rawacf.bz2"
    :return: Tuple of elements, example: (2020, 08, 04, 22, 00, 01, 'mcm', 'rawacf', 'a') or
    None if it was not possible to parse the input string. Note that the date values are all
    integers, and the rest are strings.
    """
    if not isinstance(filename, str):
        print(f"{filename} is not a string. Cannot parse filename. Returning None")
        return None

    elements = filename.split('.')

    year = int(filename[0:4])
    month = int(filename[4:6])
    day = int(filename[6:8])
    hour = int(filename[9:11])
    minute = int(filename[11:13])
    second = int(elements[2])
    abbrev = elements[3]

    channel = None
    data_type = None

    if len(elements) is 6:
        data_type = elements[4]
    elif len(elements) is 7:
        channel = elements[4]
        data_type = elements[5]
    else:  # We expect at least [yyyymmdd, hhmm, ss, rad, data_type, bz2] and optional channel
        print(f"Incorrect number of elements: {len(elements)}, for filename: {filename}. Expect 6 or 7")
        return None

    if channel:
        return year, month, day, hour, minute, second, abbrev, data_type, channel
    else:
        return year, month, day, hour, minute, second, abbrev, data_type


class Gatekeeper(object):
    """ This is the gatekeeper class. It knows about globus and will
    control data flow onto the mirror """

    # Add _test to 3rd argument in constructor below for testing purposes
    def __init__(self, client_id, client_secret=None, transfer_rt=None, working_dir=f"{HOME}/tmp/"):
        """ Initialize member variables, check arguments, etc..

        :param client_id: retrieved from "Manage Apps" section of
        https://auth.globus.org/v2/web/developers for this app. str
        :param client_secret: same as client_id, defaults to None
        :param transfer_rt: is given by manually authenticating via the get_auth_with_login
        function. Defaults to None. str
        :param working_dir: A temporary working directory for the script. Defaults to tmp in the
        home directory. Cleared upon init. str"""
        self.CLIENT_ID = client_id
        self.CLIENT_SECRET = client_secret
        self.TRANSFER_RT = transfer_rt

        self.transfer_rt_filename = TRANSFER_RT_FILENAME
        self.last_transfer_result = None
        self.working_dir = working_dir

        self.holding_dir = None
        self.mirror_root_dir = None
        self.sync_pattern = None
        # Add _test for testing purposes
        self.mirror_failed_dir = '/project/6008057/sdarn/local_data/failed/'  # TODO: This is hacky, should be handled better, using the input args or something

        self.cur_year = datetime.now().year
        self.cur_month = datetime.now().month
        self.cur_day = datetime.now().day
        self.cur_hour = datetime.now().hour
        self.cur_minute = datetime.now().minute
        self.cur_date = datetime.now().strftime("%Y%m%d")
        self.possible_data_types = ['raw', 'dat']
        
        # Potential consents required (new as of Globus endpoints v5 - see here: https://globus-sdk-python.readthedocs.io/en/stable/examples/minimal_transfer_script/index.html#example-minimal-transfer)
        self.consents = []

        # Get a transfer client
        # Note that this uuid is the new cedar globus version 5 uuid, and hardcoded here due to hacking
        # this shit together in a quick timeframe. Ideally this would be searched and found programmatically via the function below "get_superdarn_mirror_uuid, which works to get the correct uuid, but we need a transfer client to use it, but we need the uuid to get a transfer client... so yeah, chicken and egg"
        self.mirror_uuid = '8dec4129-9ab4-451d-a45f-5b4b8471f7a3'
        #self.mirror_uuid = '88cd829c-75fa-44e6-84bb-42e6250afaea'
        #self.mirror_uuid = "bc9d5b7a-6592-4156-bfb8-aeb0fc4fb07e"
        self.transfer_client = self.get_transfer_client()

        # Email information ##########################################################
        # smtpServer is the host to use that will actually send the email
        # emailFlag is set to 1 if a condition arises that requires an email alert
        # emailMessage is initialized to nothing here, and filled in with an
        #       appropriate message depending upon the reason for the email.
        self.email_recipients = ['saif.marei@usask.ca']
        self.email_from = 'superdarn-cssdp'
        self.current_time = datetime.now()
        self.email_subject = '[Gatekeeper Globus] ' + self.current_time.strftime("%Y%m%d.%H%M : ")
        self.smtp_server = 'localhost'
        self.email_message = ''

        # Check if there is a current transfer, that could be bad
        if self.check_for_transfer_to_endpoint(self.mirror_uuid):
            email_msg = "Error: current active transfer to mirror"
            self.email_subject += email_msg
            self.send_email()
            sys.exit(email_msg)

    def get_mirror_uuid(self):
        """ :return: The UUID of the mirror Globus endpoint """
        return self.mirror_uuid

    def send_email(self, message=None, subject=None, from_address=None, recipients=None):
        """
            Send an email with subject and message to recipients, from from_address.
            Used to report on results of the gatekeeper script activity
        :param message: String representing body of email
        :param subject: String representing the subject line of email
        :param from_address: Where are we sending email from?
        :param recipients: Who to send the email to (list of email addresses)
        """
        # Fill out MIME text with the body, Subject, From and To fields
        if subject is None:
            subject = self.email_subject
        if message is None:
            message = self.email_message
        if from_address is None:
            from_address = self.email_from
        if recipients is None:
            recipients = self.email_recipients
        email = MIMEText(message)
        email['Subject'] = subject
        email['From'] = from_address
        email['To'] = ', '.join(recipients)

        # Get an smtp object to send the email
        s = smtplib.SMTP(self.smtp_server)
        s.sendmail(from_address, recipients, email.as_string())
        s.quit()

    def set_holding_dir(self, holding_dir):
        """ :param holding_dir: A directory where data files exist to be uploaded to mirror """
        self.holding_dir = holding_dir
        if self.holding_dir[-1] is not '/':
            self.holding_dir += "/"

    def set_mirror_root_dir(self, mirror_root_dir):
        """ :param mirror_root_dir: The root directory of mirror, under which appear data dirs """
        self.mirror_root_dir = mirror_root_dir

    def set_sync_pattern(self, sync_pattern):
        """ :param sync_pattern: String: the sync pattern to be used to select files for upload """
        self.sync_pattern = sync_pattern

    def get_possible_data_types(self):
        """ :returns: a python list of the possible data types """
        return self.possible_data_types

    def check_for_current_transfer(self):
        """ Checks transfer client's task list to see if any of them are currently active

        :returns: True if there are any active transfers, False otherwise."""
        response = self.transfer_client.task_list()
        for task in response:
            if 'ACTIVE' in task['status']:
                return True
        return False

    def check_for_transfer_to_endpoint(self, uuid):
        """ Checks for current transfer to the endpoint given by UUID

        :returns: True if there are any active transfers to uuid, False otherwise.
        """
        response = self.transfer_client.task_list()
        for task in response:
            if 'ACTIVE' in task['status'] and uuid in task['destination_endpoint_id']:
                print(f"Task label: {task['label']}")
                print(f"Task dest EP name: {task['destination_endpoint_display_name']}")
                return True
        return False

    def get_working_dir(self):
        """:returns: String representing the working directory of the class"""
        return self.working_dir

    def get_blocklist_dir(self):
        """:returns: String representing the blocklist directory on the mirror"""
        return f"{self.mirror_root_dir}/blocklist"

    def get_holding_dir(self):
        """:returns: String representing the holding directory which contains data to be sync'd"""
        return self.holding_dir

    def get_mirror_root_dir(self):
        """:returns: String representing the mirror root directory under which appear directories
        for data type"""
        return self.mirror_root_dir

    def get_sync_pattern(self):
        """:returns: String representing the sync pattern to be used to select files for upload"""
        return self.sync_pattern

    def get_refresh_token_authorizer(self):
        """ Called when there is a refresh token available.

        :returns: globus sdk authorizer object"""
        # Get client from globus sdk to act on
        client = globus_sdk.NativeAppAuthClient(self.CLIENT_ID)
        client.oauth2_start_flow(refresh_tokens=True, requested_scopes=TransferScopes.all)

        # Get authorizer that handles the refreshing of token
        return globus_sdk.RefreshTokenAuthorizer(self.TRANSFER_RT, client)

    def get_client_secret_authorizer(self):
        """ Called when there is a client secret available.

        :returns: globus sdk authorizer object"""
        client = globus_sdk.ConfidentialAppAuthClient(self.CLIENT_ID, self.CLIENT_SECRET)
        token_response = client.oauth2_client_credentials_tokens()

        # the useful values that you want at the end of this
        # globus_auth_data = token_response.by_resource_server['auth.globus.org']
        globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']
        # globus_auth_token = globus_auth_data['access_token']
        globus_transfer_token = globus_transfer_data['access_token']

        return globus_sdk.AccessTokenAuthorizer(globus_transfer_token)

    def get_auth_with_login(self, consents=TransferScopes.all):
        """ Called when there no refresh token or client secret available. Requires manual
        authentication and will get and save a refresh token for future automatic authentication.
        *Note* The refresh token generated is a lifetime credential and should be kept secret

        :param: consents: globus sdk version 5 endpoints require consent scopes, this is the list
                          of those that we will request, to access the endpoints and paths we need
        :returns: globus sdk authorizer object"""
        client = globus_sdk.NativeAppAuthClient(self.CLIENT_ID)
        client.oauth2_start_flow(refresh_tokens=True, requested_scopes=consents)

        authorize_url = client.oauth2_get_authorize_url()
        print(f'Please go to this URL and login: {authorize_url}')

        auth_code = input('Please enter the code you get after login here: ').strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)

        # the useful values that you want at the end of this
        globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']
        globus_transfer_token = globus_transfer_data['access_token']
        # Native apps - transfer_rt are refresh tokens and are lifetime credentials,
        # so they should be kept secret. The consents for these credentials can be seen at
        # https://auth.globus.org/v2/web/consents
        print(f"Here is your refresh token: {globus_transfer_data['refresh_token']}. It has been written to the file {self.transfer_rt_filename}")
        with open(self.transfer_rt_filename, 'w') as transfer_rt_file:
            transfer_rt_file.write(globus_transfer_data['refresh_token'])

        print("Note that refresh tokens are lifetime credentials, so they should be kept secret. "
              "The consents for these credentials are at https://auth.globus.org/v2/web/consents")

        return globus_sdk.AccessTokenAuthorizer(globus_transfer_token)

    def get_transfer_client(self):
        """Call this function to get a transfer client for the globus python sdk

        :returns: A globus python sdk TransferClient object"""
        if self.TRANSFER_RT is not None:
            return globus_sdk.TransferClient(authorizer=self.get_refresh_token_authorizer())
        elif self.CLIENT_SECRET is not None:
            return globus_sdk.TransferClient(authorizer=self.get_client_secret_authorizer())
        else:
            return globus_sdk.TransferClient(authorizer=self.get_auth_with_login())

    def check_for_consent_required(self, ep_uuid=None, path=None):
        """Call this function with all endpoint uuids that you're going to use (source and destination) 
        along with all paths to be used, so that we get all the required consents at the beginning.
        New and required as of Globus endpoints v5. This modifies the self.consents list by extending it
        Defaults to the mirror endpoint and root directory, but can and should also be called on the
        other endpoint(s) you want to transfer to/from.

        :param: ep_uuid: UUID of the endpoint you want to find required consent scopes for
        :param: path: the path on the endpoint you want to find the required consent scopes for """
        if ep_uuid is None:
            ep_uuid = self.mirror_uuid
        if path is None:
            path = self.mirror_root_dir
        try:
            self.transfer_client.operation_ls(ep_uuid, path=path)
        # If there's an exception due to lack of consents, then add the consent scopes required
        # to our list so we can use them all a second time to login
        except globus_sdk.TransferAPIError as err:
            if err.info.consent_required:
                self.consents.extend(err.info.consent_required.required_scopes)

    def sync_files_from_list(self, files_to_sync,
                             source_uuid=PERSONAL_UUID, dest_uuid=None, data_type=None):
        """Will synchronize files from a one endpoint to another in the appropriate mirror
        directory structure. Emails user if it fails.

        :param files_to_sync: A list of data file names to upload to the dest_uuid endpoint
        :param source_uuid: UUID of endpoint that files are on. Default PERSONAL_UUID.
        :param dest_uuid: UUID of endpoint to sync files to.
        :param data_type: One of the possible data types
        :returns: Globus python sdk transfer result object or None if there were no files"""
        if len(files_to_sync) < 1:
            return None
        if data_type is None:
            data_type = 'raw'
        if dest_uuid is None:
            dest_uuid = self.mirror_uuid
        function_name = inspect.currentframe().f_code.co_name
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=function_name, sync_level="checksum",
                                                notify_on_succeeded=False, notify_on_failed=True)
        for holding_file in files_to_sync:
            dest_dir_prefix = f"{self.mirror_root_dir}/{data_type}/{holding_file[0:4]}/{holding_file[4:6]}/"
            transfer_data.add_item(f"{self.holding_dir}/{holding_file}",
                                   f"{dest_dir_prefix}/{holding_file}")
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def sync_failed_files_from_list(self, files_to_sync, source_uuid=PERSONAL_UUID, dest_uuid=None):
        """Will synchronize failed files from a one endpoint to another in the appropriate mirror
        directory structure. Emails user if it fails.

        :param files_to_sync: A list of failed data file names to upload to the dest_uuid endpoint
        :param source_uuid: UUID of endpoint that files are on. Default PERSONAL_UUID.
        :param dest_uuid: UUID of endpoint to sync files to.
        :returns: Globus python sdk transfer result object or None if there were no files"""
        if len(files_to_sync) < 1:
            return None
        if dest_uuid is None:
            dest_uuid = self.mirror_uuid
        function_name = inspect.currentframe().f_code.co_name
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=function_name, sync_level="checksum",
                                                notify_on_succeeded=False, notify_on_failed=True)
        for failed_file_from_list in files_to_sync:
            elements = parse_data_filename(failed_file_from_list)
            if elements is None:
                dest_dir_prefix = f"{self.mirror_failed_dir}/"
            else:
                dest_dir_prefix = f"{self.mirror_failed_dir}/{elements[6]}/"
            if not self.create_new_dir(dest_dir_prefix):
                return None  # Failed to create directory, so we can't upload these files
            transfer_data.add_item(f"{self.holding_dir}/{failed_file_from_list}",
                                   f"{dest_dir_prefix}/{failed_file_from_list}")
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def put_master_hashes(self, source_path=None,
                          source_uuid=PERSONAL_UUID, dest_uuid=None):
        """Sync master.hashes file from one endpoint to another in the appropriate mirror directory
        structure. Emails user if it fails.

        :param source_path: Where is the master.hashes file to upload? Defaults to working dir
        :param source_uuid: UUID of endpoint that file is on. Default PERSONAL_UUID.
        :param dest_uuid: UUID of endpoint to sync file to. Default self.mirror_uuid.
        :returns: Globus python sdk transfer result object"""
        if dest_uuid is None:
            dest_uuid = self.mirror_uuid
        if source_path is None:
            source_path = self.working_dir
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        transfer_data.add_item(f"{source_path}master.hashes",
                               f"{self.mirror_root_dir}/.config/master.hashes")
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def put_hashes(self, year, month, data_type="raw",
                   source_path=None, source_uuid=PERSONAL_UUID,
                   dest_uuid=None):
        """
        Sync hashes file from one endpoint to another in the appropriate mirror directory structure.
        Emails user if it fails.

        :param year: Year of hash file to upload
        :param month: Month of hash file to upload
        :param data_type: Default 'raw'. Which data type are we working with? typically dat or raw
        :param source_path: Where is the hashes file to upload? Defaults to working dir
        :param source_uuid: UUID of endpoint that file is on. Default PERSONAL_UUID.
        :param dest_uuid: UUID of endpoint to sync file to. Default self.mirror_uuid.
        :return: Globus python sdk transfer result object."""
        if dest_uuid is None:
            dest_uuid = self.mirror_uuid
        if source_path is None:
            source_path = self.working_dir
        dest_path = f"{self.mirror_root_dir}/{data_type}/{int(year):04d}/{int(month):02d}/{int(year):04d}{int(month):02d}.hashes"
        source_path += f"{int(year):04d}{int(month):02d}.hashes"
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        transfer_data.add_item(source_path, dest_path)
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def ls(self, ep_uuid, path=None):
        """
        Convenience function to print directory contents with typical outputs (permissions, user,
        group, name, type, timestamp)

        :param ep_uuid: UUID of endpoint to list directory contents on
        :param path: Path to list directory contents of. Defaults to None, or the root directory.
        """
        for entry in self.transfer_client.operation_ls(ep_uuid, path=path):
            print(f"{entry['permissions']} {entry['user']}:{entry['group']} {entry['name']} {entry['type']} {entry['last_modified']}")

    def get_file_list(self, year, month, data_type="raw", source_uuid=None):
        """ Gets a list of data files of a given type from an endpoint for a given year and month

        :param year: Year that you want to get data list for
        :param month: Month that you want to get data list for
        :param data_type: Data type that you want to get data list for
        :param source_uuid: Endpoint that you want to get data list from
        :return: python list of data files that match year, month, data type
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        path = f"{self.mirror_root_dir}/{data_type}/{year}/{month}/"
        return [ls['name'] for ls in self.transfer_client.operation_ls(source_uuid, path=path)]

    def print_endpoint(self, ep_uuid):
        """Convenience function that will print out some information about the endpoint given

        :param ep_uuid: UUID of the endpoint to print information about
        """
        for ep in self.transfer_client.endpoint_search(ep_uuid, filter_scope="my-endpoints"):
            print(f"[{ep['id']}] {ep['display_name']}")

    def get_hashes(self, year, month, data_type="raw",
                   dest_path=None, source_uuid=None,
                   dest_uuid=PERSONAL_UUID):
        """Retrieve a hashes file from an endpoint given year, month, data type, destination path,
        and endpoint UUIDs. Emails user if it fails.

        :param year: Year of the hashes file you wish to retrieve
        :param month: Month of the hashes file you wish to retrieve
        :param data_type: Data type of the hashes file you wish to retrieve
        :param dest_path: Destination path you want the hashes file to be synched to
        :param source_uuid: UUID of endpoint that contains the hashes file
        :param dest_uuid: UUID of endpoint to transfer hashes file to
        :return: Globus python sdk transfer result object
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        if dest_path is None:
            dest_path = self.working_dir
        source_path = f"{self.mirror_root_dir}/{data_type}/{int(year):04d}/{int(month):02d}/{int(year):04d}{int(month):02d}.hashes"
        dest_path += f"{int(year):04d}{int(month):02d}.hashes"
        deadline_1min = str(datetime.now() + timedelta(minutes=1))
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True, deadline=deadline_1min)
        transfer_data.add_item(source_path, dest_path)
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def get_master_hashes(self, dest_path=None,
                          source_uuid=None, dest_uuid=PERSONAL_UUID):
        """Retrieve the master hashes file from an endpoint given destination path & endpoint UUIDs
        Emails user if it fails.

        :param dest_path: Destination path you want the hash file to be synched to
        :param source_uuid: UUID of endpoint that contains the master.hashes file
        :param dest_uuid: UUID of endpoint to transfer master.hashes file to
        :return: Globus python sdk transfer result object
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        if dest_path is None:
            dest_path = self.working_dir
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        transfer_data.add_item(f"{self.mirror_root_dir}/.config/master.hashes",
                               f"{dest_path}master.hashes")
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def update_master_hashes(self, source_path=None, source_uuid=PERSONAL_UUID,
                             dest_uuid=None):
        """ Update the master.hashes file. Emails user if it fails

        :param source_uuid: UUID of endpoint that will generate the master.hashes file
        :param dest_uuid: UUID of endpoint to transfer updated master.hashes file to
        :param source_path: Source path on the source endpoint where master.hashes file will be
        synced from. Defaults to working directory.
        :return: Globus python sdk transfer result object """
        if dest_uuid is None:
            dest_uuid = self.mirror_uuid
        if source_path is None:
            source_path = self.working_dir

        # Clear the master hashes file for recalculation
        master_hashes_file_path = f"{source_path}/master.hashes"
        open(master_hashes_file_path, 'w').close()
        for data_type in self.get_possible_data_types():
            data_type_path = f"{source_path}/{data_type}/"
            try:
                mkdir(data_type_path)
            except OSError as error:
                print(f"Error trying to make directory {data_type_path}: {error}")
            self.get_hashes_all(data_type=data_type,
                                dest_path=data_type_path)
            while not self.wait_for_last_task(timeout_s=600):
                print("Still waiting for last task...")
            hash_process = subprocess.Popen(f"cd {gk.get_working_dir()}; sha1sum ./{data_type}/*hashes",
                                            shell=True, stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
            hash_process_out, hash_process_err = hash_process.communicate()
            hash_process_output = hash_process_out.decode().split("\n")
            with open(master_hashes_file_path, 'a+') as master_hash_file:
                master_hash_file.write('\n'.join(hash_process_output))
        return self.put_master_hashes(source_path, source_uuid, dest_uuid)

    def print_last_tasks(self):
        """ Convenience function. Prints out the last few tasks for the class transfer client"""
        for task in self.transfer_client.task_list(num_results=5, filter="type:TRANSFER,DELETE"):
            print(task["label"], task["task_id"], task["type"], task["status"])

    def print_endpoints(self):
        """Convenience function. Prints out information about the user's endpoints"""
        print("My endpoints:")
        for ep in self.transfer_client.endpoint_search(filter_scope="my-endpoints"):
            print(f"[{ep['id']}] {ep['display_name']}")

    def get_hashes_range(self, start_year, start_month, end_year,
                         end_month, data_type="raw", dest_path=None,
                         source_uuid=None, dest_uuid=PERSONAL_UUID):
        """ This function will get all hashes files that exist within a range of given months.
        Emails user if it fails

        :param start_year: Start year to retrieve hashes files for
        :param start_month: Start month to retrieve hashes files for
        :param end_year: End year to retrieve hashes files for
        :param end_month: End month to retrieve hashes files for
        :param data_type: Data type of hashes files to retrieve
        :param dest_path: Destination path to transfer all hashes files to on destintaion endpoint
        :param source_uuid: UUID of source endpoint to transfer files from, default self.mirror_uuid
        :param dest_uuid: UUID of destination endpoint to transfer files to
        :return: Globus python sdk transfer result object or None
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        if dest_path is None:
            dest_path = self.working_dir

        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        month_year_iter = month_year_iterator(start_month, start_year, end_month + 1, end_year)
        at_least_one_file = False
        for year_month in month_year_iter:
            year = year_month[0]
            month = year_month[1]
            source_path = f"{self.mirror_root_dir}/{data_type}/{int(year):04d}/{int(month):02d}/{int(year):04d}{int(month):02d}.hashes"
            final_dest_path = f"{dest_path}{int(year):04d}{int(month):02d}.hashes"

            if self.check_for_file_existence(source_path):
                at_least_one_file = True
                transfer_data.add_item(source_path, final_dest_path)
        if at_least_one_file:
            transfer_result = self.transfer_client.submit_transfer(transfer_data)
            self.last_transfer_result = transfer_result
            print(f"Getting at least one file. Transfer result: {transfer_result}")
            return transfer_result
        else:
            print(f"No hashes files for data type: {data_type}")
            return

    def get_hashes_all(self, data_type="raw", dest_path=None,
                       source_uuid=None, dest_uuid=PERSONAL_UUID):
        """ Convenience function to get all hash files from a given endpoint for a given data type
        Emails user if it fails

        :param data_type: Typically 'raw'. Default is 'raw'
        :param dest_path: Destination path to transfer all hashes files to, defaults to working dir
        :param source_uuid: UUID of source endpoint to transfer files from, default self.mirror_uuid
        :param dest_uuid: UUID of destination endpoint to transfer files to,
        defaults to PERSONAL_UUID
        :return: Globus python sdk transfer result object
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        if dest_path is None:
            dest_path = self.working_dir
        curyear = datetime.now().year
        curmonth = datetime.now().month
        if data_type == 'raw':
            start_year = 2005
            start_month = 6
            end_year = curyear
            end_month = curmonth
        elif data_type == 'dat':
            start_year = 1993
            start_month = 9
            end_year = 2006
            end_month = 7
        else:
            email_msg = f"Invalid data type: {data_type}"
            self.email_subject += email_msg
            self.send_email()
            sys.exit(email_msg)
        transfer_result = self.get_hashes_range(start_year, start_month, end_year, end_month,
                                                data_type, dest_path, source_uuid, dest_uuid)
        self.last_transfer_result = transfer_result
        return transfer_result

    def get_failed(self, dest_path=None, source_uuid=None, dest_uuid=PERSONAL_UUID):
        """ Get the failed file containing all files that have failed

        :param dest_path: Destination path to transfer the failed file to,
        defaults working dir
        :param source_uuid: UUID of the source endpoint to transfer failed file from,
        defaults to self.mirror_uuid
        :param dest_uuid: UUID of the destination endpoint to transfer failed file to,
        defaults to PERSONAL_UUID
        :return: Globus python sdk transfer result object
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        if dest_path is None:
            dest_path = self.working_dir
        dest_path += "/all_failed.txt"
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        transfer_data.add_item(f"{self.mirror_root_dir}/.config/all_failed.txt", dest_path)
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def update_failed(self, add_failed_files, failed_update_files=None, source_uuid=PERSONAL_UUID,
                      dest_uuid=None):
        """  Update the failed files list on the mirror with a dict of failed files

        :param add_failed_files: Dict - keys: filenames; values: tuple of (hash, reason for failure)
        :param failed_update_files: Path to file containing list of currently failed files
        :param source_uuid: UUID of the endpoint to transfer an updated list of failed files from
        :param dest_uuid: UUID of the endpoint to transfer an updated list of failed files to
        :return: Globus python sdk transfer result object
        """
        if source_uuid is None:
            source_uuid = PERSONAL_UUID
        if dest_uuid is None:
            dest_uuid = self.mirror_uuid
        if failed_update_files is None:
            failed_update_files = f"{self.get_working_dir()}/all_failed.txt"

        # Make sure that the failed_files file exists, or if None, then move on
        if failed_update_files:
            if not isfile(failed_update_files):
                print(f"Error: {failed_update_files} is not a file, cannot update failed files list")
                return None
            if stat(failed_update_files).st_size == 0:  # The file should never be empty
                print(f"Error: {failed_update_files} is empty, cannot update failed files list")
                return None
            # Add the list of failed files to the current list
            with open(failed_update_files) as file_of_failed_updates:
                filetext = file_of_failed_updates.readlines()
            with open(failed_update_files, 'a+') as file_of_failed_updates:
                # cat the list 'add_failed_files' to the file 'file_of_failed_files', keep unique
                for filename_key in add_failed_files:
                    for line in filetext:
                        if add_failed_files[filename_key][0] in line and filename_key in line:
                            print(f"'{line}' already in all_failed.txt file, moving on")
                            break
                    else:
                        file_of_failed_updates.write(f"{add_failed_files[filename_key][0]}  {filename_key} | {add_failed_files[filename_key][1]}\n")
                # Now upload the file to mirror
                transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid,
                                                        dest_uuid,
                                                        label=inspect.currentframe().f_code.co_name,
                                                        sync_level="checksum",
                                                        notify_on_succeeded=False,
                                                        notify_on_failed=True)
                transfer_data.add_item(f"{self.get_working_dir()}/all_failed.txt",
                                       f"{self.mirror_root_dir}/.config/all_failed.txt")
                transfer_result = self.transfer_client.submit_transfer(transfer_data)
                self.last_transfer_result = transfer_result
                return transfer_result
        else:
            # If we don't have a failed files list, that's an error
            raise FileNotFoundError("Error: No failed files list from mirror, cannot update.")

    def get_blocklist(self, dest_path=None, source_uuid=None, dest_uuid=PERSONAL_UUID):
        """ Get the blocklist directory containing all files that list blocked data files

        :param dest_path: Destination path to transfer the blocklist directory to,
        defaults working dir
        :param source_uuid: UUID of the source endpoint to transfer blocklist directory from,
        defaults to self.mirror_uuid
        :param dest_uuid: UUID of the destination endpoint to transfer blocklist directory to,
        defaults to PERSONAL_UUID
        :return: Globus python sdk transfer result object
        """
        if source_uuid is None:
            source_uuid = self.mirror_uuid
        if dest_path is None:
            dest_path = self.working_dir
        transfer_data = globus_sdk.TransferData(self.transfer_client, source_uuid, dest_uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        transfer_data.add_item(f"{self.mirror_root_dir}/.config/blocklist",
                               dest_path, recursive=True)
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        return transfer_result

    def wait_for_last_task(self, timeout_s=60, poll_s=15):
        """ Wait for the last transfer task to complete, given a timeout and poll time in seconds.

        :param timeout_s: How long to wait before timing out, in seconds. Doesn't cancel the task
        :param poll_s: Wait this long between polling the task to determine if it's done, in seconds
        :return: True if the task completed within the timeout, False otherwise
        """
        if self.last_transfer_result is None:
            print("Error. No last transfer, returning.")
            return
        task_id = self.last_transfer_result["task_id"]
        return self.transfer_client.task_wait(task_id, timeout=timeout_s, polling_interval=poll_s)

    def list_of_files_to_upload(self):
        """ Gets a python list of data files to upload to the mirror. Uses the holding directory
         and the sync pattern.

        :return: Python list of files in the holding directory matching the sync_pattern
        """
        files = []
        for holding_file in listdir(self.holding_dir):
            if fnmatch.fnmatch(holding_file, self.sync_pattern):
                files.append(holding_file)
        return files

    def check_for_file_existence(self, file_path, uuid=None):
        """ Check to see if a file exists or not on an endpoint given by UUID.
        *NOTE* The developers of the Globus python sdk have indicated they will be implementing
        a file existence check functionality in a future version of the SDK, so the implementation
        of this function will possibly change in the future to the sdk method.

        :param file_path: Path to the file or directory you wish to test existence of
        :param uuid: UUID of endpoint you want to test for file existence on.
        :return: True if the file or directory exists, False otherwise.
        """
        maximum_retries = 5
        retries = 0
        errormsg = None
        while retries < maximum_retries:
            if uuid is None:
                uuid = self.mirror_uuid
            try:  # TODO: Check for error.code in case msg changes.
                self.transfer_client.operation_ls(uuid, path=file_path)
                return True  # This means it was not a file and it did exist
            except globus_sdk.TransferAPIError as error:
                print(error)
                if error.message.find("not found on endpoint") != -1:
                    return False  # This means it doesn't exist
                elif error.message.find("is a file") != -1:
                    return True  # Means it raised an exception, but msg said it is a single file
                elif error.message.find("is not a directory") != -1:
                    return True  # Means it raised an exception, but msg said it is not a directory
                else:
                    # Not sure what this means so retry, then fail hard.
                    errormsg = str(error)
                    time.sleep(5)
                    retries += 1
            except globus_sdk.NetworkError as error:
                # Not good, can't make assumptions about whether the file or directory exists
                # Retry a few times, then fail hard.
                time.sleep(5)
                retries += 1
        email_msg = f"Checking for file existence failed after {retries} retries. Exiting!"
        if errormsg is not None:
            email_msg = f"{email_msg}\n{errormsg}"
        self.email_subject += "Check for file existence failed"
        self.email_message += email_msg
        self.send_email()
        sys.exit(email_msg)

    def get_task_successful_transfers(self):
        """ Get the last transfer's successfully transferred files list. Will not contain files
        that were skipped due to checksums matching. Need to use the paginated version of the
        call, or if we have over 100 file transfers it will only return the first 100.

        :return: Python list of files that were successfully transferred during the last transfer
        """
        return self.transfer_client.paginated.task_successful_transfers(
                self.last_transfer_result['task_id']).items()

    def get_hash_file_path(self, year, month, data_type="raw"):
        """ Retrieve the correct path string to the hashes file for the given year and month

        :param year: Year for the hash file. Integer
        :param month: Month for the hash file. Integer
        :param data_type: Defaults to 'raw'. Typically 'raw' or 'dat'. String
        :return: String representing the path to the hashes file
        """
        return f"{self.mirror_root_dir}/{data_type}/{int(year):04d}/{int(month):02d}/{int(year):04d}{int(month):02d}.hashes"

    def create_new_dir(self, path, uuid=None):
        """ Create a new directory given by path on an endpoint given by UUID

        :param path: string representing a path to create a directory on the given endpoint
        :param uuid: UUID of the endpoint to create the directory on
        :returns: True if the directory existed or was successfully created. False otherwise
        """
        if uuid is None:
            uuid = self.mirror_uuid
        try:
            self.transfer_client.operation_mkdir(uuid, path)
        except globus_sdk.GlobusAPIError as error:
            if error.http_status == 502:
                # This means that the directory already exists
                print(f"Directory {path} already existed.")
            else:
                print(f"Failed to create {path} directory.")
                return False
        return True

    def create_new_data_dir(self, year, month, data_type="raw", uuid=None):
        """ Create a new directory on an endpoint given by UUID for the year, month and data type.

        :param year: What year should the new directory be under?
        :param month: What month should the new directory be for?
        :param data_type: What data type should the new directory be for? Defaults to 'raw'.
        :param uuid: UUID of the endpoint to create the directory on
        :returns: True if the directory existed or was successfully created. False otherwise
        """
        if uuid is None:
            uuid = self.mirror_uuid
        year_path = f"{self.mirror_root_dir}/{data_type}/{int(year):04d}/"
        month_path = f"{year_path}/{int(month):02d}"
        try:
            self.transfer_client.operation_mkdir(uuid, year_path)
        except globus_sdk.GlobusAPIError as error:
            if error.http_status == 502:
                # This means that the directory already exists
                print(f"Directory {year_path} already existed.")
            else:
                print(f"Failed to create {year_path} directory.")
                return False
        try:
            self.transfer_client.operation_mkdir(uuid, month_path)
        except globus_sdk.GlobusAPIError as error:
            if error.http_status == 502:
                # This means the directory already exists
                print(f"Directory {month_path} already existed.")
            else:
                print(f"Failed to create {month_path} directory.")
                return False
        return True

    def get_superdarn_mirror_uuid(self):
        """ Will search endpoints and retrieve the UUID of the SuperDARN mirror endpoint.

        :returns: UUID of SuperDARN mirror endpoint """
        for ep in self.transfer_client.endpoint_search('Digital Research Alliance of Canada Cedar GCSv5'):
            if 'globus@tech.alliancecan.ca' in ep['contact_email']:
                print(f"Mirror UUID: {ep['id']}")
                return ep['id']

        email_msg = "Mirror endpoint not found"
        self.email_subject += email_msg
        self.send_email()
        sys.exit(email_msg)

    def get_num_files_skipped(self):
        """ :returns: The number of files that were skipped as a result of checksums matching
        during the last transfer."""
        return self.transfer_client.get_task(self.last_transfer_result['task_id'])['files_skipped']

    def last_task_succeeded(self):
        """ :returns: True if the last transfer task was successful, otherwise False """
        task = self.last_transfer_result['task_id']
        return 'SUCCEEDED' in self.transfer_client.get_task(task)['status']

    def move_files_on_endpoint(self, files_to_move, destination_directory,
                               uuid=None, data_type='raw'):
        """ Moves files on an endpoint into a given directory. Does it in two stages, the first is
         a TransferData task, the second is a DeleteData task.

        :param files_to_move: python list of filenames to find and move
        :param destination_directory: directory to move the files to, created if it doesn't exist
        :param uuid: UUID of the endpoint to move files on
        :param data_type: the type of data files to move (raw, dat, fit, summary, map, grid)"""
        if uuid is None:
            uuid = self.mirror_uuid
        try:
            self.transfer_client.operation_mkdir(uuid, destination_directory)
        except globus_sdk.GlobusAPIError as error:
            if error.http_status == 502:
                # This means that the directory already exists
                pass
            else:
                email_msg = f"Failed to create {destination_directory}"
                self.email_subject += email_msg
                self.send_email()
                sys.exit(email_msg)
        transfer_data = globus_sdk.TransferData(self.transfer_client, uuid, uuid,
                                                label=inspect.currentframe().f_code.co_name,
                                                sync_level="checksum", notify_on_succeeded=False,
                                                notify_on_failed=True)
        for file_to_move in files_to_move:
            transfer_data.add_item("{}/{}/{}/{}/{}".format(self.mirror_root_dir, data_type,
                                                           file_to_move[0:4], file_to_move[4:6],
                                                           file_to_move.strip('\n')),
                                   "{}/{}".format(destination_directory, file_to_move.strip('\n')))
        transfer_result = self.transfer_client.submit_transfer(transfer_data)
        self.last_transfer_result = transfer_result
        if not self.wait_for_last_task(timeout_s=60 * len(files_to_move)):
            email_msg = "Copy failed before removing from origin"
            self.email_subject += email_msg
            self.send_email()
            sys.exit(email_msg)
        # Now that all the files are copied, remove them from origin
        delete_data = globus_sdk.DeleteData(self.transfer_client, uuid,
                                            label=inspect.currentframe().f_code.co_name,
                                            notify_on_succeeded=False,
                                            notify_on_failed=True)
        for file_to_move in files_to_move:
            delete_data.add_item("{}/{}/{}/{}/{}".format(self.mirror_root_dir, data_type,
                                                         file_to_move[0:4], file_to_move[4:6],
                                                         file_to_move.strip('\n')))
        delete_result = self.transfer_client.submit_delete(delete_data)
        self.last_transfer_result = delete_result
        while not self.wait_for_last_task(timeout_s=30 * len(files_to_move)):
            print("Still waiting to delete files from origin.")
            continue


def main():
    start_time = datetime.now().strftime("%s")
    email_flag = 0

    parser = argparse.ArgumentParser(description='Given a local holding directory and a mirror directory this program'
                                                 'will perform checks on all local rawacf files, transfer all files to'
                                                 'the mirror (or other designated location -- e.g., failed,'
                                                 'blocklisted, nomatch), and update the hash files accordingly.')
    parser.add_argument('-d', '--holding', type=str, default='', help='Path to local holding directory.')
    parser.add_argument('-m', '--mirror', type=str, default='', help='Path to root directory on mirror.')
    parser.add_argument('-p', '--pattern', type=str, default="*rawacf.bz2",
                        help='Sync pattern of rawacf files, default is rawacf.bz2')
    args = parser.parse_args()

    ###################################################################################################################
    # Step 1)
    # Check for refresh token and relevant consents

    # If we have refresh token, try initializing gatekeeper object with it for auto authentication
    if isfile(TRANSFER_RT_FILENAME):
        with open(TRANSFER_RT_FILENAME) as f:
            print("Found refresh token for automatic authentication")
            gk = Gatekeeper(gatekeeper_app_CLIENT_ID, transfer_rt=f.readline())
    # Otherwise, manually authenticate and get a refresh token for future auto authentication
    else:
        print("Need to get transfer refresh token manually for future automatic authentication")
        gk = Gatekeeper(gatekeeper_app_CLIENT_ID)
        # Now check for all possible consents required on the globus endpoint and personal endpoint
        gk.check_for_consent_required()
        gk.check_for_consent_required(PERSONAL_UUID, gk.get_holding_dir())
        if gk.consents:
            print("One of the endpoints being used requires extra consent in order to be used, "
                  "and you must login a second time (dumb, I know) to get those consents.")
        gk.get_auth_with_login(gk.consents)

    ###################################################################################################################
    # Step 2)
    # Setup logger and check script arguments as well as existence of various directories

    # Setup logger
    LOGDIR = "/home/dataman/logs/globus"  # Add _test for testing purposes
    logfile = f"{LOGDIR}/{gk.cur_year:04d}/{gk.cur_month:02d}/{gk.cur_year:04d}{gk.cur_month:02d}{gk.cur_day:02d}.{gk.cur_hour:02d}{gk.cur_minute:02d}_globus_gatekeeper.log"

    # Make sure year and month directories for logfile exist
    if not isdir(f"{LOGDIR}/{gk.cur_year:04d}/"):
        mkdir(f"{LOGDIR}/{gk.cur_year:04d}/")
    if not isdir(f"{LOGDIR}/{gk.cur_year:04d}/{gk.cur_month:02d}/"):
        mkdir(f"{LOGDIR}/{gk.cur_year:04d}/{gk.cur_month:02d}/")

    function = "Gatekeeper"
    logger = extendable_logger(logfile, function, level=logging.INFO)

    # Clear out working directory /home/dataman/tmp/* before use
    if isdir(gk.get_working_dir()):
        shutil.rmtree(gk.get_working_dir())
        mkdir(gk.get_working_dir())
        logger.info(f"Clearing out working directory: {gk.get_working_dir()}")
    if not isdir(gk.get_working_dir()):
        msg = f"Directory {gk.get_working_dir()} DNE"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    logger.info(f"Args: {args.holding}  {args.mirror}  {args.pattern}")

    # Set holding directory, mirror directory, and sync pattern from parsed arguments
    gk.set_holding_dir(args.holding)
    gk.set_mirror_root_dir(args.mirror)
    gk.set_sync_pattern(args.pattern)

    logger.info("Checking for holding and mirror directories...\n")

    if not isdir(gk.get_holding_dir()):
        msg = f"Holding dir {gk.get_holding_dir()} DNE"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    if not gk.check_for_file_existence(gk.get_mirror_root_dir()):
        msg = f"Mirror root dir {gk.get_mirror_root_dir()} DNE"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    ###################################################################################################################
    # Step 3)
    # Make a list of files_to_upload consisting of all rawacf files in the holding directory
    # Get some files from mirror: master hashes, failed files list, blocklist directory

    # Get list of files to upload from the holding directory
    # Create files to upload dictionary where keys are filenames and values are empty dictionaries
    # Values will be set in Step 5) after the holding directory is hashed
    files_to_upload = gk.list_of_files_to_upload()
    files_to_upload.sort()
    files_to_upload_dict = {file: {} for file in files_to_upload}
    if len(files_to_upload) == 0:
        msg = "No files to upload. Exiting."
        logger.error(msg)
        sys.exit(msg)
    else:
        logger.info(f"Initial set of files to upload ({len(files_to_upload)}): {files_to_upload}\n")

    # Get master hashes file
    logger.info("Getting master hashes file...")
    gk.get_master_hashes()
    if not gk.wait_for_last_task():
        msg = "get_master_hashes timeout"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    # Get failed files list
    logger.info("Getting failed files list (all_failed.txt)...")
    gk.get_failed()
    if not gk.wait_for_last_task():
        msg = "get_failed timeout"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    # Recursively get blocklist folder and generate list of blocked files
    logger.info("Getting blocklist directory...\n")
    gk.get_blocklist(dest_path=f"{gk.get_working_dir()}/blocklist/")
    if not gk.wait_for_last_task(timeout_s=120):
        msg = "get_blocklist timeout"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    ###################################################################################################################
    # Step 4)
    # Make a list of blocked data files from the blocklist/ directory obtained above
    # Remove all blocked data files from files_to_upload

    # Store all txt files from blocklist directory
    blocklist_files = []
    for f in listdir(f"{gk.get_working_dir()}/blocklist/"):
        if fnmatch.fnmatch(f, "*.txt"):
            blocklist_files.append(f)

    # Store the data filenames within the txt files
    # Append filename from beginning of line
    blocked_data = []
    for f in blocklist_files:
        with open(f"{gk.get_working_dir()}/blocklist/{f}") as blocklist_file:
            for line in blocklist_file:
                blocked_data.append(line.strip('\n').strip('\r'))

    # Remove from files_to_upload if file appears in the blocklist and inform user
    blocked_files_to_remove = []
    for data_file in sorted(list(files_to_upload_dict.keys())):
        for blocked_file in blocked_data:
            if data_file in blocked_file:
                blocked_files_to_remove.append(data_file)
    blocked_files_to_remove = sorted(list(set(blocked_files_to_remove)))

    # If any blocked files were in files to upload, make blocked dir in holding dir
    # /holding_dir/blocked/cur_date/
    # Move blocked files from /holding_dir/ to /holding_dir/blocked/cur_date/
    if len(blocked_files_to_remove) > 0:
        logger.info(f"Found blocked files: {blocked_files_to_remove}")
        for file_to_remove in blocked_files_to_remove:
            files_to_upload_dict.pop(file_to_remove)

        blocked_directory = f"{gk.get_holding_dir()}/blocked"
        if not isdir(blocked_directory):
            mkdir(blocked_directory)
        blocked_subdirectory = f"{blocked_directory}/{gk.cur_date}"
        if not isdir(blocked_subdirectory):
            mkdir(blocked_subdirectory)
        logger.info(f"Moving blocked files to {blocked_subdirectory}")
        for blocked_file in blocked_files_to_remove:
            logger.info(f"Moving {gk.get_holding_dir()}/{blocked_file} to {blocked_subdirectory}/{blocked_file}\n")
            rename(f"{gk.get_holding_dir()}/{blocked_file}",
                   f"{blocked_subdirectory}/{blocked_file}")
        gk.email_subject += "Blocked files "
        gk.email_message += f"Blocked files:\r\n{blocked_files_to_remove}\r\n\r\n"
        email_flag = 1

    ###################################################################################################################
    # Step 5)
    # Hash holding directory and fill files_to_upload dictionary with relevant metadata

    # Do a sha1sum on all files in holding directory,
    sha1sum_process = subprocess.Popen(f"cd {gk.get_holding_dir()}; sha1sum {gk.get_sync_pattern()}",
                                       shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = sha1sum_process.communicate()
    sha1sum_output = out.decode().split("\n")
    sha1sum_error = err.decode().split("\n")
    # Remove empty items from the sha1sum output
    sha1sum_output = [x for x in sha1sum_output if x]
    if sha1sum_process.returncode != 0 or len(sha1sum_output) == 0:
        msg = "Error hashing files, probably no files passed"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    # Fill files_to_upload_dict with relevant metadata
    for item in sha1sum_output:
        filename = item.split()[1]
        data_hash = item.split()[0]
        year, month, day, _, _, _, _, data_type = parse_data_filename(filename)
        metadata = {'year': elements[0], 'month': elements[1], 'day': elements[2], 'yearmonth': filename[0:6],
                    'hash': data_hash, 'type': elements[7]}
        files_to_upload_dict[filename].update(metadata)

    ###################################################################################################################
    # Step 6)
    # Get unique list of yyyymm combos from files to upload dictionary
    # Create yearmonth dictionary to organize files_to_upload by their yearmonth
    # Loop through yyyymm combos and get the yyyymm.hashes file from the mirror on each iteration
    # Perform sha1sum comparison between rawacfs in holding dir and recently acquired hashfile in working dir
    # Handle each file individually depending on the result of the sha1sum comparison

    # Get unique list of yyyymm combos and create dictionary
    # Keys are yyyymm and values are the files to upload dictionary items corresponding to the given yyyymm
    yearmonth = sorted(list(set([filename[0:6] for filename in files_to_upload_dict.keys()])))
    yearmonth.sort()
    yearmonth_dict = {ym: {} for ym in yearmonth}
    for ym in yearmonth:
        d = {k: v for k, v in files_to_upload_dict.items() if k[0:6] == ym}
        yearmonth_dict[ym].update(d)

    # Get appropriate hashes files for yyyymm for all files in list
    logger.info(f"Set of years and months for data files in holding directory: {str(yearmonth)}")
    new_hash_file = False
    non_matching_files = []
    for ym in yearmonth:
        hash_path = gk.get_hash_file_path(int(ym[0:4]), int(ym[4:6]))
        logger.info(f"Checking if {hash_path} exists on mirror...")
        if gk.check_for_file_existence(hash_path):
            # Get yyyymm.hashes from mirror to working dir
            gk.get_hashes(int(ym[0:4]), int(ym[4:6]), dest_path=gk.get_working_dir())
            if not gk.wait_for_last_task():
                logger.warning(f"Get hashes for {ym} didn't complete. Removing files from files_to_upload")
                # Remove all files w/ given yyyymm from files_to_upload if get hashes timed out
                for item in list(yearmonth_dict[ym].keys()):
                    files_to_upload_dict.pop(item)
                yearmonth_dict.pop(ym)
            else:
                logger.info(f"{ym} hash file retrieved from mirror.")
                # sha1sum files in holding_dir and compare to yyyymm.hashes now in working dir (-c == compare)
                command_string = f"cd {gk.get_holding_dir()}; sha1sum -c {gk.get_working_dir()}/{ym}.hashes"
                sha1sum_process = subprocess.Popen(command_string, shell=True,
                                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = sha1sum_process.communicate()
                sha1sum_decoded_output = out.decode().split("\n")
                sha1sum_decoded_error = err.decode().split("\n")
                # Loop through result of sha1sum comparison for each file
                # Only remove from files_to_upload if file exists both in holding_dir and hashfile in working_dir
                # Need further investigation into "Failed open or read" and "" results
                for sha1sum_result in sha1sum_decoded_output:
                    hashed_file = sha1sum_result.split(":")[0]
                    if sha1sum_result.find("FAILED open or read") != -1:
                        pass
                    # If hashes do not match, add file to nonmatching files list, remove from files_to_upload
                    elif sha1sum_result.find("FAILED") != -1:
                        logger.warning(f"{hashed_file} hash doesn't match. Adding to no match list, and removing from list of files to upload.")
                        non_matching_files.append(hashed_file)
                        files_to_upload_dict.pop(hashed_file)
                    # If hashes match, remove from files_to_upload as it is already on mirror
                    elif sha1sum_result.find("OK") != -1:
                        logger.info(f"{hashed_file} already exists on mirror and hash matches. Removing from files to upload.")
                        files_to_upload_dict.pop(hashed_file)
                        # Comment out removal of matching files from holding dir for testing purposes
                        try:
                            remove(f"{gk.get_holding_dir()}/{hashed_file}")
                        except OSError as error:
                            logger.error(f"Error trying to remove file: {error}.")
                    elif sha1sum_result is "":
                        pass
                    else:
                        logger.warning(f"Error, I don't know how to deal with: {sha1sum_result}.")
        # If yyyymm.hashes DNE, create it ONLY IF yyyymm is the current year and month
        else:
            # Need to check if this is the current month, otherwise error out
            # No need to do the above checks for this yearmonth as there is clearly no data for it yet
            if gk.cur_month == int(ym[4:6]) and gk.cur_year == int(ym[0:4]):
                logger.info(f"Hash file for {ym} doesn't exist, creating new directory.")
                gk.create_new_data_dir(ym[0:4], ym[4:6])
                new_hash_file = True
            else:
                # Error, previous month's hash files should exist already
                msg = f"Hash file {ym} not found"
                gk.email_subject += msg
                gk.send_email()
                logger.error(msg)
                sys.exit(msg)

    logger.info(f"No match list: {non_matching_files}\n")

    ###################################################################################################################
    # Step 7)
    # Bzip check all files in list, and do other checks like file size check
    # Create a dictionary of failed_files, the keys are the filenames (string) and the values are
    # the hash and the reason for failure (strings) in a tuple, which is immutable and fixed in size

    failed_files = {}
    # Loop through files_to_upload_dict as it contains only rawacfs still eligible for transfer
    files_to_upload = sorted(list(files_to_upload_dict.keys()))
    for filename in files_to_upload:
        data_file = filename
        data_file_hash = files_to_upload_dict[filename]['hash']
        logger.info(f"bunzip -t {data_file}")
        # Perform bzip test on data file (-t == test)
        bunzip2_process = subprocess.Popen(f"cd {gk.get_holding_dir()}; bunzip2 -t {data_file}",
                                           shell=True, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
        out, err = bunzip2_process.communicate()
        bunzip2_process_output = out.decode().split("\n")
        bunzip2_process_error = err.decode().split("\n")

        filesize = getsize(f"{gk.get_holding_dir()}{data_file}")
        # Check error code of bzip test and log a relevant error message
        # Remove from files_to_upload if any nonzero error code
        if bunzip2_process.returncode == 1 or bunzip2_process.returncode == 3:
            logger.warning(f"OUTPUT: {bunzip2_process_output}")
            logger.warning(f"ERROR: {str(err)}")
            # File probably not there. Error so let us know
            logger.warning(f"Error. File {data_file} not found by bunzip2 test. Removing from list.")
            files_to_upload_dict.pop(data_file)
        elif bunzip2_process.returncode == 2:
            # Error with bz2 integrity of file.
            logger.warning(f"Error. File {data_file} failed the bzip2 test! Removing from list.")
            files_to_upload_dict.pop(data_file)
            failed_files[data_file] = (data_file_hash, "Failed BZ2 integrity test")
        # Check if data file is empty (header of rawacf is 14 bytes)
        elif filesize == 14 or filesize == 0:
            logger.warning(f"File {data_file} empty. Removing from list.")
            files_to_upload_dict.pop(data_file)
            failed_files[data_file] = (data_file_hash, "File contains no records (empty)")
        # Check if data file is smaller than the header (14 bytes)
        elif filesize < 14:
            logger.warning(f"File {data_file} too small. Removing from list.")
            files_to_upload_dict.pop(data_file)
            failed_files[data_file] = (data_file_hash, "File contains no records (empty)")
        # If file passed bzip test and is not empty, run bzcat to unzip the file
        else:
            # Try using backscatter package to test dmap integrity
            unzipped_filename = data_file.split(".bz2")[0]
            logger.info(f"bzcat {data_file} > {unzipped_filename}")
            bzcat_process = subprocess.Popen(f"cd {gk.get_holding_dir()}; bzcat {data_file} > {unzipped_filename}",
                                             shell=True, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
            out, err = bzcat_process.communicate()
            bzcat_process_output = out.decode().split("\n")
            bzcat_process_error = err.decode().split("\n")
            # Check error code of bzcat and log a relevant error message
            # Remove from files_to_upload if any nonzero error code
            if bzcat_process.returncode == 1 or bzcat_process.returncode == 3:
                logger.warning(f"OUTPUT: {bzcat_process_output}")
                logger.warning(f"ERROR: {bzcat_process_error}")
                # File probably not there. Error so let us know
                logger.warning(f"Error. File {data_file} not found by bzcat. Removing from list.")
                files_to_upload_dict.pop(data_file)
            elif bunzip2_process.returncode == 2:
                # Error with bz2 integrity of file.
                logger.warning(f"Error. File {data_file} failed with bzcat! Removing from list.")
                files_to_upload_dict.pop(data_file)
                failed_files[data_file] = (data_file_hash, "Failed BZ2 integrity test")
            # If bzcat succeeded on file, test dmap integrity and read using pyDARNio
            # If failed, log error message, remove from files_to_upload, add to failed_files
            else:
                try:
                    dmap_stream = open(f"{gk.get_holding_dir()}/{unzipped_filename}", 'rb').read()
                    reader = pydarnio.SDarnRead(dmap_stream, True)
                    records = reader.read_rawacf()
                except Exception as error:
                    errstr = "Error. File {0} failed with error {1}".format(data_file,
                                                                            str(error).replace("\n",
                                                                                               ""))
                    logger.warning(' '.join(errstr.split()))
                    files_to_upload_dict.pop(data_file)
                    errstr = ' '.join(str(error).replace("\n", "").split())
                    failed_files[data_file] = (data_file_hash, errstr)
                # At this point, remaining files passed bzip, bzcat, and dmap integrity test
                # Remaining files are also not empty
                else:
                    logger.info(f"{data_file} passed pydarnio dmap tests.")
                # Remove unzipped rawacf from holding_dir (created by bzcat test)
                finally:
                    remove(f"{gk.get_holding_dir()}/{unzipped_filename}")

    logger.info("Failed files list: ")
    for failed in failed_files:
        logger.info(f"{failed_files[failed][0]}  {failed} | {failed_files[failed][1]}")

    ###################################################################################################################
    # Step 8)
    # Append failed files to all_failed.txt in working dir and upload to mirror
    # Move failed files to holding_dir/failed/ and move nonmatching files to holding_dir/nomatch/
    # Transfer failed files to failed directory on mirror

    # Update all_failed.txt with new failed files and upload to mirror
    logger.info("Updating all_failed.txt\n")
    try:
        result = gk.update_failed(failed_files)
        if result is None:
            logger.warning("Error with updating failed files list on mirror")
            gk.email_subject += "error updating all_failed.txt"
            gk.email_message += "Error with updating failed files list on mirror, please check it manually\r\n"
            email_flag = 1
        while not gk.wait_for_last_task(timeout_s=300):
            logger.info("Still waiting for failed files list to upload and complete...")
    except Exception as e:
        logger.warning(f"Error: {e}. Please update manually")

    # If any non-matching files were found, make nomatch dir in holding_dir
    # /holding_dir/nomatch/cur_date/
    # Move non-matching files to /holding_dir/nomatch/cur_date/
    if len(non_matching_files) > 0:
        nomatch_directory = f"{gk.get_holding_dir()}/nomatch"
        if not isdir(nomatch_directory):
            mkdir(nomatch_directory)
        nomatch_subdirectory = f"{nomatch_directory}/{gk.cur_date}"
        if not isdir(nomatch_subdirectory):
            mkdir(nomatch_subdirectory)
        logger.info(f"Moving non-matching files to {nomatch_subdirectory}\n")
        for non_matched_file in non_matching_files:
            logger.info(f"Moving {gk.get_holding_dir()}/{non_matched_file} to {nomatch_subdirectory}/{non_matched_file}\n")
            rename(f"{gk.get_holding_dir()}/{non_matched_file}",
                   f"{nomatch_subdirectory}/{non_matched_file}")
        gk.email_subject += "Non matching files "
        gk.email_message += f"Non matching files:\r\n{non_matching_files}\r\n\r\n"
        email_flag = 1

    # Upload failed files to failed dir on mirror with a timeout of 60s plus an extra 10s
    # for each additional file
    if len(failed_files) > 0:
        # Now upload the files to the mirror
        upload_timeout = 60 + 10 * len(failed_files)
        logger.info(f"Uploading failed files to mirror failed dir with {upload_timeout} s timeout")

        if not gk.sync_failed_files_from_list(list(failed_files)):
            msg = "Failed to sync failed files, sync manually."
            gk.email_message += msg
            gk.email_subject += "sync_failed_files_from_list failed"
            gk.send_email()
        gk.wait_for_last_task(timeout_s=upload_timeout)
        while not gk.wait_for_last_task():
            logger.info("Still waiting for failed files to upload and complete...")
        if not gk.last_task_succeeded():
            msg = "Don't know which failed files were transferred successfully and which were not!"
            gk.email_message += msg
            gk.email_subject += "sync_files_from_list failed to sync failed files, sync manually."
            gk.send_email()

        # Make failed dir in holding_dir, /holding_dir/failed/cur_date
        # Move failed files to /holding_dir/failed/cur_date/
        failed_directory = f"{gk.get_holding_dir()}/failed"
        if not isdir(failed_directory):
            mkdir(failed_directory)
        failed_subdirectory = f"{failed_directory}/{gk.cur_date}"
        if not isdir(failed_subdirectory):
            mkdir(failed_subdirectory)
        logger.info(f"Moving failed files to {failed_subdirectory}\n")
        for failed_file in failed_files:
            logger.info(f"Moving {gk.get_holding_dir()}/{failed_file} to {failed_subdirectory}/{failed_file}\n")
            rename(f"{gk.get_holding_dir()}/{failed_file}",
                   f"{failed_subdirectory}/{failed_file}")
        gk.email_subject += "Failed files "
        gk.email_message += f"Failed files:\r\n{failed_files}\r\n\r\n"
        email_flag = 1

    ###################################################################################################################
    # Step 9)
    # Upload files_to_upload to mirror

    # Get updated list of files_to_upload from dictionary
    files_to_upload = sorted(list(files_to_upload_dict.keys()))
    logger.info(f"Final set of files to upload: {files_to_upload}")

    # Exit if there are no files to upload
    if len(files_to_upload) == 0:
        msg = "No files to upload"
        gk.email_subject += msg
        gk.send_email()
        logger.info(msg)
        sys.exit(msg)

    # Similar to failed files, timeout is 60 seconds plus an additional 10 seconds for each file
    upload_timeout = 60 + 10 * len(files_to_upload)
    logger.info(f"Uploading files to mirror with {upload_timeout} s timeout...\n")

    # Now sync the files up to the mirror in the appropriate place
    gk.sync_files_from_list(files_to_upload)
    gk.wait_for_last_task(timeout_s=upload_timeout)
    while not gk.wait_for_last_task():
        logger.info("Still waiting for last task to complete...")
    if not gk.last_task_succeeded():
        msg = "Don't know which files were transferred successfully and which were not!"
        gk.email_message += msg
        gk.email_subject += "sync_files_from_list failed"
        gk.send_email()
        logger.warning(msg)
        sys.exit(msg)

    ###################################################################################################################
    # Step 10)
    # Get a list of files that succeeded the transfer and a list of files that were skipped
    # Create a dictionary and store the string to append to yyyymm.hashes for each yyyymm

    # Check which files succeeded in the transfer. If a file was skipped it won't appear in this
    succeeded = gk.get_task_successful_transfers()
    # Setup filenames as keys for succeeded and skipped dictionaries
    succeeded_files = [str(info['destination_path'].split('/')[-1]) for info in succeeded]
    skipped_files = [filename for filename in files_to_upload_dict if filename not in succeeded_files]

    logger.info(f"Skipped files list: {skipped_files}")
    logger.info(f"Skipped files: {gk.get_num_files_skipped()}")
    logger.info(f"Transferred files: {len(succeeded_files)}")
    logger.info(f"Total files: {gk.get_num_files_skipped() + len(succeeded_files)}")
    logger.info(f"Files to upload: {len(files_to_upload)}\n")

    # Make a dictionary to store the '<hash1> <file1> \n <hash2> <file2>' string for each yyyymm.hashes file
    # Use list of succeeded files to get yyyymm bc only succeeded files should be added to hash file
    yearmonth = list(set([filename[0:6] for filename in succeeded_files]))
    yearmonth.sort()
    yearmonth_hash_dict = {ym: "" for ym in yearmonth}

    # Get set of data types for current run (raw, dat, etc.)
    used_data_types = []
    for file in sorted(list(succeeded_files)):
        used_data_types.append(files_to_upload_dict[file]['type'])
    used_data_types = set(used_data_types)

    # Make lists of data files on the mirror for all datatypes and all yearmonths
    mirror_list = []
    for data_type in used_data_types:
        for ym in yearmonth:
            mirror_list.append(gk.get_file_list(ym[0:4], ym[4:6]), data_type)

    # All the metadata of interest below is stored in files_to_upload_dict
    # Remove each succeeded file from the holding dir and append "<hash> <filename> \n" to dictionary for yyyymm
    for filename in sorted(list(succeeded_files)):
        ym = files_to_upload_dict[filename]['yearmonth']
        # Make sure "succeeded" file actually made it to the mirror
        if filename in mirror_list:
            data_hash = files_to_upload_dict[filename]['hash']
            remove(f"{gk.get_holding_dir()}/{filename}")  # Comment this line for testing purposes
            yearmonth_hash_dict[ym] += f"{data_hash}  {filename}\n"
        else:
            logger.warning(f"Transfer of {filename} listed as succeeded but not found on mirror!")

    ###################################################################################################################
    # Step 11)
    # Update the yyyymm.hashes files with their corresponding succeeded files and upload to mirror
    # Finally, update the master hashes on the mirror

    yearmonth = sorted(list(yearmonth_hash_dict.keys()))
    logger.info(f"Updating hash files: {yearmonth}")
    # Update yyyymm.hashes from dictionary and upload to mirror
    # for ym, hash_string in yearmonth_hash_dict.items():
    for ym in yearmonth:
        hash_string = yearmonth_hash_dict[ym]
        hashfile_path = f"{gk.get_working_dir()}/{ym}.hashes"
        # If string is not empty, append it to hashfile
        if hash_string is not "":
            hash_string.strip("\n")
            with open(hashfile_path, 'a') as f:
                f.write(f"{hash_string}")
            # Upload hashfile to mirror
            gk.put_hashes(int(ym[0:4]), int(ym[4:6]),
                          source_path=gk.get_working_dir())
            while not gk.wait_for_last_task():
                logger.info("Still waiting for hashes task to finish... ")
                continue
        else:
            msg = f"{hashfile_path} update string is empty..."
            logger.info(msg)
            email_flag = 1
            gk.email_message += msg

    # New method to update master hashes:
    # 1) get master hashes
    # 2) read master hashes
    # 3) if updated ym in master hash, replace hash
    # 4) if new ym, add to master hash
    # 5) put master hashes
    # Update master.hashes with all successfully uploaded files
    # Get master hashes file
    logger.info("Getting master hashes file...")
    gk.get_master_hashes()
    if not gk.wait_for_last_task():
        msg = "get_master_hashes timeout"
        gk.email_subject += msg
        gk.send_email()
        logger.error(msg)
        sys.exit(msg)

    # Read current master hashes file in as dictionary with filenames as keys and hashes as values
    # "Filenames" are of the form ./raw/yyyymm.hashes and ./dat/yyyymm.hashes
    hashes = {}
    with open(f"{gk.get_working_dir()}/master.hashes", 'r') as master_file:
        for line in master_file:
            (val, key) = line.split()
            hashes[key] = val

    # For each yyyymm in holding dir which passed all tests
    #    - hash the corresponding yyyymm.hashes
    #    - update/append the key, value pair to the hashes dictionary
    for ym in yearmonth:
        raw_hash_dir = f"{gk.get_working_dir()}/raw"
        if not isdir(raw_hash_dir):
            mkdir(raw_hash_dir)
        # Move hash file to working_dir/raw/ to ensure entry in master hash of the form ./raw/yyyymm.hashes
        logger.info(f"Moving {ym}.hashes to {raw_hash_dir}\n")
        rename(f"{gk.get_working_dir()}/{ym}.hashes",
               f"{raw_hash_dir}/{ym}.hashes")
        # Hash yyyymm.hashes file in working_dir/raw/ from working_dir
        hash_process = subprocess.Popen(f"cd {gk.get_working_dir()}; sha1sum ./raw/{ym}.hashes",
                                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        hash_process_out, hash_process_err = hash_process.communicate()
        hash_process_output = hash_process_out.decode().split("\n")

        # Add yyyymm.hashes to dictionary if it doesn't exist, update existing hash o/w.
        hashes[f"./raw/{ym}.hashes"] = hash_process_output[0].split()[0]

    # Overwrite entire master.hashes file with dictionary
    with open(f"{gk.get_working_dir()}/master.hashes", 'w') as master_file:
        for key in sorted(list(hashes.keys())):
            master_file.write(f"{hashes[key]}  {key}\n")

    # Upload master hash to mirror
    logger.info("Updating master hashes")
    try:
        gk.put_master_hashes()
        # gk.update_master_hashes()
        if not gk.wait_for_last_task():
            msg = "Updating of master hashes didn't complete."
            logger.warning(msg)
            email_flag = 1
            gk.email_message += msg
    except globus_sdk.GlobusError as error:
        logger.error(error)
        msg = "Updating of master hashes didn't complete."
        logger.error(msg)
        email_flag = 1
        gk.email_message += msg
        gk.email_message += error
    except Exception as error:
        logger.error(error)
        msg = "Updating master hashes failed."
        logger.error(msg)
        email_flag = 1
        gk.email_message += msg
        gk.email_message += str(error)

    if email_flag:
        gk.send_email()

    finish_time_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    finish_time = datetime.now().strftime("%s")

    logger.info(f"Finished at: {finish_time_utc}")
    total_time = (int(finish_time) - int(start_time))/60
    logger.info(f"Script finished. Total time: {total_time} minutes")


    if __name__ == "__main__":
        main()


# TODO: check if file exists in the hashes file but not on the mirror
# TODO: go through all "print" statements and either remove them or add them to logger
