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

HOME = expanduser("~")
TRANSFER_RT_FILENAME = f"{HOME}/.globus_transfer_rt"
PERSONAL_UUID_FILENAME = f"{HOME}/.globusonline/lta/client-id.txt"

if isfile(PERSONAL_UUID_FILENAME):
    with open(PERSONAL_UUID_FILENAME) as f:
        PERSONAL_UUID = f.readline().strip()


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
    def __init__(self, client_id, client_secret=None, transfer_rt=None, working_dir=f"{HOME}/tmp_test/"):
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
        self.mirror_failed_dir = '/project/6008057/sdarn/local_data/failed_test/'  # TODO: This is hacky, should be handled better, using the input args or something

        self.cur_year = datetime.now().year
        self.cur_month = datetime.now().month
        self.cur_day = datetime.now().day
        self.cur_hour = datetime.now().hour
        self.cur_minute = datetime.now().minute
        self.cur_date = datetime.now().strftime("%Y%m%d")
        self.possible_data_types = ['raw', 'dat']

        # Setup logger
        logdir = "/home/dataman/logs_test/globus"  # Add _test for testing purposes
        logfile = (f"{logdir}/{self.cur_year:04d}/{self.cur_month:02d}/{self.cur_year:04d}{self.cur_month:02d}"
                   f"{self.cur_day:02d}.{self.cur_hour:02d}{self.cur_minute:02d}_globus_gatekeeper.log")
        # Make sure year and month directories for logfile exist
        if not isdir(f"{logdir}/{self.cur_year:04d}/"):
            mkdir(f"{logdir}/{self.cur_year:04d}/")
        if not isdir(f"{logdir}/{self.cur_year:04d}/{self.cur_month:02d}/"):
            mkdir(f"{logdir}/{self.cur_year:04d}/{self.cur_month:02d}/")

        self.logger = extendable_logger(logfile, "Gatekeeper")

        # Potential consents required (new as of Globus endpoints v5 - see here: https://globus-sdk-python.readthedocs.io/en/stable/examples/minimal_transfer_script/index.html#example-minimal-transfer)
        self.consents = []

        # Get a transfer client
        # Note that this uuid is the new cedar globus version 5 uuid, and hardcoded here due to hacking
        # this shit together in a quick timeframe. Ideally this would be searched and found programmatically via the function below "get_superdarn_mirror_uuid, which works to get the correct uuid, but we need a transfer client to use it, but we need the uuid to get a transfer client... so yeah, chicken and egg"
        self.mirror_uuid = '8dec4129-9ab4-451d-a45f-5b4b8471f7a3'
        # self.mirror_uuid = '88cd829c-75fa-44e6-84bb-42e6250afaea'
        # self.mirror_uuid = "bc9d5b7a-6592-4156-bfb8-aeb0fc4fb07e"
        self.transfer_client = self.get_transfer_client()

        # Email information ##########################################################
        # smtpServer is the host to use that will actually send the email
        # emailFlag is set to 1 if a condition arises that requires an email alert
        # emailMessage is initialized to nothing here, and filled in with an
        #       appropriate message depending upon the reason for the email.
        self.email_recipients = ['saif.marei@usask.ca']
        self.email_from = 'dataman'
        self.current_time = datetime.now()
        self.email_subject = '[Gatekeeper Globus] ' + self.current_time.strftime("%Y%m%d.%H%M : ")
        self.smtp_server = 'localhost'
        self.email_message = ''

        # Check if there is a current transfer, that could be bad
        if self.check_for_transfer_to_endpoint(self.mirror_uuid):
            sub = "Error: current active transfer to mirror"
            self.log_email_exit(self.logger.error, 1, 1, sub=sub)

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

    def log_email_exit(self, loglevel, email_flag, exit_flag, msg='', sub=''):
        """ Will log a message with a given log level and optionally email the message and exit the script.

        :param loglevel: The log level method of the specified logger (logger.info, logger.warning, logger.error)
        :param email_flag: Email flag (1 -> email, 0 -> no email)
        :param exit_flag: Exit flag (1 -> exit, 0 -> no exit)
        :param msg: Message to be logged/sent as the email body
        :param sub: Subject line of email. If no message was provided, log the subject instead.
        """
        self.email_message += msg
        self.email_subject += sub
        if msg == '':
            loglevel(sub)
        else:
            loglevel(msg)
        if email_flag:
            self.send_email()
        if exit_flag:
            sys.exit()

        # Reset subject and message for future emails
        self.email_subject = '[Gatekeeper Globus] ' + self.current_time.strftime("%Y%m%d.%H%M : ")
        self.email_message = ''

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
                self.logger.info(f"Task label: {task['label']}")
                self.logger.info(f"Task dest EP name: {task['destination_endpoint_display_name']}")
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
        print(
            f"Here is your refresh token: {globus_transfer_data['refresh_token']}. It has been written to the file {self.transfer_rt_filename}")
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
            self.logger.info(
                f"{entry['permissions']} {entry['user']}:{entry['group']} {entry['name']} {entry['type']} {entry['last_modified']}")

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
            self.logger.info(f"[{ep['id']}] {ep['display_name']}")

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
                self.logger.warning(f"update_master_hashes() error. Failed to make directory {data_type_path}: {error}")
            self.get_hashes_all(data_type=data_type,
                                dest_path=data_type_path)
            while not self.wait_for_last_task(timeout_s=600):
                self.logger.info("Still waiting for last task...")
            hash_process = subprocess.Popen(f"cd {self.get_working_dir()}; sha1sum ./{data_type}/*hashes",
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
            self.logger.info(task["label"], task["task_id"], task["type"], task["status"])

    def print_endpoints(self):
        """Convenience function. Prints out information about the user's endpoints"""
        self.logger.info("My endpoints:")
        for ep in self.transfer_client.endpoint_search(filter_scope="my-endpoints"):
            self.logger.info(f"[{ep['id']}] {ep['display_name']}")

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
            self.logger.info(f"Getting at least one file. Transfer result: {transfer_result}")
            return transfer_result
        else:
            self.logger.info(f"No hashes files for data type: {data_type}")
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
            sub = f"Invalid data type: {data_type}"
            self.log_email_exit(self.logger.error, 1, 1, sub=sub)
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
                self.logger.error(f"Error: {failed_update_files} is not a file, cannot update failed files list")
                return None
            if stat(failed_update_files).st_size == 0:  # The file should never be empty
                self.logger.error(f"Error: {failed_update_files} is empty, cannot update failed files list")
                return None
            # Add the list of failed files to the current list
            with open(failed_update_files) as file_of_failed_updates:
                filetext = file_of_failed_updates.readlines()
            with open(failed_update_files, 'a+') as file_of_failed_updates:
                # cat the list 'add_failed_files' to the file 'file_of_failed_files', keep unique
                for filename_key in add_failed_files:
                    for line in filetext:
                        if add_failed_files[filename_key][0] in line and filename_key in line:
                            self.logger.info(f"'{line}' already in all_failed.txt file, moving on")
                            break
                    else:
                        file_of_failed_updates.write(
                            f"{add_failed_files[filename_key][0]}  {filename_key} | {add_failed_files[filename_key][1]}\n")
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
            self.logger.info("Error. No last transfer, returning.")
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
        msg = None
        while retries < maximum_retries:
            if uuid is None:
                uuid = self.mirror_uuid
            try:  # TODO: Check for error.code in case msg changes.
                self.transfer_client.operation_ls(uuid, path=file_path)
                return True  # This means it was not a file and it did exist
            except globus_sdk.TransferAPIError as error:
                if error.message.find("not found on endpoint") != -1:
                    self.logger.error(error)
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
        msg = f"Checking for file existence failed after {retries} retries. Exiting!"
        if msg is not None:
            msg = f"{msg}\n{errormsg}"
        sub = "Check for file existence failed"
        self.log_email_exit(self.logger.error, 1, 1, msg=msg, sub=sub)

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
                self.logger.info(f"Directory {path} already existed.")
            else:
                self.logger.error(f"Failed to create {path} directory.")
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
                self.logger.info(f"Directory {year_path} already existed.")
            else:
                self.logger.error(f"Failed to create {year_path} directory.")
                return False
        try:
            self.transfer_client.operation_mkdir(uuid, month_path)
        except globus_sdk.GlobusAPIError as error:
            if error.http_status == 502:
                # This means the directory already exists
                self.logger.info(f"Directory {month_path} already existed.")
            else:
                self.logger.error(f"Failed to create {month_path} directory.")
                return False
        return True

    def get_superdarn_mirror_uuid(self):
        """ Will search endpoints and retrieve the UUID of the SuperDARN mirror endpoint.

        :returns: UUID of SuperDARN mirror endpoint """
        for ep in self.transfer_client.endpoint_search('Digital Research Alliance of Canada Cedar GCSv5'):
            if 'globus@tech.alliancecan.ca' in ep['contact_email']:
                self.logger.info(f"Mirror UUID: {ep['id']}")
                return ep['id']

        sub = "Mirror endpoint not found"
        self.log_email_exit(self.logger.error, 1, 1, sub=sub)

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
                sub = f"Failed to create {destination_directory}"
                self.log_email_exit(self.logger.error, 1, 1, sub=sub)
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
            sub = "Copy failed before removing from origin"
            self.log_email_exit(self.logger.error, 1, 1, sub=sub)
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
            self.logger.info("Still waiting to delete files from origin.")
            continue

    def move_files_to_subdir(self, fail_type, files_to_move):
        """ Moves blocked, non-matching, or failed files to corresponding subdirectories within local holding directory.

        :param fail_type: The type of fail leading to file move. ("Blocked", "Nomatch", "Failed")
        :param files_to_move: List of filenames in holding dir to be moved to specific subdirectory.
        """

        directory = f"{self.get_holding_dir()}/{fail_type.lower()}"
        if not isdir(directory):
            mkdir(directory)
        subdir = f"{directory}/{self.cur_date}"
        if not isdir(subdir):
            mkdir(subdir)
        self.logger.info(f"Moving {fail_type.lower()} files to {subdir}")
        for file in files_to_move:
            self.logger.info(f"Moving {self.get_holding_dir()}/{file} to {subdir}/{file}\n")
            rename(f"{self.get_holding_dir()}/{file}",
                   f"{subdir}/{file}")
        msg = f"{fail_type} files:\r\n{files_to_move}\r\n\r\n"
        sub = f"{fail_type} files"
        self.log_email_exit(self.logger.info, 1, 0, msg=msg, sub=sub)
