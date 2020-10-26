#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

"""
Main functions to manage email account connections and retrieval
"""
# pylint: disable=inconsistent-quotes

import base64
import datetime
import imaplib
import json
import logging
import pickle
import re
import ssl
from pathlib import Path

import google
import google_auth_oauthlib
import mailparser
from googleapiclient.discovery import build

import util_lib

logger = logging.getLogger('ar3_mailrepo.ar3_mailreport_lib')


def create_server_connection(credentials: dict):
  if credentials['protocol'] == 'imap4':
    return IMAPServerConnection(credentials)
  if credentials['protocol'] == 'gmail':
    return GmailServerConnection(credentials)
  raise RuntimeError('Unkown Protocol ' + credentials['protocol'])


class ServerConnection:

  """
  Base class representing email server connection
  """

  def __init__(self):
    pass

  def create_connection(self):
    pass

  def close(self):
    pass

  def retrieve_messages_to_cache(self, cache_folder: Path,
                                 since_date: datetime.datetime,
                                 dupes_filterset: set):
    logger.debug(f"Begin Retrieve messages for {self.credentials['emaillabel']}"
                 f" into {cache_folder} since {since_date}")
    ok_count = 0
    dupe_count = 0
    error_count_folders = 0
    error_count_msg = 0
    download_start = datetime.datetime.now()
    for result in self.retrieve_messages(since_date, dupes_filterset):
      result_id = util_lib.create_unique_id()
      if 'is_error' in result:
        if result['error_scope'] == 'FOLDER':
          error_count_folders += 1
        elif result['error_scope'] == 'MESSAGE':
          error_count_msg += 1
        else:
          raise RuntimeError('Unknown Error scope ', result['error_scope'])
        error_fname = Path(
          cache_folder / f"Error_{result['error_scope']}_{result_id}.txt")
        with open(error_fname, 'w') as errorf:
          errorf.write(result['error_description'])
          errorf.write('\n')
      elif 'is_dupe' in result:
        dupe_count += 1
      else:
        ok_count += 1
        msg = result
        msg['ar3mr_email_account'] = self.credentials['emaillabel']
        msg['ar3mr_uuid'] = result_id
        msg['ar3mr_downloadtime'] = download_start
        if 'ar3mr_gmail_data' not in msg:
          msg['ar3mr_gmail_data'] = None
        msg_fnmame = Path(cache_folder / f'Msg_{result_id}.pickle')
        with open(msg_fnmame, 'wb') as msgf:
          pickle.dump(obj=msg, file=msgf, protocol=util_lib.PICKLE_PROTOCOL)
    download_duration_seconds = int(
      (datetime.datetime.now() - download_start).total_seconds())
    download_report = {
      'ok_count': ok_count,
      'dupe_filter_size': len(dupes_filterset),
      'since_date': since_date.isoformat(),
      'dupe_count': dupe_count,
      'error_count_folders': error_count_folders,
      'error_count_msg': error_count_msg,
      'download_start': download_start.isoformat(),
      'download_duration_seconds': download_duration_seconds,
    }
    dnreport_nmame = Path(cache_folder / 'download_report.json')
    with open(dnreport_nmame, 'w') as dnrpf:
      json.dump(obj=download_report, fp=dnrpf, indent=4)
    logger.debug(f"Finish Retrieve messages for {self.credentials['emaillabel']}"
                 f" into {cache_folder} since {since_date}")

  def retrieve_folders(self):
    pass

  def retrieve_messages(self, since_date: datetime.datetime, dupes_filterset: set):
    pass


class GmailServerConnection(ServerConnection):

  """
  GMAIL Specific email server connection class
  """

  SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

  @staticmethod
  def _build_gmail_service(generic_credentials: dict):
    token_cachefile = generic_credentials['gmail_oauth_token_cache']
    credentials = generic_credentials['gmail_oauth_credentials']
    creds = None
    if token_cachefile.exists():
      with open(token_cachefile, 'rb') as token:
        creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
        creds.refresh(google.auth.transport.requests.Request())
      else:
        flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
          credentials, GmailServerConnection.SCOPES)
        creds = flow.run_local_server(port=0)
      # Save the credentials for the next run
      with open(token_cachefile, 'wb') as token:
        pickle.dump(creds, token, protocol=util_lib.PICKLE_PROTOCOL)
    return build('gmail', 'v1', credentials=creds)

  def __init__(self, credentials: dict):
    super(GmailServerConnection, self).__init__()
    self.credentials = credentials
    self.conn = GmailServerConnection._build_gmail_service(credentials)

  def close(self):
    logger.debug('Called CLOSE on Gmail (noop)')

  def retrieve_folders(self):
    results = self.conn.users().labels().list(userId='me').execute()
    return results.get('labels', [])

  def retrieve_messages(self, since_date: datetime.datetime, dupes_filterset: set):
    expected_fileds = {
      'id',
      'threadId',
      'labelIds',
      'snippet',
      'sizeEstimate',
      'raw',
      'historyId',
      'internalDate'
    }
    download_chunk_sz = 1000
    q_param = 'after:' + since_date.strftime('%Y-%m-%d')
    results = self.conn.users().messages().list(userId='me', q=q_param,
                                                maxResults=download_chunk_sz).execute()
    resultlist = results['messages']
    while 'nextPageToken' in results:
      results = self.conn.users().messages().list(userId='me',
                                                  pageToken=results['nextPageToken'],
                                                  q=q_param,
                                                  maxResults=download_chunk_sz).execute()
      if results['resultSizeEstimate'] > 0:
        resultlist.extend(results['messages'])
    message_ids = {x['id'] for x in resultlist}
    error_limt = 1000
    error_count = 0
    for msg_ix, message_id in enumerate(message_ids):
      try:
        message = self.conn.users().messages().get(userId='me', id=message_id,
                                                   format='raw').execute()
        logger.debug(f'Download message {msg_ix + 1}/{len(message_ids)}: {message_id}')
        if len(set(message.keys()) - expected_fileds) > 0:
          logger.error(f'Unexpected contents in gmail downloaded data: \
          {set(message.keys())} compared to {expected_fileds}')
          raise RuntimeError(f'Unexpected contents in gmail downloaded data: \
          {set(message.keys())} compared to {expected_fileds}')
        if id in dupes_filterset:
          yield {
            'is_dupe': 'True',
          }
        else:
          yield self.standardise_message(message)
      except Exception as e: # pylint: disable=broad-except
        error_count += 1
        if error_count > error_limt:
          raise RuntimeError(
            f'Exceeding number of messages errors {error_count} in gmail download')
        logger.debug(f'Yielding error object {str(e)}')
        yield {
          'is_error': 'True',
          'error_description': str(e),
          'error_scope': 'MESSAGE'
        }

  def standardise_message(self, downloaded_msgitem):
    m = mailparser.parse_from_bytes(
      base64.urlsafe_b64decode(downloaded_msgitem['raw'].encode('ASCII')))
    if 'labelIds' in downloaded_msgitem and 'CHAT' in downloaded_msgitem['labelIds']:
      source = self.credentials['protocol'] + '-chat'
      store_subject = 'Chat'
      store_timestamp = datetime.datetime.fromtimestamp(
        float(downloaded_msgitem['internalDate']) / 1000)
      store_to = self.credentials['emaillabel']
    else:
      source = self.credentials['protocol']
      store_subject = m.subject
      store_timestamp = m.date
      store_to = ' '.join([y for x in m.to for y in x])  # list of tuples
    store_from = ' '.join([y for x in m.from_ for y in x])  # list of tuples
    return {
      'ar3mr_id': str(downloaded_msgitem['id']),
      'ar3mr_ts': store_timestamp,
      'ar3mr_subj': store_subject,
      'ar3mr_to': store_to,
      'ar3mr_from': store_from,
      'ar3mr_source': source,
      'ar3mr_gmail_data': json.dumps(downloaded_msgitem),
      'ar3mr_raw': base64.urlsafe_b64decode(downloaded_msgitem['raw'].encode('ASCII'))
    }


class IMAPServerConnection(ServerConnection):

  """
  IMAP Specific server connection
  """

  LIST_RESPONSE_PATTERN = re.compile(
    r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)'
  )

  @staticmethod
  def _strip_folder_name(folder_list_record: str):
    """
    Code borrowed from https://pymotw.com/3/imaplib/
    """
    match = IMAPServerConnection.LIST_RESPONSE_PATTERN.match(folder_list_record)
    unused1, unused2, mailbox_name = match.groups()  # pylint: disable=unused-variable
    mailbox_name = mailbox_name.strip('"')
    return mailbox_name

  @staticmethod
  def create_imap_connection(credentials: dict):
    logger.debug(f"Logging into IMAP Server {credentials['imap_user']} "
                 f"@ {credentials['imap_host']}")
    if credentials['imap_starttls']:
      ctx = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1)
      imap_conn = imaplib.IMAP4(host=credentials['imap_host'],
                                port=credentials['imap_port'])
      imap_conn.starttls(ssl_context=ctx)
    else:
      imap_conn = imaplib.IMAP4_SSL(host=credentials['imap_host'],
                                    port=credentials['imap_port'])
    imap_conn.login(credentials['imap_user'], credentials['imap_password'])
    return imap_conn

  def close(self):
    try:
      if self.conn:
        self.conn.logout()
    except Exception: # pylint: disable=broad-except
      logger.exception('Error during closing of connection')

  def __init__(self, credentials: dict):
    super(IMAPServerConnection, self).__init__()
    self.credentials = credentials
    self.conn = IMAPServerConnection.create_imap_connection(credentials=credentials)

  def retrieve_folders(self):
    folders = []
    account = self.credentials['imap_user'] + '@' + self.credentials['imap_host']
    return_status, binary_code_folders = self.conn.list('""', '*')
    if return_status == 'OK':
      for folder_rec in binary_code_folders:
        folders.append({
          'name': IMAPServerConnection._strip_folder_name(folder_rec.decode('utf-8'))
        })
    else:
      raise RuntimeError(f'Downloading IMAP4 folder list. Status: {str(return_status)}.'
                         f'Account: {account}')
    return folders

  def retrieve_messages(self, since_date: datetime.datetime, dupes_filterset: set):
    max_error_limit = 1000
    account = self.credentials['imap_user'] + '@' + self.credentials['imap_host']
    logger.debug(f'Retrieve IMAP messages for {account}')
    folders = self.retrieve_folders()
    msg_error_count = 0
    for folderix, folder_rec in enumerate(folders):
      folder = folder_rec['name']
      logger.debug(
        f'Processing folder {folder} ({folderix + 1}/{len(folders)}) for {account}')
      try:
        self.conn.select(folder)
        search_string = '(SINCE ' + since_date.strftime('%d-%b-%Y') + ')'
        folder_return_status, folder_data = self.conn.search(None, search_string)
        if folder_return_status != 'OK':
          folder_error_descr = f'Folder Error: Failure to download messages for ' \
                               f'{folder_return_status} with search {search_string} ' \
                               f'for {account}'
          logger.error(folder_error_descr)
          yield {
            'is_error': 'True',
            'error_description': f'Error in Folder {folder}: {folder_error_descr}',
            'error_scope': 'FOLDER'
          }
      except Exception as e: # pylint: disable=broad-except
        yield {
          'is_error': 'True',
          'error_description': f'Error in Folder {folder}: {str(e)}',
          'error_scope': 'FOLDER'
        }
        folder_return_status = 'Exception'
      if folder_return_status == 'OK':
        download_size = len(folder_data[0].split())
        for msg_ix, msg_num in enumerate(folder_data[0].split()):
          logger.debug(
            f'Download message {msg_ix + 1}/{download_size} in {folder}: {msg_num}')
          msg_return_status, msg_data = self.conn.fetch(msg_num, '(RFC822)')
          if not msg_data[0]:
            msg_return_status = f'Error: message is empty object {msg_num} in {folder}'
          if msg_return_status != 'OK':
            msg_error_count += 1
            error_descr = f'Msg Error: {msg_return_status}. Pulling {msg_num}/{folder}'
            logger.error(error_descr)
            yield {
              'is_error': 'True',
              'error_description': error_descr,
              'error_scope': 'MESSAGE'
            }
            if msg_error_count > max_error_limit:
              raise RuntimeError(
                f'Exceeding number of messages errors {msg_error_count} in {account}')
          if msg_return_status == 'OK':
            message_obj = mailparser.parse_from_bytes(msg_data[0][1])
            msg_id = str(message_obj.message_id)
            if msg_id in dupes_filterset:
              logger.debug(f'Message dupe found and ignored: {msg_id}')
              yield {
                'is_dupe': 'True',
              }
            else:
              logger.debug(f'Returning Message - no dupe, no error: {msg_num}')
              yield self.convert_imap_msgobject_to_return_dict(msg_data[0][1])

  def convert_imap_msgobject_to_return_dict(self, imap_msgobject):
    mail_object = mailparser.parse_from_bytes(imap_msgobject)
    return {
      'ar3mr_id': str(mail_object.message_id),
      'ar3mr_ts': mail_object.date,
      'ar3mr_subj': mail_object.subject,
      'ar3mr_to': ' '.join([y for x in mail_object.to for y in x]),  # list of tuples
      'ar3mr_from': ' '.join([y for x in mail_object.from_ for y in x]),
      # list of tuples,
      'ar3mr_source': self.credentials['protocol'],
      'ar3mr_raw': imap_msgobject
    }


def get_folders_for_email(credsl_root_path: Path, emaillabel: str):
  generic_creds = util_lib.load_generic_credentials(credsl_root_path, emaillabel)
  svr_conn = create_server_connection(generic_creds)
  folders = svr_conn.retrieve_folders()
  svr_conn.close()
  return folders
