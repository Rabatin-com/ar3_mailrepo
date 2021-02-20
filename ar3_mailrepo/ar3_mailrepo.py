#!/usr/bin/env python3
# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

"""
Main module to execute mail manager
"""

import argparse
import datetime
import logging
import platform
from pathlib import Path

import mailparser
import sqlalchemy

import ar3_mailrepo_config
import ar3_mailrepo_lib
import ar3_mailrepo_version_info

import searcher
import storage
import util_lib
import util_logger
from ar3_mailrepo_lib import get_folders_for_email

util_logger.apply_logger_handler()

logger = logging.getLogger('ar3_mailrepo')


def create_dupefilter_list(dbconn, emaillabel: str, lookback_days=2):
  stmt = sqlalchemy.select(
    [storage.messagedata.c.msg_id,
     storage.messagedata.c.msg_ts,
     storage.messagedata.c.id]).where \
    (storage.messagedata.c.email_account == emaillabel)
  result = dbconn.execute(stmt)
  resultset = []
  for item in result.fetchall():
    if item['msg_id'] and item['msg_ts']:
      resultset.append({'id': item[0], 'date': item[1]})
  if len(resultset) < 1:
    return datetime.date(1970, 1, 1), set()
  since_date = (max([x['date'] for x in resultset])).date() - datetime.timedelta(
    days=lookback_days)
  dupelist = {x['id'] for x in resultset if x['date'].date() >= since_date}
  logger.debug(
    f'Dupe List for {emaillabel} and lookback {lookback_days}: '
    f'{since_date} and len {len(dupelist)}')
  return since_date, dupelist


def download_emails_to_cache(dbconn, emaillabel: str, cachepath_root: Path,
                             credentials_root: Path):
  generic_creds = util_lib.load_generic_credentials(credentials_root, emaillabel)
  svr_conn = ar3_mailrepo_lib.create_server_connection(generic_creds)
  new_cache = storage.create_new_timestamped_cache_path(Path(cachepath_root / emaillabel))
  since_dt, dupefilterlist = create_dupefilter_list(dbconn=dbconn, emaillabel=emaillabel)
  svr_conn.retrieve_messages_to_cache(new_cache, since_dt, dupefilterlist)
  svr_conn.close()
  cachefolder = storage.DataCacheFolder(new_cache)
  # stored_in_db = cachefolder.store_messages_in_database(dbconn)
  # logger.debug(f'Downloaded and stored {stored_in_db} messages for {emaillabel}')


def arg_command_rebuild_search(index_root: Path, dbconn):
  searcher.build_index_from_scratch(index_root, dbconn)


def arg_command_search(index_root: Path, search_string):
  searcher.search_and_print(index_root, search_string)


def arg_command_list_all_emails(credentials_root_path: Path):
  for email in util_lib.retrieve_all_email_labels(credentials_root_path):
    print(email)


def arg_command_list_folders_for_email(credential_root_path: Path, emaillabel: str):
  if emaillabel.upper() == 'ALL':
    for email in util_lib.retrieve_all_email_labels(credential_root_path):
      arg_command_list_folders_for_single_email(credential_root_path, email)
  else:
    arg_command_list_folders_for_single_email(credential_root_path, emaillabel)


def arg_command_list_folders_for_single_email(credsl_root_path: Path, emaillabel: str):
  try:
    print(emaillabel)
    print('-' * len(emaillabel) * 2)
    for folder in get_folders_for_email(credsl_root_path, emaillabel):
      print(folder)
    print('=' * len(emaillabel) * 2)
  except Exception as e:
    print(f'Error for account {emaillabel}: {e}')


def arg_command_download_and_store_emails(emaillabel: str,
                                          cacheeroot: Path,
                                          credentials_root: Path,
                                          db_engine=storage.DBEngine):
  if emaillabel == 'ALL':
    emails = util_lib.retrieve_all_email_labels(credentials_root)
  else:
    emails = [emaillabel]
  for email in emails:
    logger.debug(f'Executing download for email {email}')
    download_emails_to_cache(db_engine.conn(), email, cacheeroot, credentials_root)


def rebuild_data_base_from_cache_for_email(cache_root: Path, email_label: str, dbconn):
  logger.debug(f'Rebuilding DB for email label {email_label} in {cache_root} -  BEGIN')
  total_stored = 0
  for datafolder in util_lib.list_avilable_cache_data_for_email(cache_root,
                                                                email_label):
    thisfolder = storage.DataCacheFolder(cache_root / email_label / datafolder)
    logger.debug(f'Processing {thisfolder.name}')
    if not thisfolder.has_download_report():
      logger.debug(f'Ignoring folder as it has no download report: {thisfolder.name}')
    else:
      total_stored += thisfolder.store_messages_in_database(dbconn)
  logger.debug(
    f'Rebuilding DB for email label {email_label} in '
    f'{cache_root}: {total_stored} total message(s) - END')
  return total_stored


def arg_command_rebuild_database(db_engine: storage.DBEngine, datacache_root: Path,
                                 email_label_or_all: str):
  total_stored = 0
  if email_label_or_all.upper() == 'ALL':
    emailfolders = util_lib.list_all_available_cache_data(datacache_root)
    for email in emailfolders:
      total_stored += rebuild_data_base_from_cache_for_email(dbconn=db_engine.conn(),
                                                             cache_root=datacache_root,
                                                             email_label=email)
  else:
    total_stored += rebuild_data_base_from_cache_for_email(dbconn=db_engine.conn(),
                                                           cache_root=datacache_root,
                                                           email_label=email_label_or_all)
  logger.debug(
    f'Imported {total_stored} Messages into '
    f'Database for email label(s): {email_label_or_all}')


def arg_command_create_db(db_engine: storage.DBEngine):
  logger.debug(f'Attemtping tp create database {db_engine.description()}')
  db_engine.establish_conn()
  if not db_engine.is_db_a_mailrepo():
    db_engine.populate_database()
  else:
    logger.debug(f'Already has AR3 MailRepo Tables: {db_engine.description()}')


def arg_command_init_cache(data_cache_dir: Path, credential_root_dir: Path,
                           db_engine: storage.DBEngine):
  logger.debug(
    f'Attempt to init cache in {data_cache_dir} using creds from{credential_root_dir}')
  if data_cache_dir.exists():
    logger.debug(f'Cache directory already exists. Not doing anything: f{data_cache_dir}')
  else:
    conf.cache_dir().mkdir(parents=True)
  create_count = 0
  emails = util_lib.retrieve_all_email_labels(credential_root_dir)
  for emailpath in emails:
    data_path_for_email = data_cache_dir / emailpath
    if not (data_path_for_email).exists():
      data_path_for_email.mkdir(parents=True)
      create_count += 1
  logger.debug(
    f'{create_count} Folder(s) created. {len(emails) - create_count} already existed')
  arg_command_create_db(db_engine)


def arg_command_extract_email(dbconn, msg_uuid, email_export_root: Path):
  logger.debug(f'Extracting Msg {msg_uuid} into folder {email_export_root}')
  result = storage.extract_msg_from_db_by_uuid(dbconn, msg_uuid)
  outpath = util_lib.safe_create_path(email_export_root, msg_uuid)
  logger.debug(f'Storing Msg Data for {msg_uuid} into folder {outpath}')
  return store_message_as_extract(mailparser.parse_from_bytes(result['raw_data']),
                                  msg_uuid)


def arg_command_extract_pickle_obj(pickle_file_name: Path, extra_root: Path):
  outpath = util_lib.safe_create_path(extra_root, Path(pickle_file_name.name))
  logger.debug(f'Storing Msg Data for {pickle_file_name} into folder {outpath}')
  msg_object = storage.load_pickle_object_as_data(pickle_file_name)['raw_data']
  return store_message_as_extract(mailparser.parse_from_bytes(msg_object), outpath)


def store_message_as_extract(msg: mailparser.MailParser, outpath: Path, msg_uuid=None):
  msg.write_attachments(Path(outpath / 'attachments'))
  try:
    with open(outpath / 'headers.txt', 'w', encoding='utf-8-sig') as f:
      for hdr, hdr_data in msg.headers.items():
        f.write(f'{hdr}:{hdr_data}')
        f.write('\n')
    with open(outpath / 'message.txt', 'w', encoding='utf-8-sig') as f:
      for txt in msg.text_plain:
        f.write(f'{txt}"\n"')
    with open(outpath / 'message.html', 'w', encoding='utf-8-sig') as f:
      for txt in msg.text_html:
        f.write(f'{txt}"\n"')
    with open(outpath / 'message_as_string.txt', 'w', encoding='utf-8-sig') as f:
      f.write(msg.message_as_string)
  except UnicodeError as ue:
    logger.exception(f'Unicode error in msg {msg_uuid}: {str(ue)}')
    with open(outpath / 'headers.txt_UNICODE_BINARY', 'wb') as f:
      for hdr, hdr_data in msg.headers.items():
        f.write(bytes(hdr_data, encoding='utf-8-sig'))
    with open(outpath / 'message.txt_UNICODE_BINARY', 'wb') as f:
      for txt in msg.text_plain:
        f.write(bytes(txt, encoding='utf-8-sig'))
    with open(outpath / 'message.html_UNICODE_BINARY', 'wb') as f:
      for txt in msg.text_html:
        f.write(bytes(txt, encoding='utf-8-sig'))


def args_command_report_dupes(dbconn):
  smt = sqlalchemy.select([storage.messagedata.c.msg_id])
  result = dbconn.execute(smt)
  cntdict = {}
  for item in result.fetchall():
    cntdict[item['msg_id']] = cntdict.get(item['msg_id'], 0) + 1
  cntdict = {key: val for key, val in cntdict.items() if val > 1}
  extr = lambda msg_id: storage.extract_msg_from_db_by_msg_id(dbconn, msg_id)
  dupelist = {key: extr(key) for key, val in cntdict.items() if val > 1}
  for k, v in dupelist.items():
    print(k)
    print('-' * len(k))
    print(len(v), ': ', ','.join([x['email_account'] for x in v]))
    print(' ')


def arg_command_extract_email_for_acct(dbconn, email_label, email_export_root: Path):
  bulk_export_root = util_lib.safe_create_path(Path(email_export_root), Path(email_label))
  for ix, msg_uuid in enumerate(storage.msg_uuid_per_account(dbconn, email_label)):
    logger.debug(
      f'Extracting messages for {email_label}: '
      f'{ix + 1}/{len(storage.msg_uuid_per_account(dbconn, email_label))} '
      f'into {bulk_export_root}')
    arg_command_extract_email(dbconn=dbconn, msg_uuid=msg_uuid,
                              email_export_root=bulk_export_root)


if __name__ == '__main__':

  logger.debug(f'{ar3_mailrepo_version_info.into_string()} '
               f'in Python {platform.python_version()} '
               f'on {platform.platform()}')

  parser = argparse.ArgumentParser(prog='AR3 Mail Repo', usage='Print -h for help')

  parser.add_argument('--create_db',
                      help='Creates a new DB. If on a server, the database must be already created, it will only be populated. With SQLite it will create the database file',
                      action='store_true')
  parser.add_argument('--init_cache', help='Creates directories to hold file caches. Can be safely re-run, does not change existing directories', action='store_true')

  parser.add_argument('--list_emails', help='Lists all email addresseses for which there is a connection specification available for download',
                      action='store_true')

  parser.add_argument('--list_folders', help='Lists all REMOTE IMAP folders for a given email or ALL. Needs access to the remote accoung',
                      action='store', type=str)

  parser.add_argument('--download', help='Downloads all emails for given email',
                      action='store', type=str)

  parser.add_argument('--rebuild_db_data',
                      help='Repopulates a database with the contents of a download cache.\
                       Does NOT check for duplicates. Pass email as arg or ALL for all',
                      action='store', type=str)



  parser.add_argument('--rebuild_index', help='Rebuild Search Index',
                      action='store_true')
  parser.add_argument('--search', help='Searches for a string',
                      action='store', type=str)



  parser.add_argument('--extract_pickle_obj',
                      help='COnverts specific pikcle file into message extract',
                      action='store', type=str)


  parser.add_argument('--store_message_cache_into_db',
                      help='Stores messages from a cache into the DB',
                      action='store', type=str)

  parser.add_argument('--extract_email',
                      help='Extracts an email with a given UUID',
                      action='store', type=str)

  parser.add_argument('--report_message_id_dupes',
                      help='Creates a report of all dupe message IDs',
                      action='store_true')


  parser.add_argument('--extract_email_for_acct',
                      help='Extracts emails for given account',
                      action='store', type=str)

  args = parser.parse_args()

  conf = ar3_mailrepo_config.AppConfig.from_configfile('ar3_mailreport_config.yaml')
  email_storage_db_engine = storage.DBEngine(conf)

  try:

    if args.rebuild_index:
      arg_command_rebuild_search(conf.search_index_root(), email_storage_db_engine.conn())

    if args.search:
      arg_command_search(conf.search_index_root(), args.search)

    if args.rebuild_db_data:
      arg_command_rebuild_database(db_engine=email_storage_db_engine,
                                   datacache_root=conf.cache_dir(),
                                   email_label_or_all=args.rebuild_db_data)

    if args.list_emails:
      arg_command_list_all_emails(credentials_root_path=conf.credentials_root())

    if args.create_db:
      arg_command_create_db(db_engine=email_storage_db_engine)

    if args.init_cache:
      arg_command_init_cache(data_cache_dir=conf.cache_dir(),
                             credential_root_dir=conf.credentials_root(),
                             db_engine=email_storage_db_engine)

    if args.store_message_cache_into_db:
      cachfolder = storage.DataCacheFolder(args.store_message_cache_into_db)
      cachfolder.store_messages_in_database(email_storage_db_engine.conn())

    if args.list_folders:
      # args.list_folders has email label as argument
      arg_command_list_folders_for_email(credential_root_path=conf.credentials_root(),
                                         emaillabel=args.list_folders)

    if args.extract_pickle_obj:
      arg_command_extract_pickle_obj(Path(args.extract_pickle_obj),
                                     conf.email_export_root())

    if args.download:
      arg_command_download_and_store_emails(emaillabel=args.download,
                                            cacheeroot=conf.cache_dir(),
                                            credentials_root=conf.credentials_root(),
                                            db_engine=email_storage_db_engine)
    if args.extract_email:
      arg_command_extract_email(dbconn=email_storage_db_engine.conn(),
                                msg_uuid=args.extract_email,
                                email_export_root=conf.email_export_root())

    if args.report_message_id_dupes:
      args_command_report_dupes(dbconn=email_storage_db_engine.conn())

    if args.extract_email_for_acct:
      arg_command_extract_email_for_acct(dbconn=email_storage_db_engine.conn(),
                                         email_label=args.extract_email_for_acct,
                                         email_export_root=conf.email_export_root())



  except Exception:  # pylint: disable=broad-except
    logger.exception('Exception caught as MAIN level')
  finally:
    email_storage_db_engine.close()
