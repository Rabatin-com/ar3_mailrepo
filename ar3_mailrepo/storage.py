import datetime
import json
import logging
import pickle
from pathlib import Path
import ar3_mailrepo_version_info

logger = logging.getLogger('ar3_mailrepo.storage')

from sqlalchemy import Table, Column, LargeBinary, Integer, String,Text, DateTime, MetaData
from sqlalchemy import create_engine, inspect, select

import ar3_mailrepo_config


def create_new_timestamped_cache_path(email_cache_folder: Path):
  ts_path = Path(datetime.datetime.now().strftime('%a_%b_%d_%Y--%H_%M_%S_%f'))
  new_dated_folder = email_cache_folder / ts_path
  new_dated_folder.mkdir(parents=True)
  return new_dated_folder


def create_download_report(len_messages_listed: int,
                           download_since_date: datetime.datetime,
                           len_filter_list: int,
                           len_removed_dupes: int,
                           download_duration_seconds: int,
                           download_count: int,
                           download_start: datetime.datetime,
                           output_filename: Path
                           ):
  result = {
    'len_messages_listed': len_messages_listed,
    'download_since_date': download_since_date.isoformat(),
    'len_filter_list': len_filter_list,
    'len_removed_dupes': len_removed_dupes,
    'download_duration_seconds': download_duration_seconds,
    'download_count': download_count,
    'download_start': download_start.isoformat()
  }
  with open(output_filename, 'w') as outf:
    json.dump(result, outf, indent=4)
  logger.debug(f'Created download report {output_filename}')


def load_pickle_object_as_data(picklefilename:Path):
  with open(picklefilename, 'rb') as f:
    load_msg = pickle.load(f)

    msg = {}
    for k,v in load_msg.items():
      if v and isinstance(v, str):
          msg[k] = v.replace('\x00', '')
      else:
        msg[k] = v

    return {
        'msg_uuid': msg['ar3mr_uuid'],
        'email_account': msg['ar3mr_email_account'],
        'msg_id': msg['ar3mr_id'],
        'msg_ts': msg['ar3mr_ts'],
        'msg_subj': msg['ar3mr_subj'],
        'msg_from': msg['ar3mr_from'],
        'msg_to': msg['ar3mr_to'],
        'source': msg['ar3mr_source'],
        'dnload_ts': msg['ar3mr_downloadtime'],
        'raw_data': msg['ar3mr_raw'],
        'gmail_data': msg['ar3mr_gmail_data']
      }

class DataCacheFolder:

  def __init__(self, foldername: Path):
    self.name = Path(foldername)
    self.downloadreport_file = Path(self.name / 'download_report.json')

  def has_download_report(self):
    return self.downloadreport_file.exists()

  def load_download_report(self):
    if not self.has_download_report():
      raise Exception(f'Could not find {self.downloadreport_file}')
    with open(self.downloadreport_file) as f:
      return json.load(f)

  def message_data_files(self):
    return len(self.name.glob('*.pickle'))

  def store_messages_in_database(self,dbconn):
    batchsize=2000

    store_list=[]
    message_files = [x for x in self.name.glob('*.pickle')]
    for ix, filename in enumerate(message_files):
      logger.debug(f'Loading file {ix + 1}/{len(message_files)}: {filename}')
      store_list.append(load_pickle_object_as_data(filename))
      # logger.debug(f"{msg['ar3mr_ts']} in " )
      if ((ix + 1) % batchsize == 0) or (ix + 1 == len(message_files)):
        logger.debug(f'Reached limit to insert in DB: {len(store_list)}')
        msg_ins = messagedata.insert()
        dbconn.execute(msg_ins, store_list)
        store_list.clear()
        logger.debug(f'Insert done')
    if len([x for x in self.name.glob('*.pickle')]) != len(message_files):
      raise Exception(f'Directory {self.name} has been modified since DB insert started')
    return len(message_files)


def msg_uuid_per_account(dbconn,email_account):
  smt = select([messagedata.c.msg_uuid]).where(messagedata.c.email_account == email_account)
  result = dbconn.execute(smt)
  all_uuids = [x['msg_uuid'] for x in result.fetchall()]
  logger.debug(f'Extracted {len(all_uuids)} UUID(s) for account {email_account}')
  return all_uuids

def extract_msg_from_db_by_uuid(dbconn, msg_uuid):
  result = extract_msg_from_db_by_uuid_or_msgid(dbconn, 'uuid', msg_uuid)
  if len(result) > 1:
    raise RuntimeError(f'Found more than 1 message for uuid {msg_uuid}')
  if result:
    return result[0]
  else:
    return []

def extract_msg_from_db_by_msg_id(dbconn, msg_id):
  return extract_msg_from_db_by_uuid_or_msgid(dbconn, 'msg_id', msg_id)

def extract_msg_from_db_by_uuid_or_msgid(dbconn, query_type, query_id):
  if query_type == 'uuid':
    smt =  select([messagedata.c.msg_uuid,messagedata.c.msg_id,  messagedata.c.email_account, messagedata.c.raw_data]).where(messagedata.c.msg_uuid == str(query_id))
  elif query_type == 'msg_id':
    smt = select([messagedata.c.msg_uuid,messagedata.c.msg_id,  messagedata.c.email_account, messagedata.c.raw_data]).where(messagedata.c.msg_id == str(query_id))
  else:
    raise RuntimeError(f'Unknown Query Type {query_type}')
  result = dbconn.execute(smt)
  results = []
  for row in result.fetchall():
    results.append({
      'msg_id': row['msg_id'],
      'msg_uuid': row['msg_uuid'],
      'email_account': row['email_account'],
      'raw_data':row['raw_data']
    })
  return results



  # stmt = sqlalchemy.select(
  #     [storage.messagedata.c.download_id,
  #      storage.messagedata.c.msg_ts]).where /
  #     (storage.messagedata.c.email_account == email_label)


  # semt = sqlalchemy.select ([storage.messagedata.c.msg_uuid, storage.messagedata.c.email_account, storage.messagedata.c.msg_subj, storage.messagedata.c.msg_to,  storage.messagedata.c.msg_from])
  # result = dbconn.execute(semt)



metadata = MetaData()

messagedata = Table('messagedata', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('msg_uuid', String(50), nullable=False,unique=True),
                    Column('email_account', String(200), nullable=False),
                    Column('msg_id', Text(), nullable=True),
                    Column('msg_ts', DateTime, nullable=True),
                    Column('msg_subj', Text(), nullable=True),
                    Column('msg_to', Text(), nullable=True),
                    Column('msg_from', Text(), nullable=True),
                    Column('source', String(50), nullable=True),
                    Column('dnload_ts', DateTime, nullable=True),
                    Column('raw_data', LargeBinary(4294967295), nullable=True),
                    Column('gmail_data', Text(), nullable=True)
                    )

dbinfo = Table('dbinfo', metadata,
               Column('dbversion', Integer, nullable=False),
               Column('app_name', String(100), nullable=False),
               Column('systemversion', String(100), nullable=False),
               Column('copyright', String(100), nullable=False),
               Column('prod_status', String(100), nullable=False)
               )


engine = create_engine(
                "mysql://scott:tiger@localhost/test",
                isolation_level="READ UNCOMMITTED"
            )



class DBEngine:

  def is_db_a_mailrepo(self):
    inspector = inspect(self.conn(validate_as_mailrepo_db=False))
    return dbinfo.name in inspector.get_table_names()

  @staticmethod
  def _load_credentials_file(cred_file:Path):
    with open(cred_file, 'r') as f:
      return json.load(f)

  def _create_conn(self):
    engine= None
    if self.app_config.data['db_driver'] == 'sqlite':
      dbfile = self.app_config.data['db_driver_credentials']['sqlite_file_path']
      self.conn_description = f'SQLite DB {dbfile}'
      engine = create_engine(f'sqlite:///{dbfile}')
    elif self.app_config.data['db_driver'] == 'mssql_local':
      srv = self.app_config.data['db_driver_credentials']['host']
      db = self.app_config.data['db_driver_credentials']['database_name']
      self.conn_description ='Local MS SQl: ' + db
      conn_string = f'{srv}/{db}?driver=SQL+Server+Native+Client+11.0?Trusted_Connection=yes'
      engine = create_engine(f'mssql+pyodbc://{conn_string}')
    elif self.app_config.data['db_driver'] == 'postgres':
      srv = self.app_config.data['db_driver_credentials']['host']
      login_creds = DBEngine._load_credentials_file(self.app_config.data['db_driver_credentials']['credentials_file'])
      username = login_creds['username']
      password = login_creds['password']
      db = self.app_config.data['db_driver_credentials']['database_name']
      conn_string = f'postgres://{username}:{password}@{srv}/{db}'
      engine = create_engine(conn_string)
    elif self.app_config.data['db_driver'] == 'mysql':
      srv = self.app_config.data['db_driver_credentials']['host']
      login_creds = DBEngine._load_credentials_file(self.app_config.data['db_driver_credentials']['credentials_file'])
      username = login_creds['username']
      password = login_creds['password']
      db = self.app_config.data['db_driver_credentials']['database_name']
      conn_string = f'mysql://{username}:{password}@{srv}/{db}?charset=utf8mb4&binary_prefix=true'
      # , pool_recycle=3600
      # , isolation_level="READ UNCOMMITTED"
      engine = create_engine(conn_string,pool_recycle=200)
    else:
      raise Exception('Unknown database driver ' + str(self.app_config.data['db_driver']))
    return engine

  def close(self):
    if self._conn:
      pass
    # Todo implement when dealing with other databases

  def __init__(self, app_config: ar3_mailrepo_config.AppConfig):
    self._conn = None
    self.app_config = app_config
    self.conn_description = 'No Connection'

  def description(self):
    return self.conn_description

  def populate_database(self):
    ins = dbinfo.insert()
    metadata.create_all(self.conn(validate_as_mailrepo_db=False))
    self._conn.execute(ins,
                       {'dbversion': 1,
                        'systemversion': ar3_mailrepo_version_info.current_system_version(),
                        'copyright': ar3_mailrepo_version_info.copyright(),
                        'prod_status':  ar3_mailrepo_version_info.prod_status(),
                        'app_name': ar3_mailrepo_version_info.app_name()
                        })
    logger.debug(f'Populated Database as MailRepo')

  def establish_conn(self):
    unused = self.conn(validate_as_mailrepo_db=False)

  def conn(self, validate_as_mailrepo_db=True):
    if not self._conn:
      self._conn = self._create_conn()
      logger.debug(f'Creating Database Connection on demand')
      if validate_as_mailrepo_db and not self.is_db_a_mailrepo():
        raise Exception('Not a valid Mail Repo Database')
    return self._conn
