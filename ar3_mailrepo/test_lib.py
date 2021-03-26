#!/usr/bin/env python3

# ---------------------------------------------------------------------------
# Copyright 2016-2021 Arthur Rabatin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ---------------------------------------------------------------------------

"""
Unit Tests
"""

import traceback
import unittest
from pathlib import Path
import imap_generic
import ar3_mailrepo
import storage
from ar3_mailrepo_config import AppConfig
import util_lib
import gmail

GLOBAL_TEST_EMAIL = ['arthur.rabatin@outlook.com', 'rabatin@gmail.com']

class TestUtilFunctions(unittest.TestCase):

  def setUp(self):

    self.testemail = GLOBAL_TEST_EMAIL
    self.testConfig = AppConfig.from_dict({'cache_dir': \
                                        Path('D:/Test1-EmailManager-Data'),
                                      'credentials_root': \
                                        Path('D:/arthur.data/Sync/Email-Manager'),
                                           "db_driver": "sqlite", "db_driver_credentials": {
        "sqlite_filename": "storage.sqllite"}})

  def test_list_all_available_cache_data(self):
    print(util_lib.list_all_available_cache_data(self.testConfig.cache_dir()))

  def test_list_avilable_cache_data_for_email(self):
    print(util_lib.list_avilable_cache_data_for_email(self.testConfig.cache_dir(), self.testemail[0]))

  def test_retrieve_all_email_labels(self):
    for x in util_lib.retrieve_all_email_labels(self.testConfig.credentials_root()):
      print('----->', x)

  def test_load_generic_credentials(self):
    for email in self.testemail:
      print('-' * 50)
      print(util_lib.load_generic_credentials(self.testConfig.credentials_root(), email))
      print('=' * 50)

class TestConfig(unittest.TestCase):

  def test_load_app_config(self):
    self.testConfig = AppConfig.from_configfile('ar3_mailreport_config.yaml', verify_paths=False)
    print(self.testConfig.data)


class TestIMAPConn(unittest.TestCase):

  def setUp(self):
    self.testConfig = AppConfig.from_dict({'cache_dir': \
                                             Path('D:/Test1-EmailManager-Data'),
                                           'credentials_root': \
                                             Path('D:/arthur.data/Sync/Email-Manager'),
                                           "db_driver": "sqlite", "db_driver_credentials": {
        "sqlite_filename": "storage.sqllite"}})

  def test_imapconn(self):
    has_err = False
    for email in util_lib.retrieve_all_email_labels(self.testConfig.credentials_root()):
      creds = util_lib.load_generic_credentials(self.testConfig.credentials_root(), email)


      print('='*50)
      print('Email account....: ', email)
      print(creds)
      try:
        if creds['protocol'] == 'imap4':
          print ('Result .........: ', imap_generic.imap_test_connection_via_noop(creds))
        elif creds['protocol'] == 'gmail':
          print('Result .........: ', gmail.test_connect_to_account(self.testConfig.credentials_root() / email / Path('gmail') ))
        else:
          raise Exception('Unknown protocol'+ creds['protocol'])

      except Exception as e:
        # traceback.print_exc()
        print ('Exception', e)
        has_err = True

    if has_err:
      self.fail()


class TestDataBaseActivity(unittest.TestCase):

  def setUp(self):
    self.testemail = GLOBAL_TEST_EMAIL
    self.testConfig = AppConfig.from_dict({'cache_dir': \
                                             Path('D:/Test2-EmailManager-Data-No-Database'),
                                           'credentials_root': \
                                             Path('D:/arthur.data/Sync/Email-Manager'),
                                           "db_driver": "sqlite", "db_driver_credentials": {
        "sqlite_filename": "storage.sqllite"}})
    dbpath = Path(self.testConfig.cache_dir() / 'storage.sqllite')
    if dbpath.is_file():
      # in python 3.8 'missing_ok' was added
      # todo rewrite when using python 3.8
      dbpath.unlink()
    db_engine = storage.DBEngine(self.testConfig)
    db_engine.populate_database()

  def test_rebuild_database_from_cached_files_all_emails(self):
    db_engine = storage.DBEngine(self.testConfig)
    ar3_mailrepo.rebuild_database_from_cached_files_all_emails(self.testConfig.cache_dir(),
                                                  self.testConfig.credentials_root(),
                                                  db_engine.conn())

if __name__ == '__main__':
    unittest.main()

