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
Configuration manager for MailRepo
"""

from pathlib import Path

import yaml


class AppConfig:
  """
  Configuration object for the application, such as data paths and db connection details
  Does not / should not contain passwords
  """

  @staticmethod
  def from_configfile(config_file):
    config_obj = AppConfig()
    with open(config_file) as config_fp:
      config_obj.data = yaml.safe_load(config_fp)
    return config_obj

  @staticmethod
  def from_dict(config_data: dict):
    config_obj = AppConfig()
    config_obj.data = config_data
    return config_obj

  def __init__(self):
    self.data = {}

  def cache_dir(self):
    return Path(self.data['cache_dir'])

  def credentials_root(self):
    return Path(self.data['credentials_root'])

  def search_index_root(self):
    return Path(self.data['whoosh_index_root'])

  def email_export_root(self):
    return Path(self.data['email_export_root'])
