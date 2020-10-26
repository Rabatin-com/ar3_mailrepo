#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

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
