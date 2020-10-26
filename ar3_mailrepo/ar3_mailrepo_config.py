#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

import yaml
from pathlib import Path

class AppConfig:

  @staticmethod
  def from_configfile(config_file, verify_paths=True):
    config_obj = AppConfig()
    with open(config_file) as config_fp:
      config_obj.data = yaml.safe_load(config_fp)
    return config_obj

  @staticmethod
  def from_dict(config_data:dict, verify_paths=True):
    config_obj = AppConfig()
    config_obj.data = config_data
    if verify_paths:
      config_obj._verify_paths()
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