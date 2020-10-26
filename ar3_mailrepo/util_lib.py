#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

"""
Utility functions to operate mail repo
"""
import datetime
import json
from pathlib import Path
import collections

import sqlalchemy
import uuid
import storage

PICKLE_PROTOCOL =4

def create_unique_id():
  return str(uuid.uuid4())


def retrieve_all_email_labels(credential_root_path: Path):
  emails=[]
  if not credential_root_path.exists():
    raise RuntimeError(f'Does not exist: {credential_root_path}')
  for child in credential_root_path.iterdir():
    if '@' in str(child.name) and child.is_dir():
      if Path(Path(child) / 'credentials.json').exists():
        emails.append(child.name)



  return emails


def load_generic_credentials(credential_root_path: Path, email_label: str):
  auth_data_path = credential_root_path / Path(email_label)
  with open(Path(auth_data_path / Path('credentials.json'))) as f:
    creds =  json.load(f)
  creds['emaillabel']=email_label
  if creds['protocol'] == 'imap4':
    if 'imap_port' not in creds:
      creds['imap_port'] = 993
    if 'imap_starttls' not in creds:
      creds['imap_starttls'] = 0
    creds['imap_starttls'] = bool(creds['imap_starttls'])
  elif creds['protocol'] == 'gmail':
    creds['gmail_oauth_token_cache'] = auth_data_path / 'gmail' / 'token.pickle'
    creds['gmail_oauth_credentials'] = auth_data_path / 'gmail' / 'credentials.json'
  else:
    raise RuntimeError(f"Unkown Protocol {creds['protocol']}")
  return creds

def list_all_available_cache_data(datacache_root_path: Path):
  cache_data = collections.defaultdict(list)
  for emailfolder in datacache_root_path.glob('*'):
    if '@' in str(emailfolder) and emailfolder.is_dir():
      cache_data[emailfolder.name] = [x for x in list_avilable_cache_data_for_email(datacache_root_path, emailfolder.name)]
  return cache_data


def list_avilable_cache_data_for_email(datacache_root_path: Path, emaillabel:str):
  for datafolder in Path(datacache_root_path/emaillabel).glob('*'):
    yield datafolder.name



def safe_create_path(create_root_path:Path, new_stem:Path):
    cnt = 0
    test_stem = new_stem
    while (Path(create_root_path/test_stem)).exists():
        cnt +=1
        test_stem = Path(f'{new_stem}({cnt})')
    Path(create_root_path/test_stem).mkdir(parents=True)
    return Path(create_root_path/test_stem)