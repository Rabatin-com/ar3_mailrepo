#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

def current_system_version():
  return '0.01'

def prod_status():
  return 'Development'

def app_name():
  return 'AR3 Mail Repo'

def copyright():
  return 'Arthur Rabatin (C) 2020'

def into_string():
  return f'{app_name()}: Version {current_system_version()}. Copyright {copyright()}'