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
Contains version and copyright information for this version of MailRepo
"""

def current_system_version():
  return '0.01'


def prod_status():
  return 'Development'


def app_name():
  return 'AR3 Mail Repo'


def rabatin_copyright():
  return 'Arthur Rabatin (C) 2020'


def into_string():
  return f'{app_name()} {current_system_version()}. Copyright {rabatin_copyright()}'
