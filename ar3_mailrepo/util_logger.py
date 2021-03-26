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
Logging Utilities
"""

import logging
import logging.handlers
import platform
import sys


def apply_logger_handler(screenoutput=True):
  logger = logging.getLogger('ar3_mailrepo')
  logger.setLevel(logging.DEBUG)

  fulllogfilename = 'ar3_mailrepo.log'

  if platform.system() == 'Windows':
    filehandler = logging.FileHandler(fulllogfilename, mode='w')
  else:
    filehandler = logging.handlers. \
      RotatingFileHandler(filename=fulllogfilename,
                          mode='a',
                          maxBytes=300 * 1024 * 1024,
                          backupCount=100)
  generalformatter = logging.Formatter(
    '%(asctime)s [%(levelname)s:%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
  filehandler.setFormatter(generalformatter)
  logger.addHandler(filehandler)

  if screenoutput:
    screenhandler = logging.StreamHandler(sys.stdout)
    screenhandler.setFormatter(generalformatter)
    logger.addHandler(screenhandler)
