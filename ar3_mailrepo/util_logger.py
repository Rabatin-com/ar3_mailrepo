#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

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
