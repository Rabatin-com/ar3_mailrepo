#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------

import mailparser
from pathlib import Path

class Email:

  def __init__(self, msg_uq_id, msg_object_as_mailparser):
    self.msg_id = msg_uq_id
    self.msg_obj = msg_object_as_mailparser

  def extract_to_file(self, extract_root: Path):
    pass


