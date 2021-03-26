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
Implements the Search Engine
"""

from pathlib import Path

import sqlalchemy
from whoosh import index
from whoosh.fields import Schema, TEXT
from whoosh.index import create_in
from whoosh.qparser import QueryParser

import storage

schema = Schema(msg_uuid=TEXT(stored=True), email_account=TEXT(stored=True),
                msg_subj=TEXT(stored=True), msg_to=TEXT(stored=True),
                msg_from=TEXT(stored=True))


def build_index_from_scratch(indexpath: Path, dbconn):
  if not indexpath.exists():
    indexpath.mkdir(parents=True)

  ix = create_in(indexpath, schema)
  writer = ix.writer()

  semt = sqlalchemy.select(
    [storage.messagedata.c.msg_uuid, storage.messagedata.c.email_account,
     storage.messagedata.c.msg_subj, storage.messagedata.c.msg_to,
     storage.messagedata.c.msg_from])
  result = dbconn.execute(semt)
  for item in result.fetchall():
    writer.add_document(msg_uuid=item[0], email_account=item[1], msg_subj=item[2],
                        msg_to=item[3], msg_from=item[4])
  writer.commit()


def search_and_print(indexpath: Path, searchstring: str):
  ix = index.open_dir(indexpath)
  qp = QueryParser("msg_subj", schema=ix.schema)
  q = qp.parse(searchstring)
  with ix.searcher() as s:
    # print(list(s.lexicon('msg_subj')))
    results = s.search(q, limit=20)
    # print(results.scored_length())
    for r in results:
      print(r)
    # for hit in results:
    #     for h in hit:
    #         print('-'*30)
    #         print(h)
