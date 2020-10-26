#!/usr/bin/env python3

# ---------------------------------------------
# Copyright Arthur Rabatin. See www.rabatin.com
# ---------------------------------------------
from whoosh.index import create_in
from whoosh import index
from whoosh.fields import Schema, TEXT
from whoosh.qparser import QueryParser

import sqlalchemy
import storage
from pathlib import Path

schema = Schema(msg_uuid=TEXT(stored=True), email_account=TEXT(stored=True), msg_subj=TEXT(stored=True), msg_to=TEXT(stored=True), msg_from=TEXT(stored=True))

def build_index_from_scratch(indexpath:Path, dbconn):
  if not indexpath.exists():
    indexpath.mkdir(parents=True)

  ix = create_in(indexpath, schema)
  writer = ix.writer()

  semt = sqlalchemy.select ([storage.messagedata.c.msg_uuid, storage.messagedata.c.email_account, storage.messagedata.c.msg_subj, storage.messagedata.c.msg_to,  storage.messagedata.c.msg_from])
  result = dbconn.execute(semt)
  for item in result.fetchall():
    writer.add_document(msg_uuid=item[0], email_account=item[1], msg_subj=item[2], msg_to=item[3], msg_from=item[4])
  writer.commit()

def search_and_print(indexpath:Path, searchstring:str):
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