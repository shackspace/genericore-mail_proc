#!/usr/bin/python2
from pymongo import Connection
from genericore import MongoConnect
import logging, sys

MODULE_NAME = 'mail_proc'

log = logging.getLogger(MODULE_NAME)

DEFAULT_CONFIG = {
  MODULE_NAME : {
    "database" : {
      "collection" : "mail_proc"
    }
  }
}

class mail_proc(MongoConnect): #mongoconnect derives from Configurable!
  
  def __init__(self,conf=None):
    MongoConnect.__init__(self,MODULE_NAME,DEFAULT_CONFIG)
    self.load_conf(conf)

  def process(self,mail):
    db = self.db
    hdr = mail['data']['Header-Fields']
    log.debug (hdr)
    entry = db.users.find_one( { 'user' : hdr['From'] } )
    log.debug (entry)

    if not entry: #already in db, update stats
      entry = {}
      entry['user'] = hdr['From']
      entry['first_post'] = hdr['Date']
      entry['last_post'] = hdr['Date']
      entry['mails'] = [mail]
      entry['mailcount'] = 1
      log.debug('adding new %s' % repr(entry))
    else :
      entry['last_post'] = hdr['Date']
      entry['mails'].append(mail)
      entry['mailcount'] += 1
      log.debug('updating %s' % repr(entry))

    db.users.save(entry)
    del entry["_id"]
    del entry["mails"]
    return entry

  def populate_parser(self,parser): 
    MongoConnect.populate_parser(self,parser)

  def eval_parser(self,parsed): 
    MongoConnect.eval_parser(self,parsed)

