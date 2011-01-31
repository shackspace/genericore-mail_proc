#!/usr/bin/python2
from genericore import MongoConnect
import logging, sys

log = logging.getLogger('mail_proc')

DEFAULT_CONFIG = {
  "database" : {
    "collection" : "mail_proc"
  }
}

class MailProc(MongoConnect): #mongoconnect derives from Configurable!
  
  def __init__(self,MODULE_NAME,conf=None):
    self.NAME=MODULE_NAME
    newConf = { MODULE_NAME : DEFAULT_CONFIG}
    MongoConnect.__init__(self,MODULE_NAME,newConf)
    self.load_conf(conf)

  def process(self,mail):
    coll = self.coll
    hdr = mail['data']['Header-Fields']
    log.debug (hdr)
    entry = coll.find_one( { 'user' : hdr['From'] } )
    log.debug (entry)

    if not entry: #already in collection, update stats
      entry = {}
      entry['user'] = hdr['From']
      entry['first_post'] = hdr['Date']
      entry['last_post'] = hdr['Date']
      entry['mails'] = [mail['data']]
      entry['mailcount'] = 1
      log.debug('adding new %s' % repr(entry))
    else :
      entry['last_post'] = hdr['Date']
      entry['mails'].append(mail['data'])
      entry['mailcount'] += 1
      log.debug('updating %s' % repr(entry))

    coll.save(entry)
    del entry["_id"]
    del entry["mails"]
    return entry

  def populate_parser(self,parser): 
    MongoConnect.populate_parser(self,parser)
    #placeholder for parser options

  def eval_parser(self,parsed): 
    MongoConnect.eval_parser(self,parsed)
    #placeholder for parser evaluator

