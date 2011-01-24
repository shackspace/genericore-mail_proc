#!/usr/bin/python2
from pymongo import Connection
from genericore import Configurable
import logging, sys
log = logging.getLogger('gen_stats')
DEFAULT_CONFIG = {
  "mail_proc" : {
    "mongdb" : {
      "host" : "localhost"
    },
    "collection" : "mail_user_stats"
  }
}

class StatsForUser(Configurable):
  
  def connect_mongo(self):
    try:
      conf = self.config['mail_proc']
      self.conn = Connection(**conf['mongdb'])
      self.db = self.conn[conf['collection']]
      self.db.users.save({})
    except Exception as e:
      log.error('Mongodb not running or unreachable ! Bailing out' + str(e))
      sys.exit(0)

  def __init__(self,conf=None):
    Configurable.__init__(self,DEFAULT_CONFIG)
    self.load_conf(conf)

  def create_connection(self):
    self.connect_mongo()
  def process_mail(self,mail):
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

  def close_mongo(self):
    self.conn.close()
