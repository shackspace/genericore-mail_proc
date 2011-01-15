#!/usr/bin/python2
from pymongo import Connection
import logging
log = logging.getLogger('gen_stats')

class StatsForUser:
  collection = 'mail_user_stats'

  def connect_mongo(self):
    self.conn = Connection(self.mongo_host)
    self.db = self.conn[self.collection]

  def __init__(self,host='localhost'):
    self.mongo_host=host
    self.connect_mongo()

  def process_mail(self,mail):
    db = self.db
    hdr = mail['data']['Header-Fields']
    log.debug (hdr)
    entry = db.users.find_one( { 'user' : hdr['From'] } )
    log.debug (entry)
    if not entry:
      entry = {}
      #already in db, update stats
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
