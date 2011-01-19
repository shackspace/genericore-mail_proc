#!/usr/bin/env python2
import json,argparse,sys
from mail_proc import StatsForUser
import logging
import genericore as gen
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('proc_mail')
PROTO_VERSION = 1

parser = argparse.ArgumentParser(description='performes statistical analysis against mails from stream ')
gen.parse_default(parser)
args = parser.parse_args()
if args.unique_key:
  print (gen.generate_unique())
  sys.exit(0)

amqp = gen.auto_amqp()
amqp.load_conf_file("conf.json")

amqp.create_connection()
print dir(amqp)
s = StatsForUser()

def cb (ch,method,header,body):
  log.info('Received Message in queue %s from exchange %s' )
  log.debug ( "Header %r" % (header,))
  log.debug ( "Body %r" % (body,))
  entry = s.process_mail(json.loads(body))
  log.debug ( "Message to send:\n%r" % (entry,))
  amqp.publish(json.dumps(entry))

amqp.consume(cb)
print "waiting for messages"
amqp.start_loop()

