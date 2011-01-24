#!/usr/bin/env python2
import argparse,sys,json
from mail_proc import StatsForUser
import logging
logging.basicConfig(level=logging.INFO)
import genericore as gen
log = logging.getLogger('proc_mail')
PROTO_VERSION = 1

conf = gen.Configurator(PROTO_VERSION)
amqp = gen.auto_amqp() 
s = StatsForUser()

parser = argparse.ArgumentParser(description='performes statistical analysis against mails from stream ')
amqp.populate_parser(parser)
conf.populate_parser(parser)
args = parser.parse_args()

conf.eval_parser(args)
amqp.eval_parser(args)

conf.configure([amqp,s])
conf.blend([amqp,s])
print str(conf.config)
s.create_connection()
amqp.create_connection()
# main method
def cb (ch,method,header,body):
  log.debug ( "Header %r" % (header,))
  log.debug ( "Body %r" % (body,))
  try:
    entry = s.process_mail(json.loads(body))
    log.info ( "Message to send:\n%r" % (entry,))
    e = json.dumps(entry)
    print e
    amqp.publish(json.dumps(entry))
    log.info ('finished dumping')
  except Exception as e:
    print 'Something just fuckin happened' + str(e)

amqp.consume(cb)
print "waiting for messages"
amqp.start_loop()
