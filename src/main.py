#!/usr/bin/env python2
import argparse,sys,json
from mail_proc import mail_proc
import logging
import genericore as gen
log = logging.getLogger('proc_mail')
PROTO_VERSION = 1
DESCRIPTION = 'performes statistical analysis against mails from stream'


# set up instances of needed modules
conf = gen.Configurator(PROTO_VERSION,DESCRIPTION)  
amqp = gen.auto_amqp()   
s = mail_proc()       # the magic mail parsing class

conf.configure([amqp,s]) #set up parser and eval parsed stuff

# start network connections
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
