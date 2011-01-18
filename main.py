#!/usr/bin/env python2
import pika
import json,argparse,hashlib,sys
from mail_proc import StatsForUser
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('proc_mail')

SRC_QUEUE='mail_proc'
SRC_EXCHANGE='mailsrc'
PROC_EXCHANGE='mail_proc'
PROTO_VERSION='1'
parser = argparse.ArgumentParser(description='performes statistical analysis against mails from stream ')
parser.add_argument('--host',default='141.31.8.11',      help='AMQP host ip address')
parser.add_argument('--port',type=int,default=5672,      help='AMQP host port')
parser.add_argument('-u','--username',default='guest',   help='AMQP username') 
parser.add_argument('-p','--password',default='guest',   help='AMQP password') 
parser.add_argument('--unique-key',action='store_true',   help='Unique Key') 
args = parser.parse_args()
if args.unique_key:
  print hashlib.sha1(PROTO_VERSION+args.host+str(args.port)).hexdigest()
  sys.exit(0)

connection = pika.AsyncoreConnection(pika.ConnectionParameters(
          credentials = pika.PlainCredentials(args.username,args.password), 
          host=args.host,port=args.port))
channel = connection.channel()

channel.exchange_declare(exchange=SRC_EXCHANGE,
#                             durable=False, auto_delete=True,
                             type='fanout')


channel.queue_declare(queue=SRC_QUEUE)

channel.queue_bind(exchange=SRC_EXCHANGE, queue=SRC_QUEUE)

channel.exchange_declare(exchange=PROC_EXCHANGE,
                             type='fanout')

log.info('Starting StatsForUser Module (mongodb)')
s = StatsForUser()

def callback(ch, method, header, body):
  log.info('Received Message in queue %s from exchange %s' % (SRC_QUEUE,PROC_EXCHANGE))
  log.debug ( "Header %r" % (header,))
  log.debug ( "Body %r" % (body,))
  entry = s.process_mail(json.loads(body))
  log.debug ( "Message to send:\n%r" % (entry,))
  channel.basic_publish(exchange=PROC_EXCHANGE,
      routing_key='',
      body=json.dumps(entry))

channel.basic_consume(callback,
  queue=SRC_QUEUE,
  no_ack=True)

print ' [*] Waiting for messages. To exit press CTRL+C'
pika.asyncore_loop()
