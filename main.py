#!/usr/bin/env python2
import pika
import json,argparse
from mail_proc import StatsForUser
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('proc_mail')

SRC_QUEUE='mail_proc'
SRC_EXCHANGE='mail_src'
PROC_EXCHANGE='mail_proc'

parser = argparse.ArgumentParser(description='generates dummy package on given exchange against AMQP')
parser.add_argument('--host',default='141.31.8.11',      help='AMQP host ip address')
parser.add_argument('--port',type=int,default=5672,      help='AMQP host port')
parser.add_argument('-u','--username',default='guest',   help='AMQP username') 
parser.add_argument('-p','--password',default='guest',   help='AMQP password') 
args = parser.parse_args()

connection = pika.AsyncoreConnection(pika.ConnectionParameters(
          credentials = pika.PlainCredentials(args.username,args.password), 
          host=args.host,port=args.port))
channel = connection.channel()

channel.exchange_declare(exchange=SRC_EXCHANGE,
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
