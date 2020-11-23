#!/usr/bin/python3

import sys
import os
import json
import signal
import threading
import socket
import time
from datetime import datetime
import paho.mqtt.client as subscribe


def getenv():
  return {
    'mqtt_addr': os.environ.get('MQTT_ADDR', '127.0.0.1'),
    'mqtt_port': os.environ.get('MQTT_PORT', 1883),
    'mqtt_user': os.environ.get('MQTT_USER', None),
    'mqtt_pass': os.environ.get('MQTT_PASS', None),
    'mqtt_qos':  os.environ.get('MQTT_QOS', int(1)),
    'mqtt_topics': os.environ.get('MQTT_TOPIC_PREFIX', 'ddns-test/+'),
    'hostfile': os.environ.get('HOSTFILE', os.path.join( os.path.dirname(os.path.realpath(__file__)) , 'ddnshosts') )
  }


def msg(text):
    """
    Function that prints timestamped messages on stdout
    :param text: Message body
    :return: It does not need to return anything
    """
    print("%s : %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), text))


class Consumer():

  def __init__(self, cfg):
    self.cfg = cfg

    self.lock = threading.Lock()
    
    self.conn = subscribe.Client()
    self.conn.on_message = self._render_file
    self.conn.on_connect = self._on_connect
    self.conn.on_disconnect = self._on_disconnect
    self.content={}

    if self.cfg['user'] is not None:
       self.conn.username_pw_set(self.cfg['user'], password=self.cfg['pass'])

    self._connect()
    # subscribe is executed via _on_connect
    self.conn.loop_start()

  def __del__(self):
    if bool(self.conn):
      msg("Closing connection")
      self.conn.loop_stop()
      self.conn.disconnect()
      del self.conn

  def _connect(self, depth=0):
    try:
      self.conn.connect(self.cfg['addr'], port=self.cfg['port'], keepalive=15)
    except Exception as e:
      msg("Connection failed : %s" % (str(e)))
      depth += 1
      if depth <= 60:
        msg("Waiting 10 seconds before reconnecting ...")
        time.sleep(10)
        self._connect(depth)
      else:
        msg("Reconnecting was failing for too long ...")
        raise e

  def _subscribe(self):
    try:
      self.conn.subscribe(self.cfg['topics'], qos=self.cfg['qos'])
    except Exception as e:
      msg("Subscription exception : %s" % (str(e)))
      raise e

  def _on_connect(self, client, userdata, flags, rc):
    msg("Connected")
    self._subscribe()

  def _on_disconnect(self, client, userdata, rc):
    msg("Disconnected")

  def _render_file(self, client, userdata, message):

    try:
      payload = json.loads(message.payload.decode("utf-8"))
    except Exception as e:
      msg("Malformed json message : %s" % (e))
      return

    if type(payload) is not dict:
      msg("mqtt_json format expected , got %s!" % (type(payload)))
      return

    self.lock.acquire(blocking=True, timeout=-1)

    if self.content.get(payload['hostn'], False) != payload['addr']:
      msg("Update %s -> %s" % (payload['hostn'], payload['addr']))
      self.content[payload['hostn']] = payload['addr']
      try:
        f = open(self.cfg['hostfile'], 'w+')
      except Exception as e:
        msg("Error opening hostfile : %s" % (e))
      else:
        for k,v in self.content.items():
          f.write("%s\t%s\r\n" % (v, k))
        f.close()
    else:
      msg("No update is needed for %s" % payload['hostn'])

    self.lock.release()


def main():
  global cfg
  global tokens

  signal.signal(signal.SIGINT, (lambda signum, frame: None))
  signal.signal(signal.SIGTERM, (lambda signum, frame: None))
  
  msg("Start ...")
  cfg = getenv()

  c = Consumer(cfg)

  signal.pause()

  msg("Stopping now on signal ...")
  del c

  

if __name__ == "__main__":
  main()

