#!/usr/bin/python3

import sys
import os
import json
import socket
import time
from datetime import datetime
import flask
from flask import request
import paho.mqtt.publish as publish

app = flask.Flask(__name__)
app.config["DEBUG"] = False


def getenv():
  return {
    'mqtt_addr': os.environ.get('MQTT_ADDR', '127.0.0.1'),
    'mqtt_port': os.environ.get('MQTT_PORT', 1883),
    'mqtt_user': os.environ.get('MQTT_USER', None),
    'mqtt_pass': os.environ.get('MQTT_PASS', None),
    'mqtt_qos':  os.environ.get('MQTT_QOS', int(1)),
    'mqtt_topic_prefix': os.environ.get('MQTT_TOPIC_PREFIX', 'ddns-test/'),
    'tokenfile': os.environ.get('TOKENFILE', os.path.join( os.path.dirname(os.path.realpath(__file__)) , 'tokenfile.json') )
  }


def msg(text):
    """
    Function that prints timestamped messages on stdout
    :param text: Message body
    :return: It does not need to return anything
    """
    print("%s : %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), text))
  

@app.route('/update', methods=['POST'])
def update():
  global cfg
  global tokens

  req = request.get_json()

  try:
    token = req.get('token', None)
    hostn = req.get('hostname', None)
  except:
    return "Missing parameters", 400

  if request.headers.getlist("X-Forwarded-For"):
    addr = request.headers.getlist("X-Forwarded-For")[-1]
  else:
    addr = req.get('ip', request.remote_addr)

  if token is None or hostn is None:
    return "Invalid token", 403

  entitled_hostn = tokens.get(token, None)
  if entitled_hostn is None or hostn not in entitled_hostn:
    return "Not entitled", 403

  try:
    socket.inet_aton(addr)
  except:
    return "Invalid IP", 400

  topic = cfg['mqtt_topic_prefix'] + str(hostn)
  payload = "{\"hostn\":\"%s\", \"addr\":\"%s\"}" % (hostn, addr)

  auth=None
  if cfg['mqtt_user'] is not None:
    auth = {'username': cfg['mqtt_user'], 'password': cfg['mqtt_pass'] }


  try:
    publish.single(topic, hostname=cfg['mqtt_addr'], port=cfg['mqtt_port'], auth=auth, payload=payload, qos=cfg['mqtt_qos'], keepalive=15, retain=True )

  except Exception as e:
    msg("Unable to publish update %s -> %s on %s broker: %s" % (addr, topic, cfg['mqtt_addr'], e))
    return "Message publish error", 503

  else:
    msg("Update published %s -> %s on %s broker" % (addr, topic, cfg['mqtt_addr']))
    return "Update published", 201

cfg = getenv()

try:
  f = open(cfg['tokenfile'])
  tokens = json.load(f)
except:
  print("Unable to load tokenfile!")
  sys.exit(-1)

#if __name__ == "__main__":
#  app.run(host='0.0.0.0',port=8080)

