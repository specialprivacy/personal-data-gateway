import flask
import requests
import os
import json
import time
import uuid
from kafka import KafkaProducer
from kafka import KafkaConsumer
from flask import Response
from json import dumps
from json import loads

app = flask.Flask(__name__)

current_milli_time = lambda: int(round(time.time() * 1000))

def init_producer():
  return KafkaProducer(bootstrap_servers=['kafka:9092'],
                           value_serializer=lambda x:
                           dumps(x).encode('utf-8'))

def put_policy_on_kafka(policy):
    producer = init_producer()
    producer.send("application-logs", policy)
    producer.close()

@app.route('/processing', methods=['POST'])
def check_processing():
  policy = json.loads(flask.request.data)
  event_id = str(uuid.uuid4())
  policy["timestamp"] = str(current_milli_time())
  policy["eventID"] = event_id

  print("[*] PUTTING POLICY ON KAFKA " + str(policy))
  put_policy_on_kafka(policy)

  print("[>] STARTING READ LOOP")
  consumer = KafkaConsumer(
    "checked-application-logs",
     bootstrap_servers=['kafka:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id=str(uuid.uuid4()),
     value_deserializer=lambda x: loads(x.decode('utf-8')))

  for message in consumer:
    message = message.value
    if message["eventID"] == event_id:
      consumer.close()
      print("[>] RESPONSE FOUND: ")
      print(str(message))
      if not message["hasConsent"]:
        return Response("Processing for the request with policy: " + str(policy) + " has been denied", 401, {})
      else:
        return Response("Processing for the request with policy: " + str(policy) + " has been approved", 204, {})

  return Response("Compliance check for processing for the request with policy: " + str(policy) + " is not found", 500, {})
