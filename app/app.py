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

producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


def put_policy_on_kafka(policy):
  producer.send("application-logs", policy)

@app.route('/processing', methods=['POST'])
def check_processing():
  raw_policy = json.loads(flask.request.data)
  event_id = str(uuid.uuid4())
  policy = {
    "timestamp": str(current_milli_time()),
    "process": raw_policy["process"],
    "purpose": raw_policy["purpose"],
    "processing": raw_policy["processing"],
    "recipient": raw_policy["recipient"],
    "storage": raw_policy["storage"],
    "userID": raw_policy["userID"],
    "data": raw_policy["data"],
    "eventID": event_id
  }

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
    print(str(message))
    if message["eventID"] == event_id:
      print("[>] RESPONSE FOUND: ")
      print(str(message))
      if not message["hasConsent"]:
        return Response("Processing for the request with policy: " + str(raw_policy) + " has been denied", 401, {})
      else:
        return Response("Processing for the request with policy: " + str(raw_policy) + " has been approved", 204, {})
      break

  return Response("Compliance check for processing for the request with policy: " + str(raw_policy) + " is not found", 500, {})
