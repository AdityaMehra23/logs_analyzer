# python3 logs_analyzer/kafka_log_producer.py
# pip install kafka-python
from kafka import KafkaProducer
import json
import time

class Producer:
    broker = ""
    topic = ""
    producer = ""
    
    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries = 3)
        
    def send_msg(self, msg,rec_num):
        print(f"sending {rec_num}th message")
        try:
            future = self.producer.send(self.topic,msg)
            print(json.dumps(msg).encode('utf-8'))
            self.producer.flush()
            #future.get(timeout=5)
            # print("message sent successfully...")
            return {'status_code':200, 'error':None}
        except Exception as ex:
            return ex


broker = 'localhost:9092'
topic = 'web-access-logs'
message_producer = Producer(broker,topic)

filepath = "logs_analyzer/access_logs/logfiles.log"

# simulate real-time logs generation
with open(filepath,"r") as logs:
    for rec_num,record in enumerate(logs):
        resp = message_producer.send_msg(record,rec_num)
        if not resp:
            break
        time.sleep(5)
        
      
        


