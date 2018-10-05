import sys
import pika
import json


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')

#document
#json_file2

f = open("D:\RabbitMQ\json_file2.json", "r")
json_data = json.dumps(json.load(f))
#print(json_data)
#print(type(json_data))
channel.basic_publish(exchange='',
                          routing_key='hello',
                          body=json_data)
print(" [x] Sent %r" % json_data)
connection.close()