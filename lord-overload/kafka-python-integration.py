import os
import csv

from json import dumps
from time import sleep
from kafka import KafkaProducer

os.chdir("C:/Users/AYUSH/Desktop/Ronnie/kafka_2.12-2.1.0/lord-overload")

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

with open("Sensor.csv", 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data = {'number' : row}
        producer.send('lord-overload', value=data)
        sleep(5)
        #print(dict(row))

csvfile.close()
