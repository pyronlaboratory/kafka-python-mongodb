import os
import csv
from json import dumps
from time import sleep
from kafka import KafkaProducer

#Changing default working directory
os.chdir("C:/Users/AYUSH/Desktop/Ronnie/kafka_2.12-2.1.0/lord-overload")

#Creating producer object; Lamba function used to serialize the data and encode it to utf-8 format
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

#Reading csv to a dictionary and sending the data with the topic name 'lord-overload'
with open("Sensor.csv", 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data = {'number' : row}
        producer.send('lord-overload', value=data)
        sleep(5)
        #print(dict(row))

#Flushing csv file object
csvfile.close()


