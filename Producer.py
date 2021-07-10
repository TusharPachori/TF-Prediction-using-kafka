from time import sleep
from json import dumps
from kafka import KafkaProducer
import cv2
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

webcam = cv2.VideoCapture("/Users/tusharpachori/Downloads/input.mp4")

while(webcam.isOpened()):
    _, frame = webcam.read()
    if(_ == True):
        data = {'frame' : frame.tolist()}
        producer.send('test1', value=data)
    else:
        break

webcam.release()