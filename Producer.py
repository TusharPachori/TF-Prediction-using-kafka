from json import dumps
from kafka import KafkaProducer
import cv2
import json
import base64


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

webcam = cv2.VideoCapture("/Users/tusharpachori/Downloads/input.mp4")
webcam.set(cv2.CAP_PROP_FPS, 1)

while(webcam.isOpened()):
    _, frame = webcam.read()
    if(_ == True):
        jpg_as_text = base64.b64encode(cv2.imencode('.jpg', frame)[1]).decode()
        data = {'frame' : jpg_as_text}
        producer.send('test10', value=data)
        cv2.imshow("video", frame)
    else:
        break

webcam.release()