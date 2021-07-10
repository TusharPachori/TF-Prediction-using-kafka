from kafka import KafkaConsumer
from json import loads
import cv2
import base64
import numpy as np


consumer = KafkaConsumer(
    'test10',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=False,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    jpg_original = base64.b64decode(message.value['frame'])
    jpg_as_np = np.frombuffer(jpg_original, dtype=np.uint8)
    img = cv2.imdecode(jpg_as_np, flags=1)
    cv2.imshow("video", img)
    # print('{}'.format(message.value[frame]))
