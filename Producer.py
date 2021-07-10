from json import dumps
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewPartitions, NewTopic
import cv2
import json
import base64
from time import sleep


topic = 'test23'
bootstrap_servers = 'localhost:9092'

admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
topic_list = []
topic_list.append(NewTopic(name=topic, num_partitions=2, replication_factor=1))
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except:
    print()
# topic_partitions = {}
# topic_partitions[topic] = NewPartitions(total_count=2)
# admin_client.create_partitions(topic_partitions)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# webcam = cv2.VideoCapture("/Users/shubhanktyagi/Downloads/open_drill_11.mp4")
# webcam.set(cv2.CAP_PROP_FPS, 1)

# while(webcam.isOpened()):
ctr = 0
while(True):
    print(ctr)
    producer.send(topic, value={"frame": ctr})
    sleep(0.3)
    ctr+=1
    # _, frame = webcam.read()
    # if(_ == True):
    #     jpg_as_text = base64.b64encode(cv2.imencode('.jpg', frame)[1]).decode()
    #     data = {'frame' : jpg_as_text}
    #     producer.send('test11', value=data)
    #     # cv2.imshow("video", frame)
    # else:
    #     producer.send('test11', value={'data': "STOP"})
    #     break

# webcam.release()