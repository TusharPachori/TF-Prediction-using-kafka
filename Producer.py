#!/Users/shubhanktyagi/opt/anaconda3/envs/cvrms/bin/python
import argparse
from json import dumps
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewPartitions, NewTopic
import cv2
import json
import base64
import time

def get_args():
    parser = argparse.ArgumentParser(description='PyTorch Detection Training', add_help=True)
    parser.add_argument('--input-video', default='/Users/shubhanktyagi/Downloads/open_drill_11.mp4', help='input video')
    parser.add_argument('--topic', default='test100', help='topic')
    return parser

if __name__ == "__main__":
    args = get_args().parse_args()
    topic = args.topic
    bootstrap_servers = 'localhost:9092'

    # admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    # topic_list = []
    # topic_list.append(NewTopic(name=topic, num_partitions=2, replication_factor=1))
    # try:
    #     admin_client.create_topics(new_topics=topic_list, validate_only=False)
    # except:
    #     print()

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))

    webcam = cv2.VideoCapture(args.input_video)
    webcam.set(cv2.CAP_PROP_FPS, 1)
    ctr = 1

    while(webcam.isOpened()):
    # while(True):
        _, frame = webcam.read()
        if(_ == True):
            jpg_as_text = base64.b64encode(cv2.imencode('.jpg', frame)[1]).decode()
            data = {"count": ctr, "frame" : jpg_as_text}
            ctr += 1
            producer.send(topic, value=data)
            # cv2.imshow("video", frame)
        else:
            print("send")
            producer.send(topic, value={"count": -1, "frame": ""})
            # producer.send(topic, value={"count": -2, "frame": ""})
            webcam.release()
            break
