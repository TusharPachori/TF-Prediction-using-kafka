from kafka import KafkaConsumer, TopicPartition
from json import loads
import cv2
import base64
import numpy as np

TOPIC = "test23"
PARTITION_0 = 0
PARTITION_1 = 1

# consumer_0 = KafkaConsumer(
#     TOPIC, group_id='my-group-2', bootstrap_servers=['10.50.23.120:9092']
# )
# consumer_1 = KafkaConsumer(
#     TOPIC, group_id='my-group-2', bootstrap_servers=['10.50.23.120:9092']
# )


consumer = KafkaConsumer(
    # 'test22',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     group_id='my-group-2',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

topic_partition_0 = TopicPartition(TOPIC, PARTITION_0)
topic_partition_1 = TopicPartition(TOPIC, PARTITION_1)
# format: topic, partition
consumer.assign([topic_partition_0])

# fourcc = cv2.VideoWriter_fourcc('m','p','4', 'v')
# writer = cv2.VideoWriter("/Users/shubhanktyagi/Desktop/out.mp4" , fourcc,30, (1280,720))

for message in consumer:
    # if "data" in message.value:
    #     writer.release()
    #     break
    # jpg_original = base64.b64decode(message.value['frame'])
    # jpg_as_np = np.frombuffer(jpg_original, dtype=np.uint8)
    # img = cv2.imdecode(jpg_as_np, flags=1)
    # cv2.imshow("video", img)
    # writer.write(img)
    print('{}'.format(message.value["frame"]))


# from kafka import KafkaConsumer
# from kafka.structs import TopicPartition

# topic = 'test22'
# bootstrap_servers = 'localhost:9092'
# consumer = KafkaConsumer(
#     topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

# partitions = consumer.partitions_for_topic(topic)
# print(partitions)