from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import csv

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
import cv2
import mediapipe as mp
import numpy as np
import matplotlib.pyplot as plt

import threading


import json


import config

import datetime
import binascii



# pip install kafka-python
# pip3 install kafka-python

from configparser import ConfigParser

import main

# import logging
# logging.basicConfig(level=logging.DEBUG)

# Loading Kafka Cluster/Server details from configuration file(datamaking_app.conf)
#############################################################################################################
conf_file_name = "C:\DoAnBigData\datamaking_app.conf"
config_obj = ConfigParser()
print(config_obj)
print(config_obj.sections())
config_read_obj = config_obj.read(conf_file_name)
print(type(config_read_obj))
print(config_read_obj)
print(config_obj.sections())

# Kafka Cluster/Server Details
kafka_host_name = config_obj.get('kafka', 'host')
kafka_port_no = config_obj.get('kafka', 'port_no')
kafka_topic_name = config_obj.get('kafka', 'input_topic_name')

KAFKA_TOPIC_NAME_CONS = kafka_topic_name
KAFKA_BOOTSTRAP_SERVERS_CONS = kafka_host_name + ':' + kafka_port_no

# # {"order_id": 101, "order_date": "2021-02-06 07:55:00", "order_amount": 100}
# kafka_topic_name = "test-topic"
# kafka_bootstrap_servers = 'localhost:9092'
#
# if __name__ == "__main__":
#
#
#     print("Bắt đầu")
#     print(time.strftime("%Y-%m-%d %H:%M:%S"))
#
#     # spark.sparkContext.setLogLevel("ERROR")
#
#     kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
#                                        api_version=(0, 11, 5),
#                                        value_serializer=lambda x: dumps(x).encode('utf-8'),
#                                        request_timeout_ms=1200000
#                                        )
#
#     with open("C:/DoAnBigData/input_data/bank_transactions.csv", "r") as csvfile:
#         # Đọc từng dòng trong file CSV
#         csv_reader = csv.reader(csvfile)
#         header = next(csv_reader)
#         print("Header: ", header)
#         for row in csv_reader:
#             # Gửi dữ liệu vào Kafka
#             event_datetime = datetime.now()
#
#             # Make sure the index is adjusted based on the actual position of "Time" column
#             time_index = 9  # Assuming "Time" is the second column (index 1)
#
#             time_value = row[time_index]
#             row[time_index] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
#             row.append(row[time_index])
#
#             print("In ra: ", row)
#
#             kafka_producer_obj.send("input_topic_name", row)
#             time.sleep(1)
#
#     # Flush dữ liệu
#     kafka_producer_obj.flush()
#
#     print('Đọc ok')
#     print("Schema")
#######################################################################################################################


# path_haar = 'C:\DoAnBigData\haar\haarcascade_frontalcatface.xml'
# face_cascade = cv2.CascadeClassifier(path_haar)

topic_name = "DoAn"
kafka_producer_obj = KafkaProducer(bootstrap_servers=[config.kafka_ip],
                                       # api_version=(0, 11, 5),
                                       # value_serializer=lambda x: dumps(x).encode('utf-8'),
                                       # request_timeout_ms=1200000
                                        max_request_size = 9000000
                                       )
path_video = 'input_data/1067069527-preview.mp4'
# path_video = 'data_KTH/handwaving/person01_handwaving_d4_uncomp.avi'
cam = cv2.VideoCapture(path_video)

i = 0
warmup_frames = 60
label = "Warmup...."
n_time_steps = 10

frame_count = 0
while True:
    ret, frame = cam.read()
    if ret:
        frame_count += 1
        if frame_count == 1 or frame_count % 5 == 0:
            frame = cv2.resize(frame, dsize=None, fx=0.5, fy=0.5)
            image = frame.copy()
            image = cv2.resize(frame, dsize=(256, 256))
            image = np.expand_dims(image, axis=0)

            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        # success, img = cam.read()
        # imgRGB = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        # results = pose.process(imgRGB)
            i = i + 1
        # if i > warmup_frames:
        #     print("Start detect....")
        #
        #
        #     # if results.pose_landmarks:
        #     #     c_lm = make_landmark_timestep(results)
        #     #
        #     #     lm_list.append(c_lm)
        #     #     if len(lm_list) == n_time_steps:
        #     #         # predict
        #     #         t1 = threading.Thread(target=detect, args=(model, lm_list,))
        #     #         t1.start()
        #     #         lm_list = []
        #     #
        #     #     img = draw_landmark_on_image(mpDraw, results, img)
        #
        # # img = draw_class_on_image(label, img)
            cv2.imshow("Image", frame)
        # plt.imshow(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        # plt.show()


        # frame = cv2.resize(frame, dsize=None, fx=0.2, fy=0.2)
        # predict = ge
        
            ret, buffer = cv2.imencode('.png', frame) # chuyển frame sang png #buffer là ảnh đã chuyển sang png
        # payload = {
        #     'timestamp': current_time,
        #     'frame': frame.tobytes()  # chuyển frame thành bytes để gửi đi
        # }
            hex_data = binascii.hexlify(buffer.tobytes()).decode('utf-8') # chuyển sang hệ 16 rồi giải ra utf8

            message = {}
            message['time'] = current_time
            message['image'] = hex_data

            keys_to_send = ['time', 'image']
            filtered_mess = {key: message[key] for key in keys_to_send} # nếu key có trong list thì thêm cả key và value vào filterred_mess
            kafka_producer_obj.send(topic_name,
                                json.dumps(filtered_mess).encode('utf-8') #từ utf8 đưa về byte      #ra  b'{"time": "2024-01-18 20:48:27", "image": "89504e470d0a1a0a0000000d494..."}'
                                # json.dumps(message).encode('utf-8') # ra b'89504e470d0a1a0a0000000d4948445...'
                                # buffer.tobytes()   # kq ra b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x01@\x00\x00\x00\
                                )
            kafka_producer_obj.flush() # xác nhận mess đã gửi
            print(f"Da gui {current_time}")
        # print("blabla", json.dumps(filtered_mess).encode('utf-8'))
            time.sleep(1)
        if cv2.waitKey(1) == ord('q'):
            break
        ###
cam.release()
cv2.destroyAllWindows()
#########################################


#################################################gốc kafka_producer
# while True:
#     ret, frame = cam.read()
#     if ret:
#         # print("OK")
#         # gray = cv2.cvtColor(frame, cv2.COLOR_BGRA2GRAY)
#         # faces = face_cascade.detectMultiScale(gray)
#         # for(x,y,w,h) in faces:
#         #     cv2.rectangle(frame, (x,y), (x+w, y+h), (0, 255, 0), 2)
#         # cv2.imshow("Detecting face", frame)
#         # if(cv2.waitKey(1) & 0xFF == ord('q')):
#         #     break
#         frame = cv2.resize(frame, dsize=None, fx=0.2, fy=0.2)
#         ret, buffer = cv2.imencode('.png', frame)
#         kafka_producer_obj.send(topic_name,
#                                  buffer.tobytes()
#                                 )
#         kafka_producer_obj.flush()
#         print("Da gui")
#         time.sleep(5)
# cam.release()



