import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from json import loads
import pandas as pd
import matplotlib.pyplot as plt


import mediapipe as mp
import threading

from configparser import ConfigParser

import os
import config
import spark_mysql

import vgg16_pretrained

import test_model

#spark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import config
from pyspark.sql.types import BinaryType
from configparser import ConfigParser
import json


# import findspark
from pyspark.shell import spark
import sys
print(sys.path)
from ultralytics.utils import ASSETS
from ultralytics.models.yolo.detect import DetectionPredictor
from ultralytics import YOLO




os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql:mysql-connector-java:8.0.23 pyspark-shell'


KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"

# Loading Kafka Cluster/Server details from configuration file(datamaking_app.conf)

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
kafka_topic_name = config_obj.get('kafka', 'output_topic_name')

kafka_input_topic_name = config_obj.get('kafka', 'input_topic_name')###???

KAFKA_TOPIC_NAME_CONS = kafka_topic_name
KAFKA_BOOTSTRAP_SERVERS_CONS = kafka_host_name + ':' + kafka_port_no





if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .appName("bla")
             # .config("spark.network.timeout", "600s")
             # .config("packages", "mysql:mysql-connector-java:8.0.23")
             .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.23")
             .getOrCreate())
    # input_shape = (160, 160, 3)
    # train_csv = pd.read_csv('C:/DoAnBigData/input_data/Human Action Recognition/Training_set.csv')
    # test_csv = pd.read_csv('C:/DoAnBigData/input_data/Human Action Recognition/Testing_set.csv')
    #
    # classes = train_csv.label.value_counts()
    # num_classes = len(classes)
    #
    # filename = train_csv.iloc[:, 0]
    # label = train_csv.iloc[:, 1]
    #
    # filename = train_csv.iloc[:, 0]
    # label = train_csv.iloc[:, 1]
    #
    # test_filename = test_csv.iloc[:, 0]
    # #########################################
    #
    # # folder ảnh
    # folder_path = 'C:/DoAnBigData/input_data/Human Action Recognition/train/'
    # resized_images = read_and_resize_blur_images(folder_path)
    #
    # img_data = []
    # img_label = []
    # test_img_data = []
    #
    # for i in range(len(resized_images)):
    #     img_data.append(np.asarray(resized_images[i]))
    #     img_label.append(label[i])
    # img_data_arr = np.array(img_data)
    # img_label_arr = np.array(img_label)

    results = [] # chứa kết quả của yolo sau khi detect và nhãn
    print("Kafka Consumer")
    try:
        # print("blaaaaaaaaaaaaaaaaaaaaaa")
        # auto_offset_reset='latest'
        # auto_offset_reset='earliest'
        topic_name = "DoAn"
        consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[config.kafka_ip],
        auto_offset_reset='latest',
        enable_auto_commit=True, # 4/3/2023
        # group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
        #value_deserializer=lambda x: loads(x.decode('utf-8')))
        # value_deserializer=lambda x: x.decode('utf-8'),
        fetch_max_bytes = 9000000, # 4/3/2023
        fetch_max_wait_ms = 10000) # thgian chờ nếu ko có data mới gửi qa thì ngắt
        # print("blaaaaaaaa222222222")

        topic_name_out = "output_topic"
        # producer = KafkaProducer(bootstrap_servers=topic_name_out)
        # print("bla3333333333333")
        for message in consumer:
            print("bat dau")
            kafka_prod_mess = message.value # luc dau ra dung cai buffer.tobyte ben producer #xài cái hexjson ra b'\x89PNG\r\n\x1a\n\x00\x00\x.../// chuỗi byte b'{time: ..., image:...}' ra kq b'{"time": "2024-01-18 19:09:02", "image": "89504e470d0a1a0a0000000d4948445200000140000000f008020000..."}'
            # stream_new = json.loads(kafka_prod_mess.decode('utf-8'))
            # print("doc messs", stream)
            decoded = kafka_prod_mess.decode('utf-8') # decode ok ra {time: ..., image:...} (chuỗi json)
            json_data = json.loads(decoded) # chuỗi json sang từ điển

            # print("qa b1", json_data)
            time_value = json_data['time']
            image_value = json_data['image']

            # print("buoc time", time_value)
            # stream = message.value
            # iiiiii = decode['time']
            # print('decode',decode)
            # print('value image', image_value)
            print("qa b2")
            image_bytes = bytes.fromhex(image_value) # chuyển hệ 16 sang byte
            stream = np.frombuffer(image_bytes, dtype=np.uint8) #từ cái byte ở trên chuyển thành mảng byte ##########??????????????????? stream =[123  34 116 ...  50  34 125]
            print("b3", stream) # ra mảng 1 chiều chứa dữ liệu byte
            image = cv2.imdecode(stream, cv2.IMREAD_COLOR) #từ mảng byte giải mã ra ảnh  # image.shape ra (240, 320, 3)
            print("b4", image.shape)
            # time = message['time']


            if image is not None:
                # print("Đã load frame")
                # print("Image shape:", image.shape)
                # print("Xử lý hình ảnh")

                # my_model = vgg16_pretrained.vgg16_train_create(input_shape, num_classes)
                image = cv2.resize(image, dsize=(256, 256))
                # image = np.expand_dims(image, axis=0)

                #predict
                # predict = my_model.predict(image)  # của vgg16 cũ

                #yolo
                # args = dict(model='C:/DoAnBigData/resnet50_yolov8_load_model/yolov8n.pt',
                #             source=image,
                #             classes=0
                #             )
                # predictor = DetectionPredictor(overrides=args)
                #
                # predictor.predict_cli()
                model = YOLO('yolov8n-pose.pt')

                predict = model.predict(source=image, save=True, classes=0, show=True)

                for result in predict:
                    print('nhãn sau khi dùng yolo', result.label_detect)
                    # results.append(result)

                    print("Spark lưu vào MySQL")
                    image_binary = bytearray(cv2.imencode('.jpg', image)[1]) #chuyển sang mảng byte để đưa vào mysql, [1] là vì cv2.imencode trả về 2 gtri 1 là chứa byte ảnh 2 là biến bool cho bít có chuyển đc ko
                    print('image shapeeeeeeeee', image.shape)
                    print("Chuyển ảnh sang byte")
                    # df = spark.createDataFrame([(image_binary,)], ["image"])
                    print('result.label_detecttttttttt', result.label_detect)
                    if result.label_detect == None:
                        df = spark.createDataFrame([(image_binary, time_value, 'no person')],
                                                   ["image", "image_time", "label"])

                        print("Bắt đầu lưu")
                        df_finish = (df.write.format("jdbc")
                                     .option("driver", "com.mysql.cj.jdbc.Driver")
                                     .option("url", "jdbc:mysql://localhost:3306/doanbigdata_db")
                                     .option("dbtable", "images")
                                     .option("user", "root")
                                     .option("password", "7153")
                                     .option("numPartitions", 1)
                                     .option("fetchsize", 20)
                                     .mode("append")
                                     .save())

                    else:
                        for i in result.label_detect:
                            if i == 'walking' or i == 'running' or i == 'jogging' or i == 'handwaving' or i == 'handclapping' or i == 'boxing':
                                df = spark.createDataFrame([(image_binary, time_value,
                                                             i,
                                                             # result.label_detect
                                                             )]
                                                           , ["image", "image_time", i])

                                print("Bắt đầu lưu")
                                df_finish = (df.write.format("jdbc")
                                             .option("driver", "com.mysql.cj.jdbc.Driver")
                                             .option("url", "jdbc:mysql://localhost:3306/doanbigdata_db")
                                             .option("dbtable", "images")
                                             .option("user", "root")
                                             .option("password", "7153")
                                             .option("numPartitions", 1)
                                             .option("fetchsize", 20)
                                             .mode("append")
                                             .save())
                    # spark_mysql.image_processing_mysql(image)
                    print("Đã lưu vào db")

                ######################################
                print(f"Thời gian: {time_value}")
                # if img_label_arr[np.argmax(predict[0])] == 'sitting':
                #     print("Kết quả: ", img_label_arr[np.argmax(predict[0])]) #hiện tên của label có xác suất cao nhất
                #     print("Xác suất của các nhãn", (predict[0])) #xác suất của tất cả các label
                #     print(np.max(predict[0], axis=0)) #hiện gtri lớn nhất trong các tất cả label
                #
                #     print("cái này là sitting")
                #     print(f"Thời gian: {time_value}")
                #
                #     # print("kiểu data", image.dtype)
                #     # print("kích thước", image.shape)
                #     # image = np.asarray(image)
                #     image = image.reshape((160,160,3))
                #     # print("kích thước sau khi chuyển", image.shape)
                #     # print(image)
                #     # plt.imshow(image)
                #     # plt.show()
                #     # image = np.array(image)
                #
                #     print("Xử lý Spark lưu vào MySQL")
                #     image_binary = bytearray(cv2.imencode('.jpg', image)[1])
                #     print("Chuyển ảnh sang byte")
                #     # df = spark.createDataFrame([(image_binary,)], ["image"])
                #     df = spark.createDataFrame([(image_binary, time_value)], ["image", "image_time"])
                #
                #     print("Bắt đầu lưu")
                #     df_finish = (df.write.format("jdbc")
                #                  .option("driver", "com.mysql.cj.jdbc.Driver")
                #                  .option("url", "jdbc:mysql://localhost:3306/doanbigdata_db")
                #                  .option("dbtable", "images")
                #                  .option("user", "root")
                #                  .option("password", "7153")
                #                  .option("numPartitions", 5)
                #                  .option("fetchsize", 20)
                #                  .mode("append")
                #                  .save())
                #     # spark_mysql.image_processing_mysql(image)
                #     print("Đã lưu vào db")

                #cái dưới nên xài ko ??????????
                # if (np.max(predict) >= 0.8) and (np.argmax(predict[0]) != 0): # kt xác suất lớn nhất trong vector predict nhãn nếu ko qa 0.8 và label dự đoán j đấy thì đéo chạy và
                    # đống này viết label predicted lên cam (đéo cần lắm)
                    # font = cv2.FONT_HERSHEY_SIMPLEX
                    # org = (50, 50)
                    # fontScale = 1.5
                    # color = (0, 255, 0)
                    # thickness = 2
                    #
                    # cv2.putText(image, img_label_arr[np.argmax(predict)], org, font,
                    #             fontScale, color, thickness, cv2.LINE_AA)

                # cv2.imshow("Picture", image) # lỗi ko đọc đc đồ họa bằng cv2 (code gốc xài cái này)
                # plt.imshow(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
                # plt.show()

                # cv2.putText(image, "PROCESSED", (10, 10), cv2.FONT_HERSHEY_COMPLEX, 2, (0, 255, 0), 2) #xài cái này để vẽ lên frame lưu vào db
                # cv2.imwrite("C:/DoAnBigData/kafka_producer_consumer/daxyly.png", image)
                # print("OKKKKKK")
            # print("frame không phải sleeping đã được xử lý")
            print("_____________________________________________________________________________________________________")
            # ret, buffer = cv2.imencode('.jpg', image)
            # consumer.send(topic_name_out, buffer.tobytes())
            # consumer.flush()


            # #print(dir(message))
            # print(type(message))
            # print("Key: ", message.key)
            # message = message.value
            # print("Message received: ", message)
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)