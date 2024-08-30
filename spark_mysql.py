import os

import cv2
import numpy as np

# from io import StringIO
# from configparser import ConfigParser

# from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import config
from pyspark.sql.types import BinaryType


# import findspark
from pyspark.shell import spark
import sys
print(sys.path)


# findspark.add_packages('mysql-connector-java-8.0.23')
# findspark.init()
# findspark.find()

from kafka import KafkaConsumer, KafkaProducer

from configparser import ConfigParser
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, when


# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
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
#########################################################################################################


# os.environ['PYSPARK_PYTHON'] = 'C:/DoAnBigData/venv/Scripts/python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/DoAnBigData/venv/Scripts/python.exe'

# spark.submit.pyFiles #********************
#spark.jars.ivy ******************
spark = (SparkSession
         .builder
         .appName("bla")
         # .config("spark.network.timeout", "600s")
         # .config("packages", "mysql:mysql-connector-java:8.0.23")
         .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.23")

         # .config("spark.jars.ivy", "C:/Users/ngsil/.ivy2/jars/mysql_mysql-connector-java-8.0.23.jar")
         #
         # .config("spark.jars.packages", "C:/Users/ngsil/.ivy2/jars/mysql_mysql-connector-java-8.0.23.jar")
         # .config("spark.jars", "C:/Users/ngsil/.ivy2/jars/mysql_mysql-connector-java-8.0.23.jar")

         # .config("spark.executor.extraClassPath", "C:/Users/ngsil/.ivy2/cache/mysql/mysql-connector-java/jars/mysql_mysql-connector-java-8.0.23.jar")
         # .config("spark.driver.extraClassPath", "C:/Users/ngsil/.ivy2/cache/mysql/mysql-connector-java/jars/mysql_mysql-connector-java-8.0.23.jarr")

         .getOrCreate())
# spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
# spark.conf.set("spark.executor.extraJavaOptions", "-Dsun.io.serialization.extendedDebugInfo=true")

spark.sparkContext.setLogLevel("ERROR")

mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"
table_name = "images"
host_name = "localhost"
port_no = "3306"
user_name = "root"
password = "7153"
database_name = "doanbigdata_db"

# my_sql_select_query = None
# my_sql_select_query = "(select * from " + table_name + ") as users"

# print("bla bla bla: ")
# print(my_sql_select_query)

mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

print("JDBC Url: " + mysql_jdbc_url)

data = 'C:/DoAnBigData/input_data/testmysql.jpg'

def image_processing_mysql(img):
    # img.encode('utf-8').strip()
    # img = img.tobytes()?????
    # img = img.decode('utf-8')?????
    # img = cv2.imread(img)
    # print('kích thước', img.shape)
    # print('kiểu', img.dtype)

    # image_binary = bytearray(open(img, 'rb').read()) ##########???????????????????
    # img = cv2.imdecode(np.frombuffer(img, np.uint8), cv2.IMREAD_COLOR)

    image_binary = bytearray(cv2.imencode('.jpg', img)[1])

    print("bước 1 ok")
    # blob_data = memoryview(image_binary).tobytes()

    df = spark.createDataFrame([(image_binary,)], ["image"])

    print("bước 2 ok")
    # df = df.withColumn("image", df["image"].cast(BinaryType()))


    df_finish = (df.write.format("jdbc")
            .option("driver","com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost:3306/doanbigdata_db")
            .option("dbtable", "images")
            .option("user", "root")
            .option("password", "7153")
            .option("numPartitions", 5)
            .option("fetchsize", 20)
            .mode("append")
            .save())
    # df = (spark.read.format("jdbc")   #đọc đc mà write đéo đc
    #         # .option("driver","com.mysql.cj.jdbc.Driver")
    #         .option("url", "jdbc:mysql://localhost:3306/doanbigdata_db")
    #         .option("dbtable", "images")
    #         .option("user", "root")
    #         .option("password", "7153")
    #         )
    # a = df.load()
    # a.show()
    # print(df)
    print("ok")

    return df_finish

# a = image_processing_mysql(data)
# a.show()
print("OKKKKKKKKKKKKKKKKKKKK")

































# test spark sql
# Khởi tạo SparkSession

# spark = SparkSession.builder \
#     .appName("SparkMySQLConnection") \
#     .getOrCreate()
#
# # Thông tin kết nối MySQL
# mysql_host = "localhost"
# mysql_port = "3306"
# mysql_database = "word"
# mysql_username = "root"
# mysql_password = "7153"
#
# # Tạo URL kết nối MySQL
# mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
#
# # Đọc dữ liệu từ MySQL vào DataFrame
# df = spark.read \
#     .format("jdbc") \
#     .option("url", mysql_url) \
#     .option("dbtable", "city") \
#     .option("user", mysql_username) \
#     .option("password", mysql_password) \
#     .load()
#
# df.show()



# mysql co ban
# import mysql.connector
# mydb = mysql.connector.connect(
#     host='localhost',
#     user='root',
#     password='7153',
#     port='3306',
#     database='world'
# )
# mycursor = mydb.cursor()
# mycursor.execute('SELECT * FROM city')
#
# cities = mycursor.fetchall()
# for city in cities:
#     print(city)
#     print('city' + city[1])
#     print('')
