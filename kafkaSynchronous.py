# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang


import sys
import time
import datetime
import configparser
from multiprocessing import Process

from tools import path
from tools.useKafka import pyKafka


# 获取项目根目录
filepath = path.get_obj_path()
config = configparser.ConfigParser()
filePath = path.sep.join([filepath, 'config', 'kafka211_to_kafka195_conf.ini'])


# 读取配置文件，获取配置的进程列表
def readConfig():
    try:
        # 从配置文件中读取当前项目信息
        config.read(filePath, encoding='utf-8')
        tread_names = eval(config["work"]["runProject"])
        for treadName in tread_names:
            # 新增进程
            child_process = Process(target=work, args=(treadName,))
            child_process.start()
    except Exception as e:
        print("读取配置文件，配置进程列表出错：%s" % e)
        sys.exit()


# kafka生产者进程（槐坎南方、大冶尖峰、泉头水泥）
def work(project):
    try:
        print("进程%s开始" % project)
        # 从配置文件中读取当前项目信息
        config.read(filePath, encoding='utf-8')

        origin_kafka_url = config[project]['originKafkaUrl']
        origin_topic_name = config[project]["originTopicName"]
        target_kafka_url = config[project]['targetKafkaUrl']
        target_topic_name = config[project]["targetTopicName"]

        origin_kafka = pyKafka(origin_kafka_url, origin_topic_name)
        target_kafka = pyKafka(target_kafka_url, target_topic_name)

        origin_kafka.syncConsumer(target_kafka)
    except Exception as e:
        print("%s" % e)
        sys.exit()


if __name__ == '__main__':
    print("kafka数据同步开始：%s" % time.time())
    readConfig()
