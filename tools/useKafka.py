# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang'


import time
from proto import tag_pb2
from datetime import datetime
from pykafka import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer


# 处理kafka消费到的数据：protobuf反序列化，并处理成指定格式
def deal_with_consumer_data(msg):
    target = tag_pb2.interface_param()
    target.ParseFromString(msg)
    dict_result = {}
    time = target.Param[0].time
    time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + " " + time[8:10] \
           + ":" + time[10:12] + ":" + time[12:14]
    dict_result['create_time'] = time
    for tar in target.Param:
        tag = str(tar.name).replace('.', '')
        if tag not in dict_result.keys():
            dict_result[tag] = tar.value
    return dict_result


# python操作kafka
class pyKafka:
    def __init__(self, hosts, topic_name):
        self.hosts = hosts
        self.topic_name = topic_name
        self.client = KafkaClient(hosts=hosts, broker_version='0.10.0')
        # 选择一个topic,不存在则新建
        self.topic = self.client.topics[bytes(topic_name, encoding='utf-8')]
        self.producer = self.topic.get_sync_producer()  # 同步生产
        # self.producer = self.topic.get_producer()  #异步生产

    # 查看所有topic
    def get_all_topics(self):
        return self.client.topics

    # 生产者
    def product(self, message, key=None):
        try:
            if key is None:
                key = datetime.now().strftime("%Y%m%d%H%M%S").encode()
            print("%s-%s：生产者" % (self.hosts, self.topic_name))
            self.producer.produce(message, key)
        except Exception as e:
            print("%s-%s：生产者错误：%s" % (self.hosts, self.topic_name, e))

    # 生产者关闭
    def product_stop(self):
        self.producer.stop()

    # 消费者
    def consumer(self):
        try:
            topic = self.topic
            # offsets_earliest = topic.earliest_available_offsets()  # 最早可用偏移量
            offsets_latest = topic.latest_available_offsets()  # 最近可以偏移量
            partitions = topic.partitions
            consumer = topic.get_simple_consumer()
            for key in partitions:
                aa = offsets_latest[key].offset[0] - 1
                consumer.reset_offsets([(partitions[key], aa)])  # 设置offset
            print("消费开始")
            while True:
                message = consumer.consume()
                if message is not None:
                    value = message.value
                    deal_with_consumer_data(value)
        except Exception as e:
            print(e)

    # 消费者
    def customConsumer(self, format_value, project, conn, first):
        try:
            group_name = project
            topic = self.topic
            if first:
                consumer = topic.get_simple_consumer(consumer_group=group_name)
            else:
                offsets_earliest = topic.earliest_available_offsets()  # 最早可用偏移量
                offsets_latest = topic.latest_available_offsets()  # 最近可以偏移量
                partitions = topic.partitions
                consumer = topic.get_simple_consumer(consumer_group=group_name)
                for key in partitions:
                    aa = offsets_latest[key].offset[0] - 1
                    consumer.reset_offsets([(partitions[key], aa)])  # 设置offset
            print("消费开始")

            while True:
                message = consumer.consume()
                if message is not None:
                    value = message.value
                    key = message.partition_key
                    format_value(project, 'kafka', value, conn)
        except Exception as e:
            print(e)

    # 用于数据同步的消费者，将源kafka数据同步到目标kafka
    def syncConsumer(self, target_kafka):
        try:
            topic = self.topic
            # offsets_earliest = topic.earliest_available_offsets()  # 最早可用偏移量
            offsets_latest = topic.latest_available_offsets()  # 最近可以偏移量
            partitions = topic.partitions
            consumer = topic.get_simple_consumer()
            for key in partitions:
                aa = offsets_latest[key].offset[0] - 1
                consumer.reset_offsets([(partitions[key], aa)])  # 设置offset

            print("%s-%s：消费开始" % (self.hosts, self.topic_name))
            while True:
                message = consumer.consume()
                if message is not None:
                    value = message.value
                    key = message.partition_key
                    target_kafka.product(value, key)
        except Exception as e:
            print("%s-%s消费者出错：%s" % (self.hosts, self.topic_name, e))


class Kafka:
    def __init__(self, hosts, topicName):
        self.hosts = hosts
        self.topic = topicName
        self.kafkaProducer = KafkaProducer(bootstrap_servers=self.hosts)

    def producer(self, value, key=None, sleepTime=1):
        try:
            if key is None:
                key = str(datetime.now()).encode()
            self.kafkaProducer.send(self.topic, value, key)
            print("%s：插入kafka成功" % datetime.now())
            time.sleep(sleepTime)
        except Exception as e:
            print(e)

    # 生产者关闭
    def producerClose(self):
        self.kafkaProducer.close()


def kafkaConsumer(host, topic, format_value, conn):
    try:
        consumer = KafkaConsumer(topic, group_id='consumer-', bootstrap_servers=host,
                                 auto_offset_reset='largest', enable_auto_commit=False)
        print("消费开始")
        for message in consumer:
            if message is not None:
                value = message.value
                format_value(value, conn)
    except Exception as e:
        print(e)