import datetime
import random

from proto import tag_pb2
from tools.useKafka import pyKafka


auto_value = None


def sim_data(tags, auto_msg):
    global auto_value
    result = []
    for tag in tags:
        res = []
        # 数值
        if tag[1] == 0:
            # 0为递增
            if tag[2] == 0:
                value = auto_value
            # 1 为随机值
            else:
                value = round(random.randrange(auto_msg[0], auto_msg[1]), 2)
        # bool
        else:
            # bool固定值
            if tag[2] == 0:
                # 0为False
                if tag[3] == 0:
                    value = False
                else:
                    value = True
            # bool随机值
            else:
                a = random.randint(0, 1)
                if a == 0:
                    value = False
                else:
                    value = True
        res.append(tag[0])
        res.append(value)
        result.append(res)
    if auto_msg is not None:
        if auto_value == auto_msg[1]:
            auto_value = auto_msg[0]
        else:
            auto_value += auto_msg[2]
    return result


def work(topic, sleep_time, tags, auto_msg, kafka):
    try:
        time_start = datetime.datetime.now()
        while True:
            if datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S') \
                    == time_start.strftime('%Y-%m-%d %H-%M-%S'):
                time_standard = time_start.strftime("%Y%m%d%H%M%S").encode()

                proto = tag_pb2.interface_param()
                values = sim_data(tags, auto_msg)
                print('%s,%s' % (time_start, values))

                for value in values:
                    param = proto.Param.add()
                    param.name = value[0]
                    param.time = time_standard
                    param.value = str(value[1])
                kafka.product(proto.SerializeToString(), time_standard)
                time_start += datetime.timedelta(seconds=sleep_time)
                print("topic：%s；-time：%s#-#" % (topic, time_standard))
    except Exception as e:
        print(e)


if __name__ == '__main__':
    print("kafka数据模拟开始：%s" % datetime.datetime.now())
    sleep_time = 10
    origin_kafka_url = '192.168.0.130:9092'
    origin_topic_name = 'opc-data'
    # 数值类型：[【位号名】,【0：数值；1：bool】，【0：递增；1：随机】]
    # bool类型：[【位号名】,【0：数值；1：bool】，【0：固定；1：随机】，【0：False；1：True】]
    # tags_msg = [['a', 0, 0], ['b', 0, 1], ['c', 1, 1]]

    tags_msg = [['test01', 0, 1], ['test02', 0, 1], ['test03', 0, 1], ['test04', 0, 0]]

    # [【最小值】，【最大值】，【步长】]
    auto_msg = [100, 150, 10]
    auto_value = auto_msg[0]
    origin_kafka = pyKafka(origin_kafka_url, origin_topic_name)
    work(origin_topic_name, sleep_time, tags_msg, auto_msg, origin_kafka)
