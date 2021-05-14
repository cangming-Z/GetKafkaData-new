# -*- coding:utf-8 -*-
# _author_ = 'zhangdeqiang

import os
import random
import sys
import time
import pymysql
import datetime
from proto import tag_pb2
from multiprocessing import Process
from tools.getMesInfo import MesData
from tools.useKafka import pyKafka


curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


# 获取项目根目录
try:
    conn = pymysql.Connect(host='192.168.8.200', port=3306, user='root',
                           passwd='root', db='byt_mes_ops')
    db_main = MesData(conn)
except Exception as e:
    print('全局变量配置出错')


# 关闭指定进程
def killProcess(processDetail):
    for key in processDetail:
        # Windows系统
        cmd = 'taskkill /pid ' + str(processDetail[key]) + ' /f'
        print("cmd指令%s" % cmd)
        try:
            print("子进程：%s(pid：%s)关闭中..." % (key, processDetail[key]))
            p = os.popen(cmd)
        except Exception as e:
            print("子进程：%s(pid：%s)关闭错误：\n%s" % (key, processDetail[key], e))
        print()


# 读取配置文件，获取配置的进程列表
def readConfig(sleepTime, firstTreadNames):
    try:
        startTime = datetime.datetime.now()

        while True:
            # 从配置文件中读取当前项目信息
            print("*---------\n%s：从数据库读取配置文件" % startTime)
            sql_kafka = "SELECT distinct kafka_addr,topic_name FROM `tmp_tag_si` where enable='0'"
            kafkas = db_main.db_rw(sql_kafka)
            kafka_msgs = []
            for kafka in kafkas:
                kafka_msg = kafka[0] + '@@@' + kafka[1]
                kafka_msgs.append(kafka_msg)
                if kafka_msg not in total_config.keys():
                    # 新增进程
                    print("配置文件更改-新增进程：%s" % kafka_msg)
                    childProcess = Process(target=work, args=(kafka[0], kafka[1],))
                    childProcess.start()
                    total_config[kafka_msg] = [childProcess.pid, {}]

            del_keys = []
            for key in total_config.keys():
                trend_mag = {}
                if key not in kafka_msgs:
                    trend_mag[key] = total_config[key][0]
                    del_keys.append(key)
                    # 删除进程
                    killProcess(trend_mag)
                    print("配置文件更改-删除进程：%s" % key)

            for del_key in del_keys:
                del total_config[del_key]

            print("配置文件读取结束，进程更新结束，当前进程：%s\n---------*"
                  % str(total_config.keys()))

            startTime += datetime.timedelta(seconds=sleepTime)

            time.sleep(60)
    except Exception as e:
        print("读取配置文件，配置进程列表出错，程序终止：%s" % e)
        sys.exit()


def work(kafkaUrl, topicName):
    try:
        print("---*---进程%s-%s开始---*---" % (kafkaUrl, topicName))
        tag_details = {}

        kafka_client = pyKafka(kafkaUrl, topicName)

        while True:
            print("*==========")
            time_start = datetime.datetime.now()
            # 从配置文件中读取当前项目信息
            sql_pro = "SELECT tag_name,value_type,increasing,lower,upper,step," \
                      "reload,time_interval " \
                      "FROM `tmp_tag_si` where kafka_addr = '%s' and topic_name = '%s' and enable='0'" \
                      % (kafkaUrl, topicName)
            pro_msgs = db_main.db_rw(sql_pro)

            tags_now = []  # 最新的标签集合

            modify = False

            # 新增或修改标签配置
            for pro_msg in pro_msgs:
                tags_now.append(pro_msg[0])
                # 标签已存在，若配置改变，则变更配置
                if pro_msg[0] in tag_details.keys():
                    for i in range(1, len(pro_msg) - 1):
                        if pro_msg[i] != tag_details[pro_msg[0]][i-1]:
                            tag_details[pro_msg[0]][i-1] = pro_msg[i]
                            modify = True

                # 标签不存在，新增配置
                else:
                    tag_detail = []
                    for i in range(1, len(pro_msg)):
                        tag_detail.append(pro_msg[i])

                    tag_detail.append(pro_msg[3])  # 设置位号的当前值，初始为下限
                    tag_details[pro_msg[0]] = tag_detail
                    modify = True

            del_tags = []
            # 删除标签，清空配置
            for key in tag_details.keys():
                if key not in tags_now:
                    del_tags.append(key)
                    modify = True

            for del_tag in del_tags:
                del tag_details[del_tag]

            time_standard = time_start.strftime("%Y%m%d%H%M%S").encode()
            # if modify is True:
            print("kafka地址：%s；topic：%s；标签配置：%s" % (kafkaUrl, topicName, tag_details))

            proto = tag_pb2.interface_param()
            values = simData(tag_details)

            for value in values:
                param = proto.Param.add()
                param.name = value[0]
                param.time = time_standard
                param.value = str(value[1])
            kafka_client.product(proto.SerializeToString(), time_standard)
            print("时间：%s；kafka的key：%s；value：%s" % (time_start, time_standard, values))

            print("==========*")
            sleep_time = int(pro_msgs[0][7])
            time.sleep(sleep_time)
    except Exception as e:
        print("进程%s-%s出错:%s" % (kafkaUrl, topicName, e))


def simData(tag_msgs):
    # 【double/bool，随机/递增，下限，上限，步长，循环，间隔时间，当前值】
    result = []
    for tag in tag_msgs.keys():
        res = []
        # 数值
        if tag_msgs[tag][0] == 0:
            # 1为递增
            if tag_msgs[tag][1] == 1:
                value = tag_msgs[tag][7]

                # 判断是否递增至最大值
                if tag_msgs[tag][7] + tag_msgs[tag][4] == tag_msgs[tag][3]:
                    # 判断是否循环，循环则赋最小值；不循环则保持最大值
                    if tag_msgs[tag][5] == 0:
                        tag_msgs[tag][7] = tag_msgs[tag][2]
                # 判断是否递增至大于最大值
                elif tag_msgs[tag][7] + tag_msgs[tag][4] > tag_msgs[tag][3]:
                    if tag_msgs[tag][5] == 0:
                        tag_msgs[tag][7] = tag_msgs[tag][2]
                    else:
                        tag_msgs[tag][7] = tag_msgs[tag][3]
                # 不到最大值则递增
                else:
                    tag_msgs[tag][7] += tag_msgs[tag][4]

            # 0 为随机值
            else:
                # value = round(random.randrange(tag[3], tag[4]), 2)
                value = round(random.uniform(tag_msgs[tag][2], tag_msgs[tag][3]), 2)
        # bool
        # 随机/递增：0随机；1固定值
        # 步长：0为FALSE；1为True
        else:
            # bool固定值
            if tag_msgs[tag][1] == 1:
                # 0为False
                if tag_msgs[tag][4] == 0:
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
        res.append(tag)
        res.append(value)
        result.append(res)
    return result


if __name__ == '__main__':
    try:
        total_config = {}  # 当前kafka、标签等配置
        # readConfig(60, [])
        readConfig(60, [])
    except Exception as e:
        sys.exit()
    # 测试
    # work('gzjf')
