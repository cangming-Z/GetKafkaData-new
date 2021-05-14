import sys
import datetime
import configparser
from tools import path
from proto import tag_pb2
from tools.logger import Logger
from tools.useKafka import pyKafka
from multiprocessing import Process
from tools.getMesInfo import MesData
from tools.globalconfig import Global


log = Logger(logger="evaluation").get_log()

# 将kafka数据写入到greenplum（为缓解数据库压力，数据为分钟级，且分项目分月建表）

# 获取项目根目录
filepath = path.get_obj_path()
config = configparser.ConfigParser()
filePath = path.sep.join([filepath, 'config', 'kafka_to_gp_conf.ini'])
# 程序开启后，记录不同项目生成的表名
exit_table_time = {}
# kafka数据每分钟只存一条（减缓数据库压力）
spec_pro_min_record = {}
# 相同项目，不同生产者，发送到kafka的数据分为多组，一分钟内，每组取一条
# 项目名_时间为key，任意组的首个标签为value，当记录不存在时，整组数据插入数据库
msg_records = {}


# 根据topic获取的数据时间，判断数据库是否有这个月的表，没有则新建
def deal_with_table(project, create_table_name, gp):
    if create_table_name not in exit_table_time[project]:
        # 查看表是否已存在
        sql_table_exists = "select * from pg_tables where tablename = '%s'" % create_table_name
        exists = gp.db_rw(sql_table_exists)
        if exists is not None and len(exists) > 0:
            print('表%s已存在' % create_table_name)
            exit_table_time[project].append(create_table_name)
        else:
            create_sql = 'create table %s(id serial,tag varchar(255),value varchar(255),' \
                         'time timestamp(0),dw_create_time timestamp(0));' \
                         % create_table_name
            sign = gp.db_rw(create_sql)
            if sign:
                exit_table_time[project].append(create_table_name)
                print('创建表%s成功' % create_table_name)
            else:
                print('创建表%s失败' % create_table_name)


# 处理kafka消费到的数据：protobuf反序列化，并处理成指定格式
def deal_with_consumer_data(project, source, msg, gp):
    try:
        target = tag_pb2.interface_param()
        target.ParseFromString(msg)

        time = target.Param[0].time
        name = project + target.Param[0].name
        if name not in msg_records.keys():
            msg_records[name] = []

        # 表名的处理
        aa = int(time[6:8])
        time_to_month = time[:6]
        if 0 <= aa <= 10:
            table_name = project + '_' + source + '_' + time_to_month + '_' + '01'
        elif 11 <= aa <= 20:
            table_name = project + '_' + source + '_' + time_to_month + '_' + '02'
        else:
            table_name = project + '_' + source + '_' + time_to_month + '_' + '03'
        deal_with_table(project, table_name, gp)

        # 数据库记录处理
        record_time = time[:12]
        if record_time not in spec_pro_min_record[project]:
            spec_pro_min_record[project].append(record_time)
            msg_records[name].append(record_time)

            time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + " " + time[8:10] \
                   + ":" + time[10:12] + ":" + time[12:14]

            sql = "INSERT INTO public.%s(tag, value, time, dw_create_time) VALUES " % table_name
            for tar in target.Param:
                sql += "('%s', '%s', '%s', '%s')," % (tar.name, tar.value, time, datetime.datetime.now())
            sql = sql[:len(sql)-1]
            gp.db_rw(sql)
            print('%s：kafka时间为%s（当前时间：%s）的数据录入数据库' % (project, time, datetime.datetime.now()))

            if len(spec_pro_min_record[project]) >= 10240:
                spec_pro_min_record[project] = spec_pro_min_record[project][5120:]

        # 同分钟不同kafka消息组数据写入数据库
        elif record_time not in msg_records[name]:
            msg_records[name].append(record_time)
            time = time[:4] + "-" + time[4:6] + "-" + time[6:8] + " " + time[8:10] \
                   + ":" + time[10:12] + ":" + time[12:14]

            sql = "INSERT INTO public.%s(tag, value, time, dw_create_time) VALUES " % table_name
            for tar in target.Param:
                sql += "('%s', '%s', '%s', '%s')," % (tar.name, tar.value, time, datetime.datetime.now())
            sql = sql[:len(sql) - 1]
            gp.db_rw(sql)
            print('%s：kafka时间为%s（当前时间：%s）的数据录入数据库' % (project, time, datetime.datetime.now()))
            print('同分钟不同kafka消息组数据写入数据库')

            for key in msg_records.keys():
                if len(msg_records[key]) >= 10240:
                    msg_records[project] = msg_records[project][5120:]
    except Exception as e:

         print("(kafka时间为%s，当前时间为%s)数据插入数据库出错：%s" % (time, datetime.datetime.now(), e))


# kafka生产者进程（槐坎南方、大冶尖峰、泉头水泥）
def work(project, first):
    try:
        print("进程%s开始" % project)

        exit_table_time[project] = []
        spec_pro_min_record[project] = []

        # 从配置文件中读取当前项目信息
        config.read(filePath, encoding='utf-8')
        origin_kafka_url = config[project]['originKafkaUrl']
        origin_topic_name = config[project]["originTopicName"]

        global_config = Global()
        gp_db = global_config.gp_db
        gp_conn = gp_db.get_postgresql_conn()
        gp = MesData(gp_conn)

        origin_kafka = pyKafka(origin_kafka_url, origin_topic_name)
        origin_kafka.customConsumer(deal_with_consumer_data, project, gp, first)
    except Exception as e:
        print("%s" % e)
        sys.exit()


if __name__ == '__main__':
    try:
        first = False
        # 从配置文件中读取当前项目信息
        config.read(filePath, encoding='utf-8')
        tread_names = eval(config["work"]["runProject"])
        for treadName in tread_names:
            # 新增进程
            child_process = Process(target=work, args=(treadName, first))
            child_process.start()
    except Exception as e:
        print("读取配置文件，配置进程列表出错：%s" % e)
        sys.exit()

    # 不用线程，进行调试时使用
    # project = 'cxnf'
    # exit_table_time[project] = []
    # spec_pro_min_record[project] = []
    #
    # global_config = Global()
    # gp_db = global_config.gp_db
    # gp_conn = gp_db.get_postgresql_conn()
    # gp = MesData(gp_conn)
    #
    # # deal_with_table(project, date, create_table_name, gp)
    #
    # # 从配置文件中读取当前项目信息
    # config.read(filePath, encoding='utf-8')
    # origin_kafka_url = config[project]['originKafkaUrl']
    # origin_topic_name = config[project]["originTopicName"]
    #
    # origin_kafka = pyKafka(origin_kafka_url, origin_topic_name)
    # origin_kafka.customConsumer(deal_with_consumer_data, project, gp)
