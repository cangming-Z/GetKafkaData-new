#!/usr/bin/evn python
# -*- coding:utf-8 -*-

from tools import path
from tools.confighttp import ConfigHttp
from tools.getdb import GetDB


# 获取项目根目录
filepath = path.get_obj_path()


class Global:
    def __init__(self):
        # 读取并配置接口服务器IP，端口等信息
        self.http = ConfigHttp(path.sep.join([filepath, 'config', 'http_conf.ini']), 'HTTP1')
        # 读取并配置数据库IP，端口等信息
        self.test_db = GetDB(path.sep.join([filepath, 'config', 'db_config.ini']), 'testDatabase')
        self.gp_db = GetDB(path.sep.join([filepath, 'config', 'db_config.ini']), 'gp')
        self.pg_db = GetDB(path.sep.join([filepath, 'config', 'db_config.ini']), 'postgreSQL')

    def get_http(self):
        return self.http

    def get_topicName(self):
        return self.http.get_topicName()

    def get_kafkaUrl(self):
        return self.http.get_kafkaUrl()

    def get_test_db_conn(self):
        return self.test_db.get_conn()

    def get_gp_db_conn(self):
        return self.gp_db.get_conn()

    def get_pg_db_conn(self):
        return self.pg_db.get_conn()

    # 释放资源
    def clear(self):
        # 关闭数据库连接
        self.test_db.get_conn().close()

