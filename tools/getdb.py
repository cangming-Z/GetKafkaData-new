#!/usr/bin/env python
# -*- coding:utf-8 -*-
import psycopg2

_author_ = 'zhushp'

import configparser
import pymysql
import sys


class GetDB:
    # 配置数据库IP，端口等信息，获取数据库连接
    def __init__(self, ini_file, db):
        config = configparser.ConfigParser()
        # 从配置文件中读取数据库服务器IP、端口，域名
        config.read(ini_file)
        self.db = config[db]['db']
        self.host = config[db]['host']
        self.port = eval(config[db]['port'])
        self.user = config[db]['user']
        self.passwd = config[db]['passwd']
        self.charset = config[db]['charset']

    def get_conn(self):
        try:
            conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                   db=self.db, charset=self.charset)
            return conn
        except Exception as e:
            print('str(e):\t\t', str(e))
            sys.exit()

    def get_postgresql_conn(self):
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.passwd,
                                    database=self.db)
            return conn
        except Exception as e:
            print('str(e):\t\t', str(e))
            sys.exit()
