# -*- coding:utf-8 -*-
# __author = 'zhushup'
import pymysql
from warnings import filterwarnings

filterwarnings('ignore', category=pymysql.Warning)



class MesData:
    """
    获取测试数据库数据
    """

    def __init__(self, db_conn):
        """
        初始化对象
        :param db_conn: 数据库连接对象
        """
        self.db_conn = db_conn
        self.cursor = self.db_conn.cursor()
        self.userid = ''
        self.username = ''
        self.realname = ''

    def get_mes_data(self):
        """
         获取当前登录用户ID、用户名、真实姓名
        :return:
        """
        get_mes_data_sql = "SELECT username, real_name FROM byt_mes.sys_user WHERE user_id=1 "

        self.cursor.execute(get_mes_data_sql)
        tmp_result = self.cursor.fetchone()
        self.db_conn.commit()
        self.username = tmp_result[0]
        self.realname = tmp_result[1]

    def db_rw(self, sql):
        """
        执行select语句查询数据库数据
        :param sql: sql
        :param args: 参数，多个条件时应传入元组
        :return:返回查询结果
        """
        try:
            self.cursor.execute(sql)
            if 'insert' in sql.lower() or 'update' in sql.lower() or 'create' in sql.lower():
                result = True
            else:
                result = self.cursor.fetchall()
            self.db_conn.commit()
            return result
        except Exception as e:
            self.db_conn.commit()
            print(sql)
            print(e)
            return False

    def db_close(self):
        self.db_conn.close()
