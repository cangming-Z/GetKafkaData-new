# _*_ coding: utf-8 _*_
# __author__ = 'zhangdeqiang'


import time
import logging
from tools import path


class Logger(object):
    def __init__(self, logger):
        """
            指定保存日志的文件路径，日志级别，以及调用文件
            将日志存入到指定的文件中
        """

        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)
        # 获取工程根目录
        path_obj = path.get_obj_path()
        # 创建一个handler，用于写入日志文件
        rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))[:8]
        # log_path = os.path.dirname(os.getcwd()) + '\result\log\'  # 项目根目录下/result/log 保存日志
        log_path = path.sep.join([path_obj, 'log'])
        log_name = log_path + path.sep + rq + '.log'
        fh = logging.FileHandler(log_name, encoding='utf-8')
        fh.setLevel(logging.INFO)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def get_log(self):
        return self.logger