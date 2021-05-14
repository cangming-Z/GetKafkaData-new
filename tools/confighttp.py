#!/usr/bin/evn python
# -*- coding:utf-8 -*-
# _author_ = 'zhushp'

import configparser


# 配置类
class ConfigHttp:
    def __init__(self, ini_file, http):
        self.__protocol = 'http://'
        config = configparser.ConfigParser()
        # 从配置文件中读取接口服务器IP、域名、端口
        config.read(ini_file)
        self.__host = self.__protocol + config.get(http, 'host')
        # self.port = config.get(http, 'port')
        self.__timeout = config.get(http, "timeout")
        self.__topicName = config.get(http, "topicName")
        self.__kafkaUrl = config.get(http, "kafkaUrl")
        self.__pTopicName = config.get(http, "preprocessorTopicName")
        self.__pKafkaUrl = config.get(http, "preprocessorKafkaUrl")
        self.__headers = None
        self.__params = None
        self.__data = None
        self.__url = None
        self.__files = None
        self.__state = None
        self.__auth = None

    def set_url(self, url):
        """
        set url
        :param: interface url
        :return:
        """
        self.__url = url

    def set_header(self, header):
        """
        set headers
        :param header:
        :return:
        """
        self.__headers = header

    def set_params(self, param):
        """
        set params
        :param param:
        :return:
        """
        if param:
            self.__params = eval(param)
        else:
            self.__params = {}

    def set_data(self, data):
        """
        set data
        :param data:
        :return:
        """
        if isinstance(data, str):
            self.__data = eval(data)
        else:
            self.__data = data

    def set_files(self, filename):
        """
        set upload files
        :param filename:
        :return:
        """
        if filename != '':
            file_path = 'F:/AppTest/Test/interfaceTest/testFile/img/' + filename
            self.__files = {'file': open(file_path, 'rb')}

        if filename == '' or filename is None:
            self.__state = 1

    def get_url(self):
        """
        get url
        """
        return self.__url.split(self.__host)[1]

    def get_params(self):
        """
        get params
        :return:
        """
        return self.__params

    def get_header(self):
        """
        get params
        :return:
        """
        return self.__headers

    def get_auth(self):
        """
        get params
        :return:
        """
        return self.__auth

    def get_host(self):
        """
        :return: get host
        """
        return self.__host

    def get_topicName(self):
        """
        :return: get host
        """
        return self.__topicName

    def get_kafkaUrl(self):
        """
        :return: get host
        """
        return self.__kafkaUrl

    def get_pTopicName(self):
        """
        :return: get host
        """
        return self.__pTopicName

    def get_pKafkaUrl(self):
        """
        :return: get host
        """
        return self.__pKafkaUrl

