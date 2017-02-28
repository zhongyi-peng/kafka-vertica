# encoding: utf-8
# 2017.02.27 create by james.peng,referenced by lele.liu

import configparser
import os

# 封装连接vertica类
# 2017.02.27 create by james.peng


class KVCONKAFKA(object):

    #初始化config解析器
    cf = configparser.ConfigParser()

    # dwh3_conf 文件存放路径
    conf_path = os.path.join(os.getcwd(), 'conf')
    conf_filename = os.path.join(conf_path, 'config.conf')

    # 读入配置文件
    cf.read(conf_filename, encoding='utf8')
    hosts = cf.get("kafka", "hosts")
    topic = cf.get("kafka", "topic")
    consumer_timeout_ms = int(cf.get("kafka", "consumer_timeout_ms"))