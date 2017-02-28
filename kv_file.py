# encoding: utf-8
# 2017.02.27 create by james.peng,referenced by lele.liu

import configparser
import os

# 封装连接vertica类
# 2017.02.27 create by james.peng


class KVFILE(object):

    #初始化config解析器
    cf = configparser.ConfigParser()

    # dwh3_conf 文件存放路径
    conf_path = os.path.join(os.getcwd(), 'conf')
    conf_filename = os.path.join(conf_path, 'config.conf')

    # 读入配置文件
    cf.read(conf_filename, encoding='utf8')
    path_name = cf.get("path", "path_name")
    log_name = cf.get("path", "log_name")
