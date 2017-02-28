# encoding: utf-8
# 2017.02.27 create by james.peng,referenced by lele.liu

import configparser
import os

# 封装连接vertica类
# 2017.02.27 create by james.peng


class KVCONVERTICA(object):

    #初始化config解析器
    cf = configparser.ConfigParser()

    # dwh3_conf 文件存放路径
    conf_path = os.path.join(os.getcwd(), 'conf')
    conf_filename = os.path.join(conf_path, 'config.conf')

    # 读入配置文件
    cf.read(conf_filename, encoding='utf8')
    host = cf.get("vertica", "host")
    port = int(cf.get("vertica", "port"))
    user = str(cf.get("vertica", "user"))
    passwd = str(cf.get("vertica", "passwd"))
    db = str(cf.get("vertica", "db"))
    read_timeout = cf.get("vertica", "read_timeout")
    unicode_error = str(cf.get("vertica", "unicode_error"))
    sql1 = str(cf.get("vertica", "sql1"))
    sql_copy_head = str(cf.get("vertica", "sql_copy_head"))
    sql_copy_nail = str(cf.get("vertica", "sql_copy_nail"))