#  代码部分
# 全量抽取
# https://docs.python.org/2/library/json.html
import sys
import time
import os
import json
import vertica_python
from pykafka import KafkaClient
from kv_con_vertica import KVCONVERTICA
from kv_con_kafka import KVCONKAFKA
from kv_file import KVFILE

# 显示当前时间,获取当前编码
start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
print('开始时间',start_time )
print(sys.getdefaultencoding())

# kafka的producer地址端口,读取策略
kafka_para=KVCONKAFKA()
client = KafkaClient(hosts=kafka_para.hosts)
print(client.topics)  # 查看所有topic
topic = client.topics[kafka_para.topic.encode('UTF-8')]  # 选择一个topic
consumer = topic.get_simple_consumer(consumer_timeout_ms=kafka_para.consumer_timeout_ms)  # 等待2秒无新数据,退出

# 填写数据库连接信息
vertica_para=KVCONVERTICA()
conn_info = {'host': vertica_para.host, 'port': vertica_para.port, 'user': vertica_para.user,
             'password': vertica_para.passwd, 'database': vertica_para.db,
             'read_timeout': vertica_para.read_timeout, 'unicode_error': vertica_para.unicode_error}
print(conn_info)
connection = vertica_python.connect(**conn_info)
cur = connection.cursor()  # 定义一个连接
cur.execute(vertica_para.sql1)
cur_result = str(cur.fetchone())
cur_loc1 = cur_result.find("'")
cur_loc2 = cur_result.rfind("'")
a_offset_start = int(cur_result[cur_loc1+1:cur_loc2])
print(a_offset_start)
connection.close()

#  清空输出文件
#  初始化offset文件和日志文件
a_error_count = 0
a_correct_count = 0
file_para=KVFILE()
output_path = os.path.abspath(file_para.path_name)
output_name = file_para.path_name
log_path = os.path.abspath(file_para.log_name)
log_name = file_para.log_name
print(output_path)
print(output_name)
print(log_path)
print(log_name)

# 打开文件
file_output = open(os.path.abspath(output_name), "w+", encoding='utf8')
file_output.truncate()

#  开始处理数据,异常则记录数量,正常则继续,
#  拼接offset后输出到文件,无异常的json格式
#  记录到文件offset.txt
for message in consumer:
    if message is not None and message.offset > a_offset_start:
        try:
            str_offset_join = message.value.decode()
            a = '{"offsets":"' + str(message.offset) + '",' + str_offset_join.lstrip('{')
            b = json.loads(a)
            file_output.write(a)
            file_output.write('\n')
            a_correct_count += 1
        except:
            a_error_count += 1
            continue

# 记录到日志
# 关闭文件输入
end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
log_output = open(os.path.abspath(log_name), "w+", encoding='utf8')
log_output.truncate()
log_output.write('offset = ' + str(message.offset))
log_output.write('\n' + '开始时间 = ' + start_time)
log_output.write('\n' + '结束时间 = ' + end_time)
log_output.write('\n' + 'a_error_count = ' + str(a_error_count))
log_output.write('\n' + 'a_correct_count = ' + str(a_correct_count))
log_output.close()
file_output.close()
# print('完成时间', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
# 文件处理完成

# copy导入至vertica
# 预处理
end_time_name=end_time.replace('-','')
end_time_name=end_time_name.replace(':','')
end_time_name=end_time_name.replace(' ','')[0:-4]
output_file = "output_"+end_time_name+".txt"
copy_statement = vertica_para.sql_copy_head + output_file+"' " +vertica_para.sql_copy_nail
# print("output_file = "+output_file)
# print("copy_statement = "+copy_statement)
# 开始导入
connection = vertica_python.connect(**conn_info)
cur = connection.cursor()
cur.execute(copy_statement)
connection.close()
print("load complete")
print('结束时间', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))