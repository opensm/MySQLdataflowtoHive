# -*- coding: utf-8 -*-
from lib.HiveOperation import HiveOpration
from lib.KafkaConsumer import KafkaQuery
from lib.HiveLoadData import RecordData
from lib.settings import ROOT_DIR, KAFKA_CONFIG, HIVE_CONFIG
import sys
import getopt

r = RecordData()
if not r.check_archives_write(archives_name=ROOT_DIR):
    sys.exit(1)
if not r.check_archives_read(archives_name=ROOT_DIR):
    sys.exit(1)


def useage():
    print("%s -h\t#帮助文档" % sys.argv[0])
    print("%s -r\t#导入hive端启动" % sys.argv[0])
    print("%s -k\t#kafka消费端启动" % sys.argv[0])


def main():
    if len(sys.argv) == 1:
        useage()
        sys.exit()
    try:
        options, args = getopt.getopt(
            sys.argv[1:],
            "rkh"
        )
    except getopt.GetoptError:
        print("%s -h" % sys.argv[0])
        sys.exit(1)
    command_dict = dict(options)
    # 帮助
    if '-h' in command_dict:
        useage()
        sys.exit()
    # 获取监控项数据
    elif '-k' in command_dict:
        k = KafkaQuery()
        k.blind(**KAFKA_CONFIG)
        exec_function = getattr(r, "service_record_table")
        k.run(exec_function=exec_function)
    elif '-r' in command_dict:
        h = HiveOpration(**HIVE_CONFIG)
        h.run()
    else:
        useage()
        sys.exit(1)


if __name__ == "__main__":
    main()
