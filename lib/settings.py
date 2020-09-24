# -*- coding: utf-8 -*-
ROOT_DIR = "/data/python"
OFFSET_TIME = 30 * 60  # 30分钟
OFFSET_SIZE = 3 * 1024 * 1024 * 1024  # 3GiB
KAFKA_CONFIG = {
    "topics": "example",
    "group_id": 'test-consumer-group222ssd',
    "bootstrap_servers": ['10.255.50.85:9092'],
    "auto_offset_reset": "earliest"
}
HIVE_CONFIG = {
    "host": "10.255.50.84",
    "port": 10000,
}
HIVE_HOME = "/usr/local/hive"
HIVE_TABLE_PARTITION = False
HIVE_TABLE_PARTITION_NAME = "dt"
HIVE_TABLE_PARTITION_TYPE = "STRING"

# ##日志配置
LOG_DIR = "/tmp"
LOG_FILE = "kafka2hive.log"
LOG_LEVEL = 'INFO'
