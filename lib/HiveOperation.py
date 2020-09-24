# -*- coding: utf-8 -*-
import os
import sys
from lib.HiveLoadData import RecordData
from lib.settings import ROOT_DIR, HIVE_HOME, HIVE_TABLE_PARTITION_NAME, HIVE_TABLE_PARTITION_TYPE, HIVE_TABLE_PARTITION
from lib.Log import RecodeLog
import simplejson as json
import time
import random
import string
import platform

if os.name.startswith("win"):
    from impala.dbapi import connect
else:
    from pyhive.hive import connect  # or import hive
if int(platform.python_version().strip(".")[0]) < 3:
    import commands
else:
    import subprocess


class HiveOpration:
    def __init__(self, **kwargs):
        assert os.path.exists(os.path.join(HIVE_HOME, 'bin', 'hive'))
        try:
            conn = connect(**kwargs)
            self.cursor = conn.cursor()
        except Exception as error:
            RecodeLog.error("初始化class {0},失败,{1}".format(self.__class__.__name__, error))
            sys.exit(1)

    def check_table(self, db, table):
        """
        :param db:
        :param table:
        :return:
        """
        try:
            self.cursor.execute('''show tables in {0} like "{1}" '''.format(db, table))
            if len(self.cursor.fetchall()) == 0:
                raise Exception("The table {0}.{1} is not exist!".format(db, table))
            return True
        except Exception as error:
            RecodeLog.error("检查表class {0},function check_table,不存在或者失败,{1}".format(
                self.__class__.__name__,
                error
            ))
            return False

    def check_db(self, db):
        """
        :param db:
        :return:
        """
        try:
            self.cursor.execute('''show databases like "%s"''' % db)
            if len(self.cursor.fetchall()) == 0:
                raise Exception("The database '%s' is not exist!" % db)
            return True
        except Exception as error:
            RecodeLog.error("检查数据库失败class {0},function check_db,失败,{1}".format(
                self.__class__.__name__,
                error
            ))
            return False

    def check_table_partitioned(self, db, table):
        """
        :param db:
        :param table:
        :return:
        """
        try:
            self.cursor.execute('''show create table `{0}`.{1}'''.format(db, table))
            result = self.cursor.fetchall()
            if len(result) == 0:
                raise Exception("The database '%s' is not exist!" % db)
            if "PARTITIONED" in result:
                return {"status": True}
            else:
                return {"status": False}
        except Exception as error:
            RecodeLog.error("检查数据库失败class {0},function check_db,失败,{1}".format(
                self.__class__.__name__,
                error
            ))
            return False

    def create_table(self, sql):
        """
        :param sql:
        :return:
        """
        try:
            self.cursor.execute(sql)
            RecodeLog.info("创建表class {0},function create_table,成功,{1}".format(
                self.__class__.__name__,
                sql
            ))
            return True
        except Exception as error:
            RecodeLog.error("创建表失败class {0},function create_table,{1},失败,{2}".format(
                self.__class__.__name__,
                sql,
                error
            ))
            return False

    def create_database(self, db):
        """
        :param db:
        :return:
        """
        try:
            self.cursor.execute("create database %s" % db)
            return True
        except Exception as error:
            RecodeLog.error("创建数据库失败class {0},function create_database,失败,{1}".format(
                self.__class__.__name__,
                error
            ))
            return False

    def produce_name(self, data, default_name="ddl"):
        """
        :param data:
        :param default_name:
        :return:
        """
        if not isinstance(data, dict):
            return False
        if default_name in data.keys():
            result = "{0}{1}".format(default_name, time.strftime("%S", time.localtime()))
            return self.produce_name(data=data, default_name=result)
        else:
            return default_name

    def format_create_table_sql(
            self,
            db,
            table,
            data,
            row_format=",",
            line_terminate=r"\n",
            partition=None,
            partition_type=None
    ):
        """
        :param db:
        :param table:
        :param data:
        :param row_format:
        :param line_terminate:
        :param partition:
        :param partition_type:
        :return:
        """
        if not isinstance(data, dict):
            RecodeLog.error("生成创建表语句 class {0},function format_create_table_sql,失败,导入数据错误".format(
                "HiveOpration",
            ))
            return False
        after_table = dict()
        # MySQL与Hive字段类型映射
        for key, value in data.items():
            if "VARCHAR" in value.upper() or "CHAR" in value.upper():
                after_table[key] = "STRING"
                continue
            elif "INT" in value.upper() or "SMALLINT" in value.upper() or "MEDIUMINT" in value.upper():
                after_table[key] = "INT"
                continue
            elif "TINYINT" in value.upper():
                after_table[key] = "TINYINT"
            elif "DOUBLE" in value.upper():
                after_table[key] = "DOUBLE",
            elif "DECIMAL" in value.upper():
                after_table[key] = "DECIMAL"
            elif "FLOAT" in value.upper():
                after_table[key] = "FLOAT"
            else:
                RecodeLog.error("字段未能正确识别类型，以STRING处理 class {0},function format_create_table_sql,报警".format(
                    "HiveOpration",
                ))
                after_table[key] = "STRING"
        split_data = """ROW FORMAT DELIMITED FIELDS TERMINATED BY '{0}' LINES TERMINATED BY '{1}' STORED AS TEXTFILE""".format(
            row_format,
            line_terminate
        )
        # 产生一个随机ddl字段
        ddl = self.produce_name(data=after_table)
        # 格式化字段
        # after_table = "{0},{1} INT".format(
        #     json.dumps(after_table).rstrip("}").lstrip("{").replace(":", " ").replace('"', ""), ddl
        # )
        after_table = json.dumps(after_table).rstrip("}").lstrip("{").replace(":", " ").replace('"', "")
        if partition and partition_type:
            create_table_sql = "create table `{0}`.`{1}` ({2})partitioned by ({3} {4}) {5}".format(
                db,
                table,
                after_table,
                partition,
                partition_type,
                split_data
            )
        else:
            create_table_sql = "create table `{0}`.`{1}` ({2}) {3}".format(
                db,
                table,
                after_table,
                split_data
            )
        return create_table_sql

    def insert(self, sql, db):
        """
        :param sql:
        :param db:
        :return:
        """
        if not isinstance(sql, str):
            RecodeLog.error("输入类型错误 class {0},function insert,{1}失败".format(
                self.__class__.__name__,
                sql
            ))
            return False
        if 'INSERT INTO' not in sql.upper():
            RecodeLog.error("sql 错误 class {0},function insert,{1}失败".format(
                self.__class__.__name__,
                sql
            ))
            return False
        if "CREATE TABLE" in sql.upper():
            pass
        if not self.check_db(db=db):
            self.create_database(db=db)
        try:
            self.cursor.execute("use %s" % db)
            self.cursor.execute(sql)
            RecodeLog.info("class {0},function insert,{1}成功".format(
                self.__class__.__name__,
                sql
            ))
            return True
        except Exception as error:
            RecodeLog.error("class {0},function insert,{1}失败,{2}".format(
                self.__class__.__name__,
                sql,
                error
            ))
            return False

    def alert_table_partition(self, db, table, partition_name, partition_type):
        """
        :param db:
        :param table:
        :param partition_name:
        :param partition_type:
        :return:
        """
        if not HIVE_TABLE_PARTITION:
            return True
        partition = time.strftime("%Y%m%d", time.localtime())
        sql = "alter table `{2}`.`{3}` add if not exists partition({0}='{1}') location '{1}'".format(
            partition_name,
            partition,
            db,
            table
        )
        try:
            self.cursor.execute(sql)
            return True
        except Exception as error:
            RecodeLog.error("创建表分区失败 class {0},function alert_table_partition,{1}.{2} {4}={3}失败{5},{6}".format(
                self.__class__.__name__,
                db,
                table,
                partition,
                partition_name,
                error,
                sql
            ))
            return False

    def create_if_not_exist(self, db, table):
        """
        :param db:
        :param table:
        :return:
        """
        # ##################################创建数据库############################
        try:
            # 判断并创建数据库
            if not self.check_db(db=db):
                if not self.create_database(db=db):
                    raise Exception("该Hive实例不存在该数据库，创建数据库失败！")
                else:
                    RecodeLog.info("创建数据库 class {0},function create_if_not_exist,{1}成功".format(
                        self.__class__.__name__,
                        db
                    ))
        except Exception as error:
            RecodeLog.error("创建数据库 class {0},function create_if_not_exist,{1}失败{2}".format(
                self.__class__.__name__,
                db,
                error
            ))
            return False
        # ##########################创建表##############################
        try:
            if not self.check_table(db=db, table=table):
                create_data = RecordData.read_create_table_data(db=db, table=table)
                if HIVE_TABLE_PARTITION:
                    create_params = {
                        "db": db,
                        "table": table,
                        "partition": HIVE_TABLE_PARTITION_NAME,
                        "partition_type": HIVE_TABLE_PARTITION_TYPE,
                        "data": json.loads(create_data)
                    }
                else:
                    create_params = {
                        "db": db,
                        "table": table,
                        "data": json.loads(create_data)
                    }
                if not create_data:
                    raise Exception("读取表结构数据失败")

                sql = self.format_create_table_sql(**create_params)
                if not sql:
                    raise Exception("产生的建表语句异常")
                if not self.create_table(sql=sql):
                    raise Exception("创建表失败{0}".format(sql))
            return True
        except Exception as error:
            RecodeLog.error("创建表 class {0},function create_if_not_exist,{1}.{2}失败{3}".format(
                self.__class__.__name__,
                db,
                table,
                error
            ))
            return False

    def exec_hive_sql_file(self, sql_file, pro_root, data_root):
        """
        :param sql_file:
        :param pro_root:
        :param data_root:
        :return:
        """
        hive_bin = os.path.join(pro_root, 'bin', 'hive')
        abs_sql_file = os.path.join(data_root, sql_file)
        exec_str = "{0} -f {1}".format(hive_bin, abs_sql_file)
        # 获取数据库名称
        db = os.path.splitext(sql_file)[0].split("-")[-2]
        # 获取表名称
        table = os.path.splitext(sql_file)[0].split("-")[-1]
        try:
            if not self.check_table(db=db, table=table):
                return True
            if int(platform.python_version().strip(".")[0]) < 3:
                status, msg = commands.getstatusoutput(exec_str)
            else:
                status, msg = subprocess.getstatusoutput(exec_str)
            if status != 0:
                raise Exception(msg)
            new_name = "{0}.success.{1}".format(
                sql_file,
                ''.join(random.sample(string.ascii_letters + string.digits, 8))
            )
            RecordData.rename_record(archives_name=sql_file, new_archives=new_name)
            RecodeLog.info("class {0},function command_load,执行完成：{1}成功".format(
                self.__class__.__name__,
                exec_str
            ))
            return True
        except Exception as error:
            new_name = "{0}.error.{1}".format(
                sql_file,
                ''.join(random.sample(string.ascii_letters + string.digits, 8))
            )
            RecordData.rename_record(archives_name=sql_file, new_archives=new_name)
            RecodeLog.error("class {0},function command_load,{1}执行失败,{2}".format(
                self.__class__.__name__,
                exec_str,
                error
            ))
            return False

    def command_load(self, db, table, data_file, pro_root):
        """
        :param pro_root:
        :param db:
        :param table:
        :param data_file:
        :return:
        """
        HIVE_BIN = os.path.join(pro_root, 'bin', 'hive')
        archives = os.path.join(ROOT_DIR, data_file)
        # 带分区的表结构
        if HIVE_TABLE_PARTITION:
            exec_str = '''{0} -e "use {3}; LOAD DATA LOCAL INPATH '{1}' INTO TABLE \`{2}\` partition ({5}='{4}')"'''.format(
                HIVE_BIN,
                archives,
                table,
                db,
                time.strftime("%Y%m%d", time.localtime()),
                HIVE_TABLE_PARTITION_NAME
            )
        # 不带分区的表结构
        else:
            exec_str = '''{0} -e "use {3}; LOAD DATA LOCAL INPATH '{1}' INTO TABLE \`{2}\`"'''.format(
                HIVE_BIN,
                archives,
                table,
                db
            )
        try:
            # 创建表
            if not self.create_if_not_exist(db=db, table=table):
                raise Exception("创建Hive数据库失败")
            # 如果有表分区，修改表分区
            if not self.alert_table_partition(
                    db=db,
                    table=table,
                    partition_name=HIVE_TABLE_PARTITION_NAME,
                    partition_type=HIVE_TABLE_PARTITION_TYPE
            ):
                raise Exception("""修改分区信息失败""")
            RecodeLog.info("class {0},function command_load,开始导入：{1}".format(
                self.__class__.__name__,
                exec_str
            ))
            if int(platform.python_version().strip(".")[0]) < 3:
                status, msg = commands.getstatusoutput(exec_str)
            else:
                status, msg = subprocess.getstatusoutput(exec_str)
            if status != 0:
                raise Exception(msg)
            new_name = "{0}.success".format(os.path.splitext(data_file)[0])
            RecordData.rename_record(archives_name=data_file, new_archives=new_name)
            RecodeLog.info("class {0},function command_load,导入完成：{1}成功".format(
                self.__class__.__name__,
                exec_str
            ))
            return True
        except Exception as error:
            new_name = "{0}.error".format(os.path.splitext(data_file)[0])
            RecordData.rename_record(archives_name=data_file, new_archives=new_name)
            RecodeLog.error("class {0},function command_load,{1}失败,{2}".format(
                self.__class__.__name__,
                exec_str,
                error
            ))
            return False

    def run(self):
        RecodeLog.info("初始化成功，开始监听导入任务！")
        while True:
            for archive in RecordData.get_list_record("*.standby"):
                # 获取数据库名称
                db = os.path.splitext(archive)[0].split("-")[-2]
                # 获取表名称
                table = os.path.splitext(archive)[0].split("-")[-1]
                # standby 文件时间戳
                standby_unixtime = int(os.path.splitext(archive)[0].split("-")[-3])
                # 然后将表名修改
                alter_sql_list = RecordData.get_list_record("*-{0}-{1}.sql".format(db, table))
                if len(alter_sql_list) == 0:
                    self.command_load(
                        db=db,
                        table=table,
                        data_file=archive,
                        pro_root=HIVE_HOME
                    )
                else:
                    for sql in alter_sql_list:
                        # standby 文件时间戳
                        sql_unixtime = int(os.path.splitext(sql)[0].split("-")[-3])
                        # 导入先执行
                        if sql_unixtime > standby_unixtime:
                            # 导入
                            self.command_load(
                                db=db,
                                table=table,
                                data_file=archive,
                                pro_root=HIVE_HOME
                            )
                            # 删除
                            if not self.exec_hive_sql_file(
                                    sql_file=sql,
                                    data_root=ROOT_DIR,
                                    pro_root=HIVE_HOME
                            ):
                                return False
                        else:
                            # 删除
                            if not self.exec_hive_sql_file(
                                    sql_file=sql,
                                    data_root=ROOT_DIR,
                                    pro_root=HIVE_HOME
                            ):
                                return False
                            # 导入
                            self.command_load(
                                db=db,
                                table=table,
                                data_file=archive,
                                pro_root=HIVE_HOME
                            )
