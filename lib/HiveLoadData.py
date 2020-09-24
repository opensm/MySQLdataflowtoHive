# -*- coding: utf-8 -*-
import sys
import os
import time
from lib.Log import RecodeLog
from .settings import *
from glob import glob


class RecordData:
    def __init__(self):
        # 不存在目录变量直接退出
        assert ROOT_DIR
        assert OFFSET_TIME
        # 创建目录
        if not os.path.exists(ROOT_DIR):
            os.mkdir(ROOT_DIR, 644)

    @staticmethod
    def check_record_time(archives_name, time_offset, max_size=300 * 1024 * 1024):
        """
        :param archives_name:
        :param time_offset:
        :param max_size:
        :return:
        """
        try:
            archives_unixtime = int(os.path.splitext(archives_name)[0].split("-")[-3]) / 1000000
        except Exception as error:
            RecodeLog.error("检查文件创建时间class {0},function check_record_time,失败,{1},{2},{3},{4}".format(
                "RecordData",
                archives_name,
                time_offset,
                max_size,
                error

            ))
            return False
        archives_name = os.path.join(ROOT_DIR, archives_name)
        try:
            if (time.time() - archives_unixtime) > time_offset:
                raise Exception("已经超出与设定时间，需要新生成文件！")
            size = os.path.getsize(archives_name)
            if max_size <= size:
                raise Exception("文件大小已经超过限制大小。需要生成新文件！")
            return True
        except Exception as error:
            RecodeLog.error("检查文件创建时间失败class {0},function check_record_time,失败,{1},{2},{3},{4}".format(
                "RecordData",
                archives_name,
                time_offset,
                max_size,
                error

            ))
            return False

    @staticmethod
    def check_record(archives_name):
        """
        :param archives_name:
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        return os.path.exists(archives_name)

    @staticmethod
    def check_archives_write(archives_name):
        """
        :param archives_name:
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        return os.access(archives_name, os.W_OK)

    @staticmethod
    def check_archives_read(archives_name):
        """
        :param archives_name:
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        return os.access(archives_name, os.R_OK)

    @staticmethod
    def touch_record(archives_name):
        """
        :param archives_name:
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        try:
            with open(archives_name, 'w+') as f:
                f.close()
                return archives_name
        except Exception as error:
            RecodeLog.error("生成文件 class {0},function touch_record,失败,{1},{2}".format(
                "RecordData",
                archives_name,
                error
            ))
            return False

    @staticmethod
    def read_record(archives_name):
        """
        :param archives_name:
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        try:
            with open(archives_name, 'r') as f:
                return f.readlines()
        except Exception as error:
            RecodeLog.error("读取文件 class {0},function read_record,失败,{1},{2}".format(
                "RecordData",
                archives_name,
                error
            ))
            return False

    @staticmethod
    def write_record(archives_name, data):
        """
        :param archives_name:
        :param data:
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        try:
            with open(archives_name, "a") as f:
                f.write("{0}\n".format(data))
                f.close()
                return True
        except Exception as error:
            RecodeLog.error("写入文件 class {0},function write_record,失败,{1},{2},{3}".format(
                "RecordData",
                archives_name,
                data,
                error
            ))
            return False

    @staticmethod
    def rename_record(archives_name, new_archives):
        """
        :param archives_name:
        :param new_archives
        :return:
        """
        archives_name = os.path.join(ROOT_DIR, archives_name)
        new_archives = os.path.join(ROOT_DIR, new_archives)
        try:
            os.rename(archives_name, new_archives)
            RecodeLog.info("重命名文件 class {0},function rename_record,成功,befor={1},after={2}".format(
                "RecordData",
                archives_name,
                new_archives
            ))
            return True
        except Exception as error:
            RecodeLog.error("重命名文件 class {0},function rename_record,失败,befor={1},after={2},{3}".format(
                "RecordData",
                archives_name,
                new_archives,
                error
            ))
            return False

    @staticmethod
    def check_record_line(archives_name):
        """
        :param archives_name:
        :return:
        """
        pass

    @staticmethod
    def get_list_record(archives_regular):
        """
        :param archives_regular:
        :return:
        """
        archives_regular = os.path.join(ROOT_DIR, archives_regular)
        return glob(archives_regular)

    def make_create_table_data(self, db, table, data):
        """
        :param db:
        :param table:
        :param data:
        :return:
        """
        archives_name = "CreateTableData-{0}-{1}.dict".format(db, table)
        if os.path.exists(os.path.join(ROOT_DIR, archives_name)):
            return True
        if not self.touch_record(archives_name=archives_name):
            return False
        if not self.check_archives_write(archives_name=archives_name):
            return False
        return self.write_record(archives_name=archives_name, data=data)

    @classmethod
    def read_create_table_data(cls, db, table):
        """
        :param db:
        :param table:
        :return:
        """
        archives_name = "CreateTableData-{0}-{1}.dict".format(db, table)
        if not cls.check_archives_read(archives_name=archives_name):
            return False
        result = cls.read_record(archives_name=archives_name)
        if result:
            return result[0].replace("'", '"')
        else:
            return False

    def check_rename_pre(self, archives_name):
        """
        :param archives_name:
        :return:
        """
        pass

    def service_record_table(self, data):
        """
        :param data:
        :return:
        """
        error_archives = "{0}.error".format(
            time.strftime("%Y%m%d%H", time.localtime())
        )

        if not isinstance(data, dict):
            RecodeLog.error("产生表结构数据 class {0},function service_record_table,失败,输入参数异常，请检查，{1}".format(
                "RecordData",
                data
            ))
            return False
        if data['type'] != "INSERT":
            return True
        # if data['type'] not in ("INSERT", "UPDATE", "DELETE", "ERASE", "ALTER"):
        #     return True

        database = data['database']
        table = data['table']
        prepare_name = "{0}-{3}-{1}-{2}.pre".format(
            time.strftime("%Y%m%d%H%M", time.localtime()),
            database,
            table,
            int(time.time() * 1000000),
        )

        # 处理 DROP ALERT TABLE
        if data['type'] in ['ALTER', "ERASE"]:

            sql = "ALTER TABLE `{0}`.`{1}` RENAME TO `{0}`.`{1}_{2}`".format(
                database,
                table,
                time.strftime("%Y%m%d%H%M%S", time.localtime())
            )
            archives_name = "{2}TableSQL-{3}-{0}-{1}.sql".format(
                database,
                table,
                data['type'],
                int(time.time() * 1000000)  # 生成检查时间字段
            )
            # in
            if not self.touch_record(archives_name=archives_name):
                RecodeLog.error("创建 {2} 失败 class {0},function service_record_table，请检查，{1}".format(
                    "RecordData",
                    sql,
                    archives_name
                ))
                return False
            if not self.write_record(archives_name=archives_name, data=sql):
                RecodeLog.error("写入 {2} 失败 class {0},function service_record_table，请检查，{1}".format(
                    "RecordData",
                    sql,
                    archives_name
                ))
                return False

            prepare_first_list_alert = self.get_list_record(
                archives_regular="*-{0}-{1}.pre".format(
                    database,
                    table
                )
            )

            # 强制修改pre文件为standby
            if len(prepare_first_list_alert) != 0:
                for i in prepare_first_list_alert:
                    # 将文件重命名为 准备状态
                    archives = os.path.splitext(i)[0]
                    new_name = "{0}.standby".format(archives)
                    self.rename_record(archives_name=i, new_archives=new_name)
            return True

        # 产生表结构文件
        if not self.make_create_table_data(db=database, table=table, data=data['mysqlType']):
            RecodeLog.error("产生表结构数据 class {0},function service_record_table，请检查，{1}".format(
                "RecordData",
                str(data['mysqlType'])
            ))
            self.write_record(archives_name=error_archives, data=data)
        # 以下为检查文件并按照需求生成新的pre文件
        # 先获取数据目录下文件列表
        prepare_first_list = self.get_list_record(archives_regular="*.pre")
        if len(prepare_first_list) == 0:
            self.touch_record(archives_name=prepare_name)
            RecodeLog.info("即将生成pre文件 class {0},function service_record_table,archives_name={1}".format(
                self.__class__.__name__,
                prepare_name
            ))
        else:
            for i in prepare_first_list:
                if not self.check_record_time(
                        archives_name=i,
                        time_offset=OFFSET_TIME,
                        max_size=OFFSET_SIZE
                ):
                    # 将文件重命名为 准备状态
                    archives = os.path.splitext(i)[0]
                    new_name = "{0}.standby".format(archives)
                    self.rename_record(archives_name=i, new_archives=new_name)

        # 再次获取可以写入的文件名
        prepare_list = self.get_list_record(archives_regular="*.pre")
        if len(prepare_list) > 0:
            prepare_archive = prepare_list[0]
        else:
            # 执行异常将数据写入错误文件中
            # 产生一个新文件
            prepare_archive = self.touch_record(archives_name=prepare_name)
            if not prepare_archive:
                self.write_record(archives_name=error_archives, data=data)
                return False

        if not self.check_archives_write(archives_name=prepare_archive):
            # 执行异常将数据写入错误文件中
            self.write_record(archives_name=error_archives, data=data)
            return False

        # 写入数据到文件
        if data['data'] == 'null' or not data['data']:
            self.write_record(archives_name=error_archives, data=str(data))
            return False
        for x in range(0, len(data['data'])):
            # 以下为处理 DELETE,UPDATE,INSERT 三个事务的操作
            new_value = data['data'][x]
            new_value_str = ",".join('%s' % x for x in new_value.values())
            if data['type'] == "DELETE":
                record_line = "{0},-1".format(new_value_str)
            elif data['type'] == "INSERT":
                # record_line = "{0},1".format(new_value_str)
                record_line = new_value_str
            else:
                old_value = data['old'][x]
                record_line = "{0},1".format(new_value_str)
                for key, value in old_value.items():
                    new_value[key] = value
                new_value = "{0},-1".format(
                    ",".join(new_value.values())
                )
                record_line = "{0}\n{1}".format(record_line, new_value)
            self.write_record(archives_name=prepare_archive, data=record_line)
