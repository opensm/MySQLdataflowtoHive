# -*- coding: utf-8 -*-
import sys
from kafka import KafkaConsumer
from lib.Log import RecodeLog
import simplejson as json


class KafkaQuery:
    def __init__(self):
        self.__consumer = None

    def blind(self, **kwargs):
        topic = kwargs.pop('topics')
        try:
            self.__consumer = KafkaConsumer(topic, **kwargs)
            RecodeLog.info("卡夫卡初始化成功，{0}成功".format(
                json.dumps(kwargs)
            ))
        except Exception as error:
            RecodeLog.info("卡夫卡初始化失败,function blind,{0},{1}".format(
                json.dumps(kwargs),
                str(error)
            ))
            sys.exit(1)

    def run(self, exec_function):
        """
        :param exec_function:
        :return:
        """
        if not self.__consumer:
            RecodeLog.info("没完成初始化，监听失败")
        for msg in self.__consumer:
            value = json.loads(msg.value)
            if value['type'] == "QUERY":
                continue
            exec_function(data=value)
