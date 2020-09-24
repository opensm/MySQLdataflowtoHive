import time

ddd = {"ddl": "dd", "ddl12": "ss"}


def produce_name(data, default_name="ddl"):
    """
    :param data:
    :param default_name:
    :return:
    """
    if not isinstance(data, dict):
        return False
    if default_name in data.keys():
        result = "{0}{1}".format(default_name, time.strftime("%S", time.localtime()))
        return produce_name(data=data, default_name=result)
    else:
        return default_name


print(produce_name(ddd))
