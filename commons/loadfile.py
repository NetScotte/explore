# -*- coding: utf-8 -*-
import os
import yaml


# 获取文件内容
def getfile(filename, filepath=None):
    """
    获取文件内容
    :param filename: 文件名，可以为文件路径
    :param filepath: 文件路径，默认空
    :return:
    """
    if not os.path.exists(filename) or not filepath:
        filepath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'conf')
    filename = os.path.join(filepath, filename)
    if not os.path.exists(filename):
        raise Exception("no such file %s" % filename)
    with open(filename, 'r') as f:
        filecontent = f.read()
    return filecontent


# 从yaml文件中获取配置信息
def getyaml(filename, filepath=None):
    filecontent = getfile(filename, filepath)
    return yaml.load(filecontent)


if __name__ == "__main__":
    print(getyaml("database"))