# -*- coding: utf-8 -*-
# 用户类
# 功能：创建用户， 删除用户， 存钱， 取钱， 转帐， 查询
from commons.ider import *
from commons.database_operation import *


class User:
    def __init__(self):
        """
        初始化信息
        """

    def create_user(self, name, age, gender):
        """
        创建用户
        :return:
        """
        assert len(name) < 15
        assert age < 200
        assert gender in ('m', 'w')
        id = hash_obj(name)[:7] + get_chars(7)
        asset = 0.0
        info = {
            "id": id,
            "name": name,
            "age": age,
            "gender": gender,
            "asset": asset
        }
        UserTable().create_user(info=info)

    def delete_user(self, condition):
        """
        删除用户
        :info: 对象特征，如name=bob
        :return:
        """
        UserTable().delete_user(condition=condition)

    def getinfo(self):
        """
        查询信息
        :return:
        """
        pass

    def deposit(self):
        """
        存钱
        :return:
        """

    def withdrawal(self):
        """
        取钱
        :return:
        """

    def shift(self):
        """
        转账
        :return:
        """


if __name__ == "__main__":
    user = User()
    # user.create_user(name="bob", age=22, gender="m")
    user.delete_user(condition="name='bob'")
