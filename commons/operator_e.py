# -*- coding: utf-8 -*-
"""
功能：执行各种操作的管理员
设计：
备注：
无论是存、取、转账的原子操作为
查询用户表，用户是否存在，查询余额
修改用户表，增加用户余额，减少用户余额，需要考虑事务或读写锁
查询交易表，主要用于统计分析
修改交易表，主要是增加记录，另外也要留一个删除记录的方法方便调试
时间：
"""


from datetime import datetime
from commons.database_operation import UserTable
from commons.database_operation import TransactionTable
from commons.data_producer import *
from commons.banklogger import *


class Operator_E:
    @exception_capture("Operator")
    def __init__(self):
        self.userTable = UserTable()
        self.transactionTable = TransactionTable()

    @exception_capture
    def query_user_asset(self, userinfo):
        """
        查询用户的信息
        :return:
        """
        if isinstance(userinfo, str):
            userlist = self.userTable.query(condition="id='{}'".format(userinfo))
        elif isinstance(userinfo, list):
            userlist = userinfo
        else:
            raise Exception("params error, should be str or list")
        if len(userlist) != 1:
            raise Exception("query error with result {}".format(userlist))
        return userlist[0][4]

    @exception_capture
    def query_record(self):
        """
        查询交易信息
        :return:
        """

    @exception_capture
    def savein(self, user, money):
        """
        存钱
        :return:
        """
        if not self.userTable.query(condition="id='{}'".format(user)):
            raise Exception("no such user id: %s" % user)
        money = self.query_user_asset(user) + money
        self.userTable.modify_user(modify={"asset": money}, query="id='{}'".format(user))
        self.transactionTable.create_transaction(suser=None, duser=user, money=money)

    @exception_capture
    def getout(self, user, money):
        """
        取钱
        :return:
        """
        userinfo = self.userTable.query(condition="id='{}'".format(user))
        if not userinfo:
            raise Exception("no such user id: %s" % user)
        if userinfo < money:
            raise Exception("余额不足")
        self.userTable.modify_user(modify={"asset": money}, query="id='{}'".format(user))
        self.transactionTable.create_transaction(suser=user, duser=None, money=money)

    # 这里不应该出现sql语句，不然那些集成和封装就没有意义了
    @exception_capture
    def transaction(self, suser, duser, money):
        """
        交易
        :param suser: 转出方id
        :param duser: 受理方id
        :param money: 交易金额
        :return:
        """
        self.getout(suser, money)
        self.savein(duser, money)
        self.transactionTable.create_transaction(suser=suser, duser=duser, money=money)

    @exception_capture
    def check_consistence(self):
        total_loss = ""
        loss_info = {}

if __name__ == "__main__":
    actionmap = {0: "query", 1: "savein", 2: "getout", 3: "transaction"}
    logger = get_logger("operator", 'debug')
    operator = Operator_E()
    for i in get_transaction(10):
        a = random.randint(0, 4)
        logger.debug("get transaction data: \n{} and action: {}".format(i, actionmap[a]))
        if a == 1:
            operator.savein(i[0], i[2])
        elif a == 2:
            operator.getout(i[1], i[2])
        elif a == 3:
            operator.transaction(*i)
        else:
            logger.debug("a ha, I just query")