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
from commons.database_operation import UserTable
from commons.database_operation import TransactionTable
from commons.data_producer import *
from commons.banklogger import *
import logging.config
import os
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Operator_E:
    @exception_capture("Operator")
    def __init__(self):
        self.userTable = UserTable()
        self.transactionTable = TransactionTable()
        self.initial_user_asset = self.get_user_asset()
        self.transactionmap = {}

    @exception_capture()
    def get_user_asset(self):
        user_asset = {}
        user_asset_list = self.userTable.query(result="id, asset")
        for user in user_asset_list:
            user_asset[user[0]] = user[1]
        return user_asset

    @exception_capture()
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

    @exception_capture()
    def query_record(self):
        """
        查询交易信息
        :return:
        """

    @exception_capture()
    def savein(self, user, money, transaction=True):
        """
        存钱
        :return:
        """
        assert isinstance(money, (float, int))
        userinfo = self.userTable.query(condition="id='{}'".format(user))
        if not userinfo:
            raise Exception("no such user id: %s" % user)

        self.userTable.modify_user(modify=("asset", userinfo[0][4] + money), query="id={}".format(user))
        if transaction:
            self.transactionTable.create_transaction(suser=None, duser=user, money=money)
        if user not in self.transactionmap:
            self.transactionmap[user] = +money
        else:
            self.transactionmap[user] += +money



    @exception_capture()
    def getout(self, user, money, transaction=True):
        """
        取钱
        :return:
        """
        userinfo = self.userTable.query(condition="id='{}'".format(user))
        if not userinfo:
            raise Exception("no such user id: %s" % user)
        if userinfo[0][4] < money:
            raise Exception("余额不足")
        self.userTable.modify_user(modify=("asset", userinfo[0][4] - money), query="id={}".format(user))
        if transaction:
            self.transactionTable.create_transaction(suser=user, duser=None, money=money)
        if user not in self.transactionmap:
            self.transactionmap[user] = round(-money, 2)
        else:
            self.transactionmap[user] += round(-money, 2)

    # 这里不应该出现sql语句，不然那些集成和封装就没有意义了
    @exception_capture()
    def transaction(self, suser, duser, money):
        """
        交易
        :param suser: 转出方id
        :param duser: 受理方id
        :param money: 交易金额
        :return:
        """
        self.getout(suser, money, transaction=False)
        self.savein(duser, money, transaction=False)
        self.transactionTable.create_transaction(suser=suser, duser=duser, money=money)

    @exception_capture()
    def check_consistence(self):
        initial_asset = 0
        for asset in self.initial_user_asset.values():
            initial_asset += asset

        current_asset = 0
        current_user_asset = self.get_user_asset()
        for asset in current_user_asset.values():
            current_asset += asset
        logger.info("initial asset: %s" % initial_asset)
        logger.info("current asset: %s" % current_asset)
        logger.warning("loss list:\n")
        for user in self.transactionmap:
            shouldbe = self.initial_user_asset[user] + self.transactionmap[user]
            if shouldbe != current_user_asset[user]:
                logger.warning("loss occured on user: {} initial: {} current: {} should be {}".format(
                    user, self.initial_user_asset[user], current_user_asset[user], shouldbe))


if __name__ == "__main__":
    actionmap = {0: "query", 1: "savein", 2: "getout", 3: "transaction"}
    logging.config.fileConfig("conf/mylog.conf")
    logger = logging.getLogger("bank")
    operator = Operator_E()
    for i in get_transaction(10000):
        a = random.randint(0, 3)
        logger.info("get transaction data: \n{} and action: {}".format(i, actionmap[a]))
        if a == 1:
            logger.info("user: {name} save: {money} to bank".format(name=i[0], money=i[2]))
            operator.savein(i[0], i[2])
        elif a == 2:
            logger.info("user: {name} get: {money} out of bank".format(name=i[1], money=i[2]))
            operator.getout(i[1], i[2])
        elif a == 3:
            logger.info("user: {suser} turn: {money} into user: {duser}".format(suser=i[0], money=i[2], duser=i[1]))
            operator.transaction(*i)
        else:
            logger.info("a ha, I just query")
    operator.check_consistence()