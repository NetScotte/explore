# -*- coding: utf-8 -*-
import mysql.connector
from datetime import datetime

from commons import banklogger, loadfile
from commons.data_producer import *
import logging


class Mysql:
    @banklogger.exception_capture("mysql")
    def __init__(self):
        self.logger = logging.getLogger("bank.mysql")
        self.pool_name = 'mysql'
        self._getconfig()
        self.logger.debug("connect to database {host} {user} {database}".format(**self.config))
        self.connections = mysql.connector.connect(pool_name=self.pool_name, pool_size=5, **self.config)

    def _get_connect(self):
        self.logger.debug("get connection from connection pool id: %s" % self.connections.connection_id)
        return mysql.connector.connect(pool_name=self.pool_name)

    def _getconfig(self, configname="database"):
        self.config = loadfile.getyaml(configname)
        self.logger.debug("get connect params from configfile: {}".format(configname))

    @banklogger.exception_capture()
    def execute_one(self, sql):
        if not isinstance(sql, str):
            raise Exception("error params[sql] type: {} value: {}".format(type(sql), sql))
        if sql.startswith("select"):
            return self.execute_query(sql)
        connection = self._get_connect()
        cursor = connection.cursor()
        self.logger.debug("execute sql: {}".format(sql))
        cursor.execute(sql)
        self.logger.debug("fetchone result {}".format(cursor.fetchone()))
        connection.commit()
        cursor.close()
        connection.close()

    @banklogger.exception_capture()
    def execute_query(self, sql):
        if not isinstance(sql, str):
            raise Exception("error params[sql] type: {} value: {}".format(type(sql), sql))
        connection = self._get_connect()
        cursor = connection.cursor()
        self.logger.debug("execute sql: {}".format(sql))
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) < 10:
            self.logger.debug("fetchall result {}".format(results))
        else:
            self.logger.debug("fetchall result {}.... which total {}".format(results[:10], len(results)))
        cursor.close()
        connection.close()
        return results

    @banklogger.exception_capture()
    def excute_many(self, sql):
        if not isinstance(sql, list):
            raise Exception("error params[sql] type: {} value: {}".format(type(sql), sql))
        connection = self._get_connect()
        cursor = connection.cursor()
        try:
            for s in sql:
                self.logger.debug("excute sql: {}".format(s))
                cursor.execute(s)
            connection.commit()
        except Exception as e:
            self.logger.error(e)
            connection.rollback()
            exit(1)
        finally:
            cursor.close()
            connection.close()

    @banklogger.exception_capture()
    def excute_with_params(self, sql, value):
        self.logger.debug("get params sql: {} values: {}".format(sql, value))
        connection = self._get_connect()
        cursor = connection.cursor()
        cursor.execute(sql, value)
        if sql.startswith("select"):
            result = cursor.fetchall()
        else:
            result = cursor.rowcount
            connection.commit()
        cursor.close()
        connection.close()
        return result

    @banklogger.exception_capture()
    def parser_query(self, query):
        for i in ['<', '=', ">"]:
            if i in query:
                return query.partition(i)
        return query

    def close(self):
        self.logger.info("close database connections")
        self.connections.close()


class Table(Mysql):
    @banklogger.exception_capture("table")
    def __init__(self, tablename=None):
        super().__init__()
        self.tablename = tablename
        self.logger = logging.getLogger("bank.Table")

    @banklogger.exception_capture()
    def create_table(self, tablename, tableinfo):
        """
        创建表
        :return:
        """
        if isinstance(tableinfo, list):
            tableinfo = ",\n".join(tableinfo)
        sqlcommand = """
        create table if not exists {tablename}(
        {tableinfo}
        );
        """.format(tablename=tablename, tableinfo=tableinfo)
        self.logger.info("create table %s" % tablename)
        self.execute_one(sqlcommand)

    @banklogger.exception_capture()
    def delete_table(self, tablename):
        """
        删除表
        :return:
        """
        sqlcommand = """
        drop table if exists {tablename}
        """.format(tablename=tablename)
        self.logger.warn("delete table %s" % tablename)
        self.execute_one(sqlcommand)

    @banklogger.exception_capture()
    def query_ins(self, tablename, result="*", condition=None, customer=None):
        if customer and customer.lower().startswith("select"):
            self.logger.info("execute customer query %s")
            return self.execute_query(customer)

        sqlcommand = """
        select {result} from {tablename}
        """.format(result=result, tablename=tablename)

        if condition:
            sqlcommand += " where {condition}".format(condition=condition)
        self.logger.debug("execute query %s" % sqlcommand)
        return self.execute_query(sqlcommand)

    @banklogger.exception_capture()
    def create_ins(self, tablename, tableattr, insvalue):
        """
        创建实例
        :return:
        """
        assert isinstance(insvalue, tuple)
        sqlcommand = """
        insert into {tablename} ({tableattr}) values {insvalue}
        """.format(tablename=tablename, tableattr=tableattr, insvalue=insvalue )
        self.logger.debug("create instance {}".format(sqlcommand))
        self.execute_one(sqlcommand)

    @banklogger.exception_capture()
    def delete_ins(self, tablename, attr, insvalue):
        """
        删除实例
        :return:
        """
        if isinstance(attr, list):
            attr = ",".join(attr)
        assert isinstance(tablename and attr, str)
        sqlcommand = """
        delete from {tablename} where {attr}='{insvalue}'
        """.format(tablename=tablename, attr=attr, insvalue=insvalue)
        self.logger.warn("delete instance %s" % sqlcommand)
        self.execute_one(sqlcommand)

    @banklogger.exception_capture()
    def modify_ins(self, tablename, mattr, mvalue, qattr, qvalue):
        """
        修改实例
        :return:
        """
        assert isinstance(tablename and mattr and qattr, str)
        sqlcommand = """
        update {tablename} set {mattr}={mvalue} where {qattr}={qvalue}
        """.format(tablename=tablename, mattr=mattr, mvalue=mvalue,
                   qattr=qattr, qvalue=qvalue)
        self.logger.debug("modify instance %s" % sqlcommand)
        self.execute_one(sqlcommand)

    @banklogger.exception_capture()
    def set_lock(self, tablename):
        """
        设置锁
        :return:
        """
        self.logger.debug("set write lock")
        self.execute_one("lock tables {tablename} write" % tablename)

    @banklogger.exception_capture()
    def release_lock(self):
        """
        释放锁
        :return:
        """
        self.logger.debug("release table lock")
        self.execute_one("unlock tables")


class UserTable(Table):
    @banklogger.exception_capture("userTable")
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("bank.UserTable")
        self.tablename = 'user'
        self.tableattr= ("id", "name", "age", "gender", "asset", "datetime")

    @banklogger.exception_capture()
    def create_user_table(self):
        """
        创建用户表
        id 14位字符，7位姓名hash, 7位随机数，
        name 姓名
        age 年龄
        gender 性别，m表示男人，w表示女人
        asset 资产
        datetime 用户创建时间
        :return:
        """
        table_creater = [
            "id char(14) not null primary key",
            "name varchar(30) not null",
            "age int",
            "gender enum('M', 'F')",
            "asset double not null",
            "datetime date not null",
        ]
        self.logger.info("create user table \n%s" % table_creater)
        self.create_table(self.tablename, table_creater)

    @banklogger.exception_capture()
    def delete_user_table(self):
        """
        删除用户表
        :return:
        """
        self.logger.warn("delete table: %s" % self.tablename)
        self.execute_one("drop table if exists user")

    @banklogger.exception_capture()
    def query(self, condition=None, result="*", customer=False):
        """
        查询对象
        :param condition: 如果customer为true,那么condition为sql语句，否则为字典
        :param result: 查询结果属性，默认*
        :param customer: 是否自定义查询，输入完整的sql查询语句
        :return: queryresult
        """
        if customer and condition.lower().startswith("select"):
            queryresult = self.execute_one(condition)
        else:
            queryresult = self.query_ins(result=result, tablename=self.tablename, condition=condition)
        return queryresult

    @banklogger.exception_capture()
    def create_user(self, info):
        """
        创建用户
        :return:
        """
        assert isinstance(info, dict)
        userid = self._get_nameid(info['name'])
        userinfo = (userid, info.get("name", None), info.get("age", None),
                    info.get("gender", None), info.get("asset", None), info.get("datetime", ""))
        self.logger.debug("create user {}".format(userinfo))
        self.create_ins(tablename=self.tablename, tableattr=", ".join(self.tableattr), insvalue=userinfo)

    @banklogger.exception_capture()
    def delete_user(self, **kwargs):
        """
        删除用户
        :return:
        """
        result = "id"
        target = self.query(result=result, **kwargs)
        if len(target) == 0:
            raise Exception("no such user")
        target = [ins[0] for ins in target]
        self.logger.warn("get user number {}, they are {}".format(len(target), target))
        for userid in target:
            self.delete_ins(tablename=self.tablename, attr='id', insvalue=userid)

    @banklogger.exception_capture()
    def modify_user(self, modify, query):
        """
        修改信息,根据一个条件改变一项
        :return:
        """
        assert isinstance(modify, tuple), "修改信息需要以键值对的形式出现"
        query = self.parser_query(query)
        assert isinstance(query, tuple) and len(query) == 3
        values = [modify[1], query[2]]
        modify_command = "update user set {modifyitem}=%s where {condition}{relation}%s".format(
            modifyitem=modify[0], condition=query[0], relation=query[1])
        self.excute_with_params(sql=modify_command, value=values)

    @banklogger.exception_capture()
    def _get_nameid(self, name):
        return get_hash(name)[:7] + get_chars(7)


class TransactionTable(Table):
    @banklogger.exception_capture("transactionTable")
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("bank.TransactionTable")
        self.tablename = 'transaction'
        self.tableattr= ("id", "type", "suser", "duser", "money", "datetime")

    @banklogger.exception_capture()
    def create_transaction_table(self):
        """
        创建交易表
        id 时间序列+4位随机数
        type: 存0，取1，转2
        suser 转出方
        duser 受理方
        monery 交易金额
        datetime 交易时间
        :return:
        """
        self.tableinfo = [
            "id char(18) not null primary key",
            "type int(1) not null",
            "suser char(14)",
            "duser char(14)",
            "money double not null",
            "datetime datetime not null",
            "constraint Fk_suserid_id foreign key(suser) references user(id) on delete cascade on update cascade",
            "constraint Fk_duserid_id foreign key(duser) references user(id) on delete cascade on update cascade",
        ]
        self.create_table(tablename=self.tablename, tableinfo=self.tableinfo)

    @banklogger.exception_capture()
    def delete_transaction_table(self):
        """
        删除交易表
        :return:
        """
        self.delete_table(tablename=self.tablename)

    @banklogger.exception_capture()
    def query(self, info, result="*", customer=False):
        """
        查询对象
        :param info: 如果customer为true,那么info为sql语句，否则为字典
        :param result: 查询结果属性，默认*
        :return: queryresult
        """
        if customer and isinstance(info, str):
            return self.query_ins(info)
        elif not customer and isinstance(info, dict):
            condition = ""
            for attr, value in info.items():
                condition += attr + str(value) + " and "
            condition = condition[:-5]
            return self.query_ins(result=result, tablename=self.tablename, condition=condition)
        else:
            raise Exception("params error")

    @banklogger.exception_capture()
    def create_transaction(self, suser, duser, money):
        """
        创建交易记录
        :return:
        """
        timenow = datetime.now()
        numberlist = [str(i) for i in range(10)]
        timeobj = timenow.strftime('%Y-%m-%d %H:%M:%S')
        transactionid = timenow.strftime("%Y%m%d%H%M%S") + "".join(random.choices(numberlist, k=4))
        # type 存0，取1，转2
        # 没有转出方，就是存钱
        if suser and duser:
            stype = 2
            self.excute_with_params("insert into {}(id, type, suser, duser, money, datetime) values(%s, %s, %s, %s, %s, %s);".format(
                    self.tablename), value=(transactionid, stype, suser, duser, money, timeobj))
        elif suser:
            stype = 1
            self.excute_with_params("insert into {}(id, type, suser, money, datetime) values(%s, %s, %s, %s, %s);".format(
                    self.tablename), value=(transactionid, stype, suser, money, timeobj))
        # 没有受理方，就是取钱
        elif duser:
            stype = 0
            self.excute_with_params("insert into {}(id, type, duser, money, datetime) values(%s, %s, %s, %s, %s);".format(
                    self.tablename), value=(transactionid, stype, duser, money, timeobj))
        else:
            raise Exception("not exists suser and duser")

    # 交易信息不应该被删除
    @banklogger.exception_capture()
    def delete_user(self, **kwargs):
        """
        删除用户
        :return:
        """
        target = self.query(result="id", **kwargs)
        self.delete_ins(tablename=self.tablename, attr='id', insvalue=target)

    # 交易信息不应该不修改
    @banklogger.exception_capture()
    def modify_transaction(self, mattr, mvalue, **kwargs):
        """
        修改信息
        :return:
        """
        target = self.query(result='id', **kwargs)
        self.modify_ins(tablename=self.tablename, mattr=mattr, mvalue=mvalue, qattr='id', qvalue=target)


if __name__ == "__main__":
    # 测试
    # 删除user表
    # userTable = User()
    # print(userTable.query(condition="id='a061c1fsRrAtTe'"))
    # operator = Operator()
    # operator.transaction(suser="", duser="", money=1000)
    # for user in get_user(1):
    #     userTable.create_user(user)
    # transactionTable = TransactionTable()
    # # 删除transaction表
    # transactionTable.delete_transaction_table()
    # userTable.delete_user_table()
    # # 创建user表
    # userTable.create_user_table()
    # # 创建transaction表
    # transactionTable.create_transaction_table()
    m = Mysql()
    userinfo = m.execute_query("select id, asset from user")
    sql = []
    for user in userinfo:
        sql.append("update user set asset=%s where id='%s';" % (round(user[1], 2), user[0]))
    m.excute_many(sql)





