# -*- coding: utf-8 -*-
"""
功能：
设计：
备注：
时间：
"""
import mysql.connector
import logging
from commons import loadfile


class Mysql:
    def __init__(self):
        self.logger = logging.getLogger("data.mysql")
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
        rowcount = cursor.rowcount
        cursor.close()
        connection.close()
        return rowcount

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


    def parser_query(self, query):
        for i in ['<', '=', ">"]:
            if i in query:
                return query.partition(i)
        return query

    def close(self):
        self.logger.info("close database connections")
        self.connections.close()