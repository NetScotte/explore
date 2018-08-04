# -*- coding: utf-8 -*-
"""
功能：测试mysql的功能
设计：
备注：
时间：
"""
import unittest
from commons.database_operation import Mysql


class TestMysql(unittest.TestCase):
    def test__init(self):
        m = Mysql()
        self.assertTrue(m)
        m.close()

    def test_execute_one(self):
        cases = [
            "select * from user;",
            "insert into user(id, name, age, gender, asset, datetime) values('00000000000','OSRIG', 7, 'M', "
            "225248.71, '2018-07-14')"
            "update user set age = 11 where id='00000000000';"
            "delete user where id = '00000000000'"
        ]
        m = Mysql()
        self.assertTrue(isinstance(m.execute_one(cases[0]), list))
        self.assertEqual(m.execute_one(cases[1]), 1)
        m.close()

    def test_execute_many(self):
        cases = [
            "select * from user;",
            "insert into user(id, name, age, gender, asset, datetime) values('00000000000','OSRIG', 7, 'M', "
            "225248.71, '2018-07-14')"
            "update user set age = 11 where id='00000000000';"
            "delete user where id = '00000000000'"
        ]
        m = Mysql()
        self.assertTrue(m.excute_many(cases))
        m.close()

    def test_excute_query(self):
        cases = [
            "select * from user;",
            "insert into user(id, name, age, gender, asset, datetime) values('00000000000','OSRIG', 7, 'M', "
            "225248.71, '2018-07-14')"
            "update user set age = 11 where id='00000000000';"
            "delete user where id = '00000000000'"
        ]
        m = Mysql()
        self.assertTrue(m.execute_query(cases[0]))
        m.close()

    def test_excute_with_params(self):
        m = Mysql()
        self.assertTrue(isinstance(m.excute_with_params(sql="select count(*) from user where gender=%s", value=['M']), list))
        m.close()

if __name__ == "__main__":
    unittest.main()
