# -*- coding: utf-8 -*-
"""
功能：
设计：
将用户的操作迁移到此处
这里只做了redis到mysql的同步，未做mysql到redis的同步
1. 根据指定id查询用户的资产
2. 修改用户的资产
备注：
时间：
"""
import redis
import time
import threading
from commons.mysql_operation import Mysql

class SimpleRedis:
    def __init__(self, host=None, port=None):
        self.redis = redis.StrictRedis(host=host, port=port)
        self.database = Mysql()

    # 此处仅设置用户的资产
    # 如果检查需要查询数据库，那检查就该交给底层的数据库来做
    # 多进程的方式显示设置时立即返回
    def _set_redis_mysql(self, key, value):
        if not self._get(key):
            raise Exception("no such user, please check carefully !")
        self.redis.set(key, value)
        threading.Thread(target=self._write2mysql, args=(key, value)).start()

    def _get(self, key):
        asset = self.redis.get(key)
        if not asset:
            asset = self.database.excute_with_params("select asset from user where id = %s", (key, ))
        else:
            return asset
        if not asset:
            return None
        asset = asset[0][0]
        self.redis.set(key, asset)
        return asset

    def _write2mysql(self,key, value):
        # 假定该操作很耗时
        time.sleep(10)
        self.database.excute_with_params("update user set asset = %s where id = %s", (key, value))

    def query_user_asset(self, key):
        return self._get(key)

    def change_user_asset(self, key, value):
        return self._set_redis_mysql(key, value)

    def clear_key(self, key):
        self.redis.delete(key)


if __name__ == "__main__":
    r = SimpleRedis("192.168.1.105", port=6379)
    users = ['0083012XHpgtJz', '008ae8cCJLCVnw', '008db5cDIMOnDJ', '00b816dypWiTLi', '00d1f54pRpfzbL',
             '014608aGIwyixC', '015681fkLwioWZ', '020c3c6sdchcgj', '0285082jFILgDz', '02a9fb6ggjQacM']
    for user in users:
        starttime = time.time()
        r.change_user_asset(user, 123456)
        endtime = time.time()
        # print("query user: %s get asset: %s cost time: %s" %(user, asset, endtime-starttime))
        print("change user: %s asset cost time: %s" %(user, endtime-starttime))
        exit(0)