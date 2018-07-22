# -*- coding: utf-8 -*-
import random
import time
import hashlib
from commons.banklogger import exception_capture

@exception_capture()
def get_chars(length=None, mode=2):
    """

    :param length: 生成字符的长度，如果不指定则为返回长度为1-20
    :param mode: 0，小写，1，大写，2,混合
    :return:
    """
    if not length:
        length = random.randint(1,20)
    lc = 'abcdefghijklmnopqrstuvwxyz'
    if mode == 0:
        cs = random.choices(list(lc), k=length)
    elif mode == 1:
        cs = random.choices(list(lc.upper()), k=length)
    else:
        lc = lc + lc.upper()
        cs = random.choices(list(lc), k=length)
    return "".join(cs)


# 产生交易信息
@exception_capture()
def get_transaction(numbers):
    from commons.database_operation import Mysql
    results = Mysql().execute_query("select id from user")
    # 如果是多元组，则转换为单列表
    if isinstance(results[0], tuple):
        tmpresults = [r[0] for r in results]
        results = tmpresults

    # 产生需要的数据
    user_number = len(results)
    for i in range(numbers):
        if not results:
            raise Exception("no valid users")
        s_index = random.choice(range(user_number))
        d_index = random.choice(range(user_number))
        while d_index == s_index:
            d_index = random.choice(range(user_number))
        suser = results[s_index]
        duser = results[d_index]
        money = round(random.uniform(1, 10000), 2)
        yield suser, duser, money


# 产生用户信息
@exception_capture()
def get_user(numbers):
    assert isinstance(numbers, int), "params should be int"
    for i in range(numbers):
        user = {}
        user['name'] = get_chars()
        user['age'] = random.randint(0, 70)
        user['gender'] = random.choice(['M', 'F'])
        user['asset'] = round(random.uniform(0, 10000000), 2)
        user['datetime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(random.uniform(631123200, 1577807999)))
        yield user


@exception_capture()
def get_hash(value):
    if isinstance(value, str):
        value = value.encode("utf-8")
    myhash = hashlib.sha256()
    myhash.update(value)
    return myhash.hexdigest()


if __name__ == "__main__":
    for suser, duser, money in get_transaction(1):
        print(suser, duser, money)