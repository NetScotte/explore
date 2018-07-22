# -*- coding: utf-8 -*-
import random
import hashlib

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


def hash_obj(obj):
    if isinstance(obj, str):
        obj = obj.encode("utf-8")
    m = hashlib.sha256()
    m.update(obj)
    return m.hexdigest()


if __name__ == "__main__":
    print(hash_obj("hello world"))

