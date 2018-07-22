# -*- coding: utf-8 -*-
import memcache
mc = memcache.Client(['192.168.1.105:13131'], debug=True)
mc.set("user:19", "Simple is better than complex")
print(mc.get("user:19"))