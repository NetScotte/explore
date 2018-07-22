# -*- coding: utf-8 -*-
import logging


def exception_capture(text=""):
    """
    装饰器，用于异常捕获和显示
    :param func:
    :return:
    """
    def decorator(func):
        logger = logging.getLogger("bank.decorator")
        funcname = func.__name__ + " " + text
        params = []
        name_params = {}

        def wrapper(*args, **kw):
            params.extend(args)
            name_params.update(kw)
            try:
                logger.debug("execute function: %s" % funcname)
                return func(*args, **kw)
            except Exception as e:
                errorinfo = "\nfunction: {}\nparams: {} {}\nerror: {}".format(funcname, params, name_params, e)
                logger.error(errorinfo)
                exit(0)
        return wrapper

    return decorator


if __name__ == "__main__":
    pass
