# -*- coding: utf-8 -*-
import logging


def get_logger(logname, loglevel, logout=None):
    format_str = '[%(asctime)s %(filename)s line: %(lineno)d %(funcName)s %(levelname)s] %(message)s'
    logmap = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARN, 'error': logging.ERROR}
    loglevel = logmap.get(loglevel)
    if logout:
        logging.basicConfig(format=format_str, filename=logout, level=loglevel)
    else:
        logging.basicConfig(format=format_str, level=loglevel)
    return logging.getLogger(logname)


def exception_capture(text=""):
    """
    装饰器，用于异常捕获和显示
    :param func:
    :return:
    """
    def decorator(func):
        logger = get_logger("catchException", 'debug')
        funcname = func.__name__ + " " + text
        params = []
        name_params = {}

        def wrapper(*args, **kw):
            params.extend(args)
            name_params.update(kw)
            try:
                logger.info("execute function: %s" % funcname)
                return func(*args, **kw)
            except Exception as e:
                errorinfo = "\nfunction: {}\nparams: {} {}\nerror: {}".format(funcname, params, name_params, e)
                logger.error(errorinfo)
                exit(0)
        return wrapper

    return decorator


if __name__ == "__main__":
    logger = get_logger("test", 'info')
    logger.debug("hello")
