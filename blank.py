import logging.config
logging.config.fileConfig("conf/mylog.conf")
logger = logging.getLogger("bank")
logger.debug("debug")
i=("root", 1133)
logger.info(i)