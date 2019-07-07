import logging
from colorlog import ColoredFormatter


def init_logger():
    """
    Configures a colored logging
    """
    LOG_LEVEL = logging.INFO
    LOGFORMAT = "%(log_color)s%(levelname)-1s: %(log_color)s%(message)s"
    logging.root.setLevel(LOG_LEVEL)
    formatter = ColoredFormatter(LOGFORMAT)
    stream = logging.StreamHandler()
    stream.setLevel(LOG_LEVEL)
    stream.setFormatter(formatter)
    log = logging.getLogger('pythonConfig')
    log.setLevel(LOG_LEVEL)
    log.addHandler(stream)
    return log


class Logger(object):
    log = init_logger()
