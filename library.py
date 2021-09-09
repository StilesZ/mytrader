#  -*- coding:utf-8 -*-
import os
import zlib
import base64
import logging
import logging.handlers


def get_logger(log_name, file):
    # 创建文件夹
    # if not os.path.exists(file):
    #     os.mkdir(file)
    logger = logging.getLogger(log_name)
    if not logger.handlers:  # 这里进行判断，如果logger.handlers列表为空，则添加，否则，直接去写日志
        logger.setLevel(level=logging.INFO)
        handler = logging.handlers.TimedRotatingFileHandler(filename=file, when='MIDNIGHT', interval=1, backupCount=0, encoding='utf-8')
        formatter = logging.Formatter(fmt='[ %(asctime)s ] : %(filename)s[line:%(lineno)d] : %(levelname)s : %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def convert_single_to_double(s):
    symbol = ["'"]
    result = []
    for _index, each in enumerate(s):
        if each in symbol:
            result.append('"')
        else:
            result.append(each)
    return ''.join(result)


def base64_encode(password) -> bytes:
    if isinstance(password, str):
        password = password.encode('utf-8')
    return base64.b64encode(password)


def base64_decode(password) -> bytes:
    if isinstance(password, str):
        password = password.encode('utf-8')
    return base64.b64decode(password)


def compress_string(password):
    if isinstance(password, str):
        password = password.encode('utf-8')
    return zlib.compress(password)


def decompress_string(password):
    if isinstance(password, str):
        password = password.encode('utf-8')
    return zlib.decompress(password)
