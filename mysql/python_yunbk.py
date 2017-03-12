#encoding:utf-8
# -*- coding: utf-8 -*-

from yunbk import YunBK
from yunbk.backend.local import LocalBackend
from yunbk.constants import KEEPS_NORMAL
import sh
import shutil
import logging
logger = logging.getLogger('yunbk')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)
backend = LocalBackend('/backup')
with YunBK('yb', [backend], keeps=KEEPS_NORMAL) as ybk:
    # 生成文件
    with open('t.txt', 'w') as f:
        f.write('ok')
    sh.mongodump(
        u='',
        p='',
        h='192.168.0.21',
        port=27017,
        d='admin',  # 不传-d参数即备份所有库
        o='mongo_dump',
    )

    ybk.backup()