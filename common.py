# -*- coding: utf-8 -*-
"""
@author:neo
@file: common.py
@time: 2020/01/06 13:11
@desc:common
@blog:https://blog.csdn.net/mingtiannihaoabc
"""
import subprocess
import logging
import sys
from logging.handlers import RotatingFileHandler
from concurrent_log_handler import ConcurrentRotatingFileHandler

log = logging.getLogger('filelog')
flashlog = log
if not log.handlers:
    if sys.platform != 'win32':
        fh = ConcurrentRotatingFileHandler(default_log_file, "a", logfile_maxsize, logfile_max_keep,
                                           delay=1)  # Max 50M * 50
    else:
        fh = RotatingFileHandler(default_log_file, "a", logfile_maxsize, logfile_max_keep)  # Max 50M * 50
    formatter = logging.Formatter('%(asctime)s[%(filename)s:%(lineno)d]: %(levelname)s[PID:%(process)d] - %(message)s',
                                  "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    log.addHandler(fh)


class DbfenError(Exception):
    def __init__(self, code, errmsg):
        self.code = code
        self.errmsg = errmsg
        self.is_logged = False
        message = 'error code: ' + str(code) + ', message: ' + str(errmsg)
        Exception.__init__(self, message)


def exec_cmd2(cmd):
    result = {}
    msg = ''
    errmsg = ''
    try:
        output = subprocess.PIPE
        p = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=output,
        )
        for line in p.stdout.readlines():
            msg = '{}{}'.format(msg, line.decode('utf-8', 'ignore'))
        for line in p.stderr.readlines():
            errmsg = '{}{}'.format(errmsg, line.decode('utf-8', 'ignore'))
        p.communicate()
        code = p.returncode
        if msg:
            msg = str(msg).rstrip('\n')
    except Exception as err:
        errmsg = 'exec command error: {}'.format(err)
        code = 360
        msg = errmsg
    result['ret'] = code
    result['msg'] = msg
    result['errmsg'] = errmsg
    return result


def is_win_platform():
    plfm = sys.platform
    if plfm != 'win32' and plfm != 'win64':
        return False
    else:
        return True


def error():
    try:
        import traceback
        err = sys.exc_info()[0]
        err_debug = traceback.format_exc()
        debug_msg = str(err) + ':' + str(err_debug)
        log.debug(debug_msg)
    except:
        print('log debug msg failed.')


def apply_source_and_mount(self, re_id=None, mount_path=None, backup_mode=None):
    """:arg"""


def umount_device(path):
    """:arg"""
