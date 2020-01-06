# -*- coding: utf-8 -*-
"""
@author:neo
@file: hana_backup_recovery.py
@time: 2020/01/06 13:11
@desc:backup and recovery
@blog:https://blog.csdn.net/mingtiannihaoabc
"""
import os
import json
import datetime
import shutil
from hana_base import HanaLinuxBase, HanaWindowBase
from common import exec_cmd2, log, DbfenError, is_win_platform, error, apply_source_and_mount, umount_device


class HanaLinuxBackup(HanaLinuxBase):
    """
    数据库备份
    """

    def __init__(self, params):
        super(HanaLinuxBackup, self).__init__(params)
        self.backup_mode = params.get('backup_mode', '')
        self.backup_dir = params.get('backup_dir', '')
        self.get_idx_path = params.get('get_idx_path', '')

    # Get the id of the last full backup of a database
    def get_hana_last_fullback_backup_id(self):
        sql_cmd = r'select BACKUP_ID from sys.M_BACKUP_CATALOG where ENTRY_TYPE_NAME=\'complete data backup\' and ' \
                  r'STATE_NAME = \'successful\' order by SYS_START_TIME desc limit 1'
        user, passpwd = (self.tenant_user, self.tenant_passwd) if self.target_db.upper() != 'SYSTEMDB' else \
            (self.system_db_user, self.system_db_pwd)
        exec_command = r'su - {} -c "{} -n localhost -i {} -d {} -u {} -p \"{}\" -x {}"'.format(self.hana_adm,
                                                                                                self.hdb_sql,
                                                                                                self.instance,
                                                                                                self.target_db,
                                                                                                user,
                                                                                                passpwd,
                                                                                                sql_cmd)
        result = exec_cmd2(exec_command)
        exec_command_log = exec_command.replace(r'-p \"{}\"'.format(self.system_db_pwd), '-p ******')
        log.info('exec_command:{},{}'.format(result, exec_command_log))
        if result['ret'] != 0:
            raise DbfenError(20120077, result['msg'] + result['errmsg'])
        else:
            msg_info = result['msg'].split(os.linesep)
            # 0 Indicates that a full backup of the database has not been done before
            last_fullback_backup_id = msg_info[1] if len(msg_info) == 2 else 0
            return last_fullback_backup_id

    # Generate a full backup of hana
    def gen_hana_fullback_info_file(self):
        config = getconf()
        db_index_file = self.get_idx_path(self.task_id, self.task_type, config.IDX_EXTRA_STR_EMPTY, True)
        if not os.path.exists(db_index_file):
            log.error('[TASK_ID:' + str(self.task_id) + '] gen_hana_fullback_info_file fail! db_index_file not exists!')
        db_index_dir = os.path.dirname(db_index_file)
        hana_fullback_info_file = db_index_dir + '/hana_fullback_info.json'
        full_back_info = {self.target_db.upper(): self.get_hana_last_fullback_backup_id()}
        with open(hana_fullback_info_file, 'w') as f:
            json.dump(full_back_info, f)

    def backup_redo_log(self, save_path):
        dir_log_source = 'SYSTEMDB' if self.target_db.upper() == 'SYSTEMDB' else 'DB_{}'.format(self.target_db.upper())
        default_log_path = '{}/backup/log/{}'.format(self.dir_instance, dir_log_source)
        cp_redo_log = "su - {} -c 'cp {}/* {}'".format(self.hana_adm, default_log_path, save_path)
        try_times = 3
        try:
            while try_times <= 3:
                result = exec_cmd2(cp_redo_log)
                status = result['ret']
                output = result['msg'].strip()
                if status == 0:
                    return True
                else:
                    log.warn('[TASK_ID:' + str(self.task_id) + '] backup_redo_log cmd fail! dump_cmd:' + cp_redo_log
                             + 'status:' + str(status) + ' output:' + output)
                    exec_cmd2('rm -rf {}/log_*'.format(self.backup_dir))
            return False
        except Exception as ex:
            error()
            log.error('[backup_redo_log]' + str(type(ex)) + ":" + str(ex))
            return False

    def full_backup_clear_old_log(self):
        dir_log_source = 'SYSTEMDB' if self.target_db.upper() == 'SYSTEMDB' else 'DB_{}'.format(self.target_db.upper())
        default_log_path = '{}/backup/log/{}'.format(self.dir_instance, dir_log_source)
        if os.path.isdir(default_log_path):
            shutil.rmtree(default_log_path)

    def hana_db_backup(self):
        try:
            config = getconf()
            try_times = 0
            save_path = os.path.join(self.backup_dir, self.target_db)
            if self.backup_mode == config.DB_BACKUP_TYPE_FULL:
                self.full_backup_clear_old_log()
                backup_command = r"\"backup data for {} using file ('{}/full')\"".format(self.target_db, save_path)
            else:
                backup_command = r"\"backup data DIFFERENTIAL for {} using file ('{}/diff')\"".format(self.target_db,
                                                                                                      save_path)
            exec_command = self.system_db_exec_command_str(backup_command)
            exec_command_log = exec_command.replace(r'-p \"{}\"'.format(self.system_db_pwd), '-p ******')
            log.info('backup cmd is {}:'.format(exec_command_log))
            while try_times < 3:
                log.debug(
                    '[TASK_ID:' + str(self.task_id) + '] hana_db_backup cmd execute. cmd:' + exec_command_log + '')
                result = exec_cmd2(exec_command)
                status = result['ret']
                output = result['msg'].strip()
                log.debug('[TASK_ID:' + str(self.task_id) + '] hana_db_backup cmd finish! status:' + str(status) +
                          ' output:' + output)
                try_times += 1
                if status != 0:
                    log.warn('[TASK_ID:' + str(self.task_id) + '] hana_db_backup cmd fail! dump_cmd:' + exec_command_log
                             + 'status:' + str(status) + ' output:' + output)
                    continue
                if self.backup_mode == config.DB_BACKUP_TYPE_FULL:
                    self.gen_hana_fullback_info_file()
                    return True
                if self.backup_mode == config.DB_BACKUP_TYPE_DIFF:
                    if self.backup_redo_log(save_path):
                        return True
            return False
        except Exception as ex:
            error()
            log.error('[HANA_DB_BACKUP]' + str(type(ex)) + ":" + str(ex))
            return False

    def start_back_hana_db(self):
        log.info('begin start_back_hana_db')
        backup_count = 0
        config = getconf()
        save_path = os.path.join(self.backup_dir, self.target_db)
        if self.transfer_method != 'tcp':
            mount_result = apply_source_and_mount(mount_path=self.backup_dir, backup_mode=self.backup_mode)
            if not mount_result:
                return False
            self.mount_path = self.backup_dir
        if not os.path.exists(save_path):
            os.makedirs(save_path)
            exec_cmd2('chown {}:sapsys {}'.format(self.hana_adm, save_path))
        while 1:
            result = self.hana_db_backup()
            if result is True:
                break
            else:
                backup_count += 1
                if backup_count > config.DB_BACKUP_RETRY_TIMES:
                    log.error('[TASK_ID:' + str(self.task_id) + '] hana backup error! exit.')
                    return False
        return True

    def check_backup_mode(self):
        log.info('begin check hana backup mode!!!')
        config = getconf()
        db_index_file = self.get_idx_path(self.task_id, self.task_type, config.IDX_EXTRA_STR_EMPTY, True)
        if not os.path.exists(db_index_file):
            log.error('[TASK_ID:' + str(self.task_id) + '] check_backup_mode fail! db_index_file not exists!')
        db_index_dir = os.path.dirname(db_index_file)
        hana_fullback_info_file = db_index_dir + '/hana_fullback_info.json'
        if not os.path.isfile(hana_fullback_info_file):
            return config.DB_BACKUP_TYPE_FULL
        with open(hana_fullback_info_file, 'r') as f:
            result = json.load(f)
        if result.get(self.target_db.upper(), None) is None:
            return config.DB_BACKUP_TYPE_FULL
        last_backup_id = self.get_hana_last_fullback_backup_id()
        if last_backup_id == result.get(self.target_db.upper()):
            return config.DB_BACKUP_TYPE_DIFF
        return config.DB_BACKUP_TYPE_FULL

    def __del__(self):
        if self.mount_path:
            log.info('umount ' + self.backup_dir)
            umount_device(self.backup_dir)


class HanaLinuxRecovery(HanaLinuxBase):

    def __init__(self, params):
        super(HanaLinuxRecovery, self).__init__(params)
        self.backup_dir = params.get('backup_dir', '')
        self.recv_file_dir = os.path.join(self.backup_dir, self.target_db)
        self.to_db = params.get('to_db', '').lower()  # 对应界面上的恢复时的目标数据库或者自定义数据库，target_db为原数据库
        self.task_id = params.get('task_id', '')
        self.re_id = params.get('re_id', '')
        self.params = params

    def stop_tenant_database(self, db_name):
        sql_cmd = r"ALTER SYSTEM STOP DATABASE {}".format(db_name)
        exec_command = self.system_db_exec_command_str(sql_cmd)
        result = exec_cmd2(exec_command)
        if result['ret'] != 0:
            raise DbfenError(20120078, result['msg'] + result['errmsg'])

    def reset_tenant_database_password(self):
        # 暂时不用
        default_new_pwd = 'Suj000123'
        sql_cmd = r"ALTER DATABASE {} SYSTEM USER PASSWORD {}".format(self.target_db, default_new_pwd)
        self.stop_tenant_database(self.target_db)
        exec_command = self.system_db_exec_command_str(sql_cmd)
        result = exec_cmd2(exec_command)
        if result['ret'] != 0:
            raise DbfenError(20120079, result['msg'] + result['errmsg'])

    def create_tenant_db(self):
        sql_cmd = r"CREATE DATABASE {} SYSTEM USER PASSWORD Suj000123".format(self.to_db)
        exec_command = self.system_db_exec_command_str(sql_cmd)
        result = exec_cmd2(exec_command)
        if result['ret'] != 0:
            raise DbfenError(20120080, result['msg'] + result['errmsg'])

    def obtain_full_backup_id_from_full_backup_file(self):
        full_backup_file = os.path.join(self.recv_file_dir, 'full_databackup_0_1')
        exec_command = "su - {} -c \"hdbbackupcheck -v {} | grep backupId\"|awk -F : \'{{print $2}}\'".format(
            self.hana_adm, full_backup_file)
        log.info('obtain_full_backup_id_from_full_backup_file exec_command:{}'.format(exec_command))
        result = exec_cmd2(exec_command)
        if result['ret'] != 0:
            raise DbfenError(20120081, result['msg'] + result['errmsg'])
        return result['msg'].replace(' ', '')

    def check_source_db_target_db(self):
        if self.target_db != 'systemdb' and self.to_db != 'systemdb':
            return True
        if self.target_db == 'systemdb' and self.to_db == 'systemdb':
            return True
        log.info('recovery target_db:{},to_db:{}'.format(self.target_db, self.to_db))
        return False

    def check_customize_recovery(self, is_create_db):
        if is_create_db:
            self.stop_tenant_database(self.to_db)
            sql_cmd = r'drop database {}'.format(self.to_db)
            drop_tenant_cmd = self.system_db_exec_command_str(sql_cmd)
            exec_cmd2(drop_tenant_cmd)
            log.info('recovery fail!!!drop customize database:{}'.format(self.to_db))

    def real_recovery_db(self):
        try:
            config = getconf()
            db_recovery_idx_file = os.path.join(self.backup_dir, config.client.db_idx_name)
            if self.transfer_method != 'tcp':
                mount_result = apply_source_and_mount(re_id=self.re_id, mount_path=self.backup_dir)
                self.mount_path = self.backup_dir
                if not mount_result:
                    log.info('mount {} fail!!!'.format(self.recv_file_dir))
                    return False, 0
            if not os.path.exists(db_recovery_idx_file):
                log.error('TASKID:{},db recovery index file not exist! recovery fail!'.format(self.task_id))
                return False, 0
            log.info('db_recovery_idx_file:{},recv_file_dir:{}'.format(db_recovery_idx_file, self.recv_file_dir))
            sqlite_conn = get_sqlite_conn(db_recovery_idx_file)
            task_info = get_db_info_record(sqlite_conn)
            backup_type = int(task_info[2])
            sqlite_conn.close()
            log.info('recovery backup_type is:{}'.format(backup_type))
            if not self.check_source_db_target_db():
                log.error('target_db and to_db wrong relationship!!!')
                return False, 0
            hdb_setting = os.path.join(self.dir_instance, 'HDBSettings.sh')
            recover_sys = os.path.join(self.dir_instance, 'exe/python_support/recoverSys.py')
            is_create_db = False
            if self.to_db != 'systemdb':
                if self.to_db not in self.show_hana_databases():
                    self.create_tenant_db()
                    is_create_db = True
                self.stop_tenant_database(self.to_db)
            if backup_type == config.DB_BACKUP_TYPE_FULL:  # full recovery
                log.info('begin login hana recovery!!!,recv_file_dir：{}'.format(self.recv_file_dir))
                file_path = os.path.join(self.recv_file_dir, 'full')
                if self.target_db == 'systemdb':
                    recovery_command = r"\"RECOVER DATA ALL USING FILE ('{}') " \
                                       r"CLEAR LOG\"".format(file_path)
                    exec_command = 'su - {} -c "{} {} --command={}"'.format(self.hana_adm, hdb_setting, recover_sys,
                                                                            recovery_command)
                    result = exec_cmd2(exec_command)
                else:
                    recovery_command = r"\"RECOVER DATA for {} ALL USING FILE ('{}') CLEAR LOG\"".format(
                        self.to_db, file_path)
                    exec_command = self.system_db_exec_command_str(recovery_command)
                    result = exec_cmd2(exec_command)
                if result['ret'] != 0:
                    log.error('recovery {} fail!!!'.format(self.target_db))
                    self.check_customize_recovery(is_create_db)
                    return False, 0
                return True, 1
            else:  # diff recovery
                future_time = (datetime.datetime.now() + datetime.timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
                full_backup_id = self.obtain_full_backup_id_from_full_backup_file()
                if self.target_db == 'systemdb':
                    recovery_command = r"\"RECOVER DATABASE UNTIL TIMESTAMP '{future_time}'  clear log USING CATALOG PATH " \
                                       r"('{recv_file_path}') USING LOG PATH ('{recv_file_path}') USING DATA PATH " \
                                       r"('{recv_file_path}') USING BACKUP_ID {full_backup_id} CHECK ACCESS USING FILE\"". \
                        format(future_time=future_time, full_backup_id=full_backup_id,
                               recv_file_path=self.recv_file_dir)
                    exec_command = 'su - {} -c "{} {} --command={}"'.format(self.hana_adm, hdb_setting, recover_sys,
                                                                            recovery_command)
                    exec_command_log = exec_command
                else:
                    recovery_command = r"\"RECOVER DATABASE for {new_db_name} UNTIL TIMESTAMP '{future_time}'  clear log " \
                                       r"USING CATALOG PATH ('{recv_file_path}') USING LOG PATH ('{recv_file_path}') " \
                                       r"USING DATA PATH ('{recv_file_path}') USING BACKUP_ID {full_backup_id} CHECK " \
                                       r"ACCESS USING FILE\"".format(new_db_name=self.to_db,
                                                                     future_time=future_time,
                                                                     full_backup_id=full_backup_id,
                                                                     recv_file_path=self.recv_file_dir)
                    exec_command = self.system_db_exec_command_str(recovery_command)
                    exec_command_log = exec_command.replace(r'-p \"{}\"'.format(self.system_db_pwd), '-p ******')
                log.info('exec_command is:{}'.format(exec_command_log))
                result = exec_cmd2(exec_command)
                if result['ret'] != 0:
                    log.error('recovery {} fail!!!'.format(self.target_db))
                    self.check_customize_recovery(is_create_db)
                    return False, 0
                return True, 1
        except Exception as ex:
            log.exception(ex)
            raise ex

    def __del__(self):
        if self.mount_path:
            log.info('umount ' + self.backup_dir)
            umount_device(self.backup_dir)


class HanaWindowBackup(HanaWindowBase):
    def __init__(self, params):
        super(HanaWindowBase, self).__init__(params)


class HanaWindowRecovery(HanaWindowBase):
    def __init__(self, params):
        super(HanaWindowRecovery, self).__init__(params)


def recovery_or_backup(msg):
    if is_win_platform():
        return HanaWindowBackup(msg) if msg.get('re_id', None) is None else HanaWindowRecovery(msg)
    else:
        return HanaLinuxBackup(msg) if msg.get('re_id', None) is None else HanaLinuxRecovery(msg)
