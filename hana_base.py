# -*- coding: utf-8 -*-
"""
@author:neo
@file: hana_base.py
@time: 2020/01/06 13:11
@desc:base
@blog:https://blog.csdn.net/mingtiannihaoabc
"""
import os
from common import exec_cmd2, DbfenError, log


class HanaLinuxBase(object):
    def __init__(self, params):
        super(HanaLinuxBase, self).__init__(params)
        self.hana_adm = params['hana_adm']
        necessary_info = self.get_necessary_info()
        self.instance = necessary_info['instance']
        self.dir_instance = necessary_info['dir_instance']
        self.hdb_sql = '{}/exe/hdbsql'.format(self.dir_instance)
        self.system_db_user = params['db_user']
        self.system_db_pwd = params['db_passwd']
        self.tenant_user = params.get('tenant_user', '')
        self.tenant_passwd = params.get('tenant_passwd', '')
        self.target_db = params.get('target_db', '').lower()
        self.transfer_method = params.get('transfer_method', 'tcp')
        self.mount_path = None

    def get_necessary_info(self):
        log.info('begin get_necessary_info!!!')
        split_result = '|awk -F \'[=]\' \'{print $2}\''
        get_instance = '|grep TINSTANCE{}'.format(split_result)
        dir_instance = '|grep DIR_INSTANCE{}'.format(split_result)
        get_instance_cmd = "su - {} -c 'env'{}".format(self.hana_adm, get_instance)
        get_dir_instance_cmd = "su - {} -c 'env'{}".format(self.hana_adm, dir_instance)
        try:
            log.info('get_instance_cmd:{}'.format(get_instance_cmd))
            log.info('get_dir_instance_cmd:{}'.format(get_dir_instance_cmd))
            instance_result = exec_cmd2(get_instance_cmd)
            if instance_result['ret'] != 0:
                raise DbfenError(20120073, instance_result['msg'] + instance_result['errmsg'])
            dir_instance_result = exec_cmd2(get_dir_instance_cmd)
            if dir_instance_result['ret'] != 0:
                raise DbfenError(20120074, dir_instance_result['msg'] + dir_instance_result['errmsg'])
        except Exception as ex:
            log.exception(ex)
            raise ex
        return {'instance': instance_result['msg'], 'dir_instance': dir_instance_result['msg']}

    def hana_check_version(self):
        # check hana database version
        hdb = '{}/HDB'.format(self.dir_instance)
        find_version_cmd = "su - {} -c '{} version'".format(self.hana_adm, hdb)
        find_version_result = exec_cmd2(find_version_cmd)
        if find_version_result['ret'] != 0:
            raise DbfenError(20120075, find_version_result['msg'] + find_version_result['errmsg'])
        version = ''
        for i in find_version_result['msg'].split(os.linesep):
            if 'version' in i and 'HDB' not in i:
                version = i.split(':')[1].replace(' ', '')
                break
        return version

    def check_hdb_daemon(self):
        # check hana hdb is normal running
        log.info('begin check_hdb_daemon!!!')
        sap_control = '{}/exe/sapcontrol'.format(self.dir_instance)
        get_processlist_cmd = "su - {} -c '{} -nr {} -function GetProcessList'".format(self.hana_adm, sap_control,
                                                                                       self.instance)
        log.info('begin check_hdb_daemon:{}'.format(get_processlist_cmd))
        get_processlist_result = exec_cmd2(get_processlist_cmd)
        for i in get_processlist_result['msg'].split(os.linesep):
            if 'hdbdaemon' in i:
                split_i = i.split(',')
                if split_i[3] == 'Stopped':
                    return False
                else:
                    return True
        else:
            return False

    def system_db_exec_command_str(self, sql_cmd, show_all=True):
        if show_all:
            log.info('show all databases!!!')
            command_str = r'su - {} -c "{} -n localhost -i {} -d SystemDB -u {} -p \"{}\" -x {}"'.format(self.hana_adm,
                                                                                                         self.hdb_sql,
                                                                                                         self.instance,
                                                                                                         self.system_db_user,
                                                                                                         self.system_db_pwd,
                                                                                                         sql_cmd)
        else:
            log.info('check logic backup target_db user and password!!!')
            command_str = r'su - {} -c "{} -n localhost -i {} -d {} -u {} -p \"{}\" -x {}"'.format(self.hana_adm,
                                                                                                   self.hdb_sql,
                                                                                                   self.instance,
                                                                                                   self.target_db,
                                                                                                   self.tenant_user,
                                                                                                   self.tenant_passwd,
                                                                                                   sql_cmd)
        return command_str

    def show_hana_databases(self, is_show=True):
        log.info('begin show_hana_databases!!!')
        sql_cmd = r"select DATABASE_NAME from SYS.M_DATABASES where ACTIVE_STATUS=\'YES\'"
        exec_command = self.system_db_exec_command_str(sql_cmd)
        exec_command_log = exec_command.replace(r'-p \"{}\"'.format(self.system_db_pwd), '-p ******')
        log.info('check_system_db_cmd:{}'.format(exec_command_log))
        try:
            result = exec_cmd2(exec_command)
            log.info('result is:{}'.format(result))
            status = result['ret']
            output = result['msg'].strip()
            if status != 0 and self.tenant_user == '':
                log.error('system db abnormal,please check system db!!!,maybe is password not correct or others!!!')
                raise DbfenError(20120082, result['msg'] + result['errmsg'])
            databases = self.split_result_database_str(output)
            if len(databases) == 1:
                log.error('system db abnormal,please check system db!!!,maybe is password not correct or others!!!')
                raise DbfenError(20120082, result['msg'] + result['errmsg'])
            if is_show:
                return databases
            exec_command = self.system_db_exec_command_str(sql_cmd, False)
            exec_command_log = exec_command.replace(r'-p \"{}\"'.format(self.tenant_passwd), '-p ******')
            log.info('check target db {} is whether normal!!!'.format(exec_command_log))
            result = exec_cmd2(exec_command)
            status = result['ret']
            output = result['msg'].strip()
            tenant = self.split_result_database_str(output)
            if len(tenant) == 0:
                log.error('tenant db not active!!!please check tenant {} status!!!'.format(self.target_db))
                raise DbfenError(20120083, result['msg'] + result['errmsg'])
            if status != 0:
                log.error('maybe is db: {} password incorrect!!!'.format(self.target_db))
                raise DbfenError(20120076, result['msg'] + result['errmsg'])
            return tenant
        except Exception as ex:
            log.exception(ex)
            raise ex

    @staticmethod
    def split_result_database_str(output):
        databases = output.replace('"', '').split(os.linesep)
        if 'DATABASE_NAME' in databases:
            databases.remove('DATABASE_NAME')
        databases = [name.lower() for name in databases]
        log.info('split_result_database_str is:{}'.format(databases))
        return databases


class HanaWindowBase(object):
    def __init__(self, params):
        super(HanaWindowBase, self).__init__(params)
