#  -*- coding:utf-8 -*-
import os
import json
import traceback
from time import sleep
from pathlib import Path

from library import get_logger, convert_single_to_double, base64_encode, compress_string
from tdengine import DBTDengine
from mysql import DBMySQL

BASE_DIR = Path(__file__).resolve().parent  # 文件所在目录路径
LOG_DIR = str(BASE_DIR) + '/log'    # 日志路径
PROCESS_SIZE = 10 # 进程数量
TASK_SIZE = 10   # 为每个进程分配的任务数量

with open('./config.json', 'r', encoding='utf-8') as f:
    config_ = json.loads(f.read())
CONFIG_MYSQL = config_['database']['MySQL']
CONFIG_TD = config_['database']['TDengine']


def task_save(func):
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        if result:
            process = result['process']
            task = result['task']
            strategy = result['strategy']
            strategy_no = strategy['strategy_no']
            # 任务内容为dict类型，通过json.dumps序列化成str类型，用zlib压缩成byte类型，用base64加密成str类型，最后存储至数据库
            text = base64_encode(compress_string(json.dumps(strategy))).decode('utf-8')
            sql = f'''insert into strategy_plan_{process} using strategy_plan_super tags ("{process}") values (NOW, "{process}", "{strategy_no}", {task}, "{text}")'''
            # print('>>>sql:', sql)
            try:
                self.td.nonquery(sql)
            except:
                print('sql error:', traceback.format_exc())
            return result
        else:
            return False
    return wrapper


def task_log(func):
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        action = {1: '启动', 2: '停止', 3: '更新配置', 4: '撤单', 5: '平仓', 6: '重启', 7: '暂停', 8: '恢复'}
        if result:
            process = result['process']
            task = result['task']
            strategy = result['strategy']
            msg = f'分配进程[{process}]任务：策略号--{strategy["strategy_no"]}--{action[task]}'
            self.logger.info(msg)
            return True
        else:
            msg = f'无法分配任务'
            self.logger.info(msg)
            return False
    return wrapper


class Processor(object):

    def __init__(self, name):
        self.name = name
        self.process_name = f"p{self.name}"
        self.action = {'process': self.name, 'strategy': {}, 'task': None}
        self.max_size = TASK_SIZE
        self.strategy_dict = {}

    # 子进程中的策略列表更新
    def update_strategy_dict(self, **kwargs):
        strategy_list = kwargs.get('strategy_list', [])
        for strategy in self.strategy_dict.keys():
            if strategy not in strategy_list:
                self.strategy_dict.pop(strategy)

    def start(self, strategy):
        if len(self.strategy_dict.keys()) >= self.max_size:
            return False
        self.strategy_dict[strategy['strategy_no']] = strategy
        self.action['strategy'] = strategy
        self.action['task'] = 1
        return self.action

    def stop(self, strategy_no):
        result = self.strategy_dict.get(strategy_no, None)
        if not result:
            return False
        self.strategy_dict.pop(strategy_no)
        self.action['strategy'] = {'strategy_no':strategy_no}
        self.action['task'] = 2
        return self.action

    def update_setting(self, strategy_no, setting):
        result = self.strategy_dict.get(strategy_no, None)
        if not result:
            return False
        result['strategy_setting'] = setting
        self.action['strategy'] = result
        self.action['task'] = 3
        return self.action

    # 取消订单
    def cancel_all(self, strategy_no):
        result = self.strategy_dict.get(strategy_no, None)
        if not result:
            return False
        self.action['strategy'] = {'strategy_no':strategy_no}
        self.action['task'] = 4
        return self.action

    # 平仓
    def cover_position(self, strategy_no, rate):
        result = self.strategy_dict.get(strategy_no, None)
        if not result:
            return False
        self.action['strategy'] = {'strategy_no':strategy_no, 'rate': rate}
        self.action['task'] = 5
        return self.action

    # 重启
    def restart(self, strategy_no):
        result = self.strategy_dict.get(strategy_no, None)
        if not result:
            return False
        self.action['strategy'] = {'strategy_no':strategy_no}
        self.action['task'] = 6
        return self.action

    # 暂停
    def pause(self, strategy_no):
        result = self.strategy_dict.get(strategy_no, None)
        if not result:
            return False
        self.strategy_dict.pop(strategy_no)
        self.action['strategy'] = {'strategy_no': strategy_no}
        self.action['task'] = 7
        return self.action

    # 继续
    def resume(self, strategy):
        if len(self.strategy_dict.keys()) >= self.max_size:
            return False
        self.strategy_dict[strategy['strategy_no']] = strategy
        self.action['strategy'] = strategy
        self.action['task'] = 8
        return self.action


class TaskSchedule(object):

    def __init__(self, **kwargs):
        self.config_mysql = CONFIG_MYSQL
        self.config_td = CONFIG_TD
        self.mysql = DBMySQL(
            host=self.config_mysql['host'],
            port=self.config_mysql['port'],
            user=self.config_mysql['user'],
            password=self.config_mysql['password'],
            database=self.config_mysql['db'],
        )
        self.strategy_list = []  # 用户策略任务号集合
        self.exchanges = {}  # 交易所列表信息
        self.strategy_models = {}  # 策略种类列表
        self.exchanges_connection_settings = {}  # 交易所连接配置信息
        self.process_manager = kwargs['process_manager']
        self.logger = None
        self.log_name = '调度器'
        self.log_dir = LOG_DIR + '/schedule_log'
        self.td = DBTDengine(
            host=self.config_td['host'],
            port=self.config_td['port'],
            database=self.config_td['db']
        )

    def init(self):
        self.init_logger()
        self.logger.info(msg='任务调度器初始化中...')
        self.td.connect()
        self.mysql.connect()
        self.init_exchange()
        self.init_strategy_model()
        self.init_connection_settings()

    # 初始化日志
    def init_logger(self):
        self.logger = get_logger(log_name=self.log_name, file=self.log_dir + '/log.log')

    # 初始化交易所
    def init_exchange(self):
        exchanges = self.query_exchange()
        for exchange in exchanges:
            self.exchanges.update({
                exchange[0]: {
                    'name': exchange[1],
                    'en_name': exchange[2],
                }
            })
        self.logger.info(msg='交易所初始化完成')

    # 初始化策略种类
    def init_strategy_model(self):
        strategy_models = self.query_strategy_model()
        for model in strategy_models:
            self.strategy_models.update({
                model[0]: {
                    'name': model[1],
                    'en_name': model[2],
                }
            })
        self.logger.info(msg='策略种类初始化完成')

    # 初始化交易所连接配置信息
    def init_connection_settings(self):
        for each in self.exchanges.keys():
            k = self.exchanges[each]['en_name'].lower()
            if k not in self.exchanges_connection_settings.keys():
                with open(f'./gateway_config/{k}.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.exchanges_connection_settings[k] = data
        self.logger.info(msg='交易所连接配置初始化完成')

    # 查询待运行的策略
    def query_strategy_for_running(self, **kwargs):
        status = kwargs.get('status', 1)
        sql = 'select id, vt_symbol, diyform_val, exch_id, sid, account_id, status from ob_user_strategy_plan where status={} and pre_plan_time between date_sub(now(), interval 1 hour) and now() and id>382'.format(status)
        result_strategy = self.mysql.query(sql)
        return result_strategy

    # 查询恢复暂停中的策略
    def query_strategy_for_resuming(self, **kwargs):
        status = kwargs.get('status', 5)
        sql = 'select id, vt_symbol, diyform_val, exch_id, sid, account_id, status from ob_user_strategy_plan where status={} and id>382'.format(status)
        result_strategy = self.mysql.query(sql)
        return result_strategy

    # 查询需要停止、暂停的策略
    def query_strategy_for_stopping(self, strategy_no):
        sql = 'select status from ob_user_strategy_plan where id={}'.format(strategy_no)
        result_status = self.mysql.query(sql)
        return result_status

    # 查询用户api
    def query_api_setting(self, account_id):
        sql = 'select api_key, api_secret from ob_user_api where id={}'.format(account_id)
        result_api = self.mysql.query(sql)
        return result_api

    # 查询交易所
    def query_exchange(self):
        sql = 'select * from ob_ctastrategy_exchange'
        result_exchange = self.mysql.query(sql)
        return result_exchange

    # 查询策略种类
    def query_strategy_model(self):
        sql = 'select id, sn, en_name from ob_strategy'
        result_strategy_model = self.mysql.query(sql)
        return result_strategy_model

    # 查询策略操作
    def query_strategy_for_controling(self):
        sql = 'select id, strategy_no_id, operation, setting, cover_rate from ob_ctastrategy_controls where status=0'
        result_controls = self.mysql.query(sql)
        return result_controls

    # 查询策略配置
    def query_strategy_new_setting(self):
        sql = 'select id, diyform_val, sid from ob_user_strategy_plan where updated=0'
        result_setting = self.mysql.query(sql)
        return result_setting

    # 更新策略运行状态
    def update_strategy_starting_status(self, strategy_no):
        sql = 'update ob_user_strategy_plan set status=2 where id={}'.format(strategy_no)
        self.mysql.nonquery(sql)
        return True

    # 更新策略停止、暂停状态
    def update_strategy_stopping_status(self, strategy_no, status):
        sql = 'update ob_user_strategy_plan set status={} where id={}'.format(status, strategy_no)
        self.mysql.nonquery(sql)
        return True

    def update_strategy_new_setting_status(self, strategy_no):
        sql = 'update ob_user_strategy_plan set updated=1 where id={}'.format(strategy_no)
        self.mysql.nonquery(sql)
        return True

    def update_strategy_control_status(self, id):
        sql = 'update ob_ctastrategy_controls set status=1 where id={}'.format(id)
        self.mysql.nonquery(sql)
        return True

    # 获取交易所连接配置信息
    def get_api_setting(self, exchange, api):
        connection_params = {}
        exch_name = self.exchanges[exchange]['en_name'].lower()
        exch_connection = self.exchanges_connection_settings[exch_name]
        connection_params[exch_connection['Key Name']] = api[0][0]
        connection_params[exch_connection['Secret Name']] = api[0][1]
        for k in list(exch_connection.keys())[2:]:
            connection_params[k] = exch_connection[k]
        return connection_params

    # 获取策略参数配置
    def get_strategy_setting(self, setting, strategy_name):
        _setting = convert_single_to_double(setting)
        params = json.loads(_setting)
        params['class_name'] = self.strategy_models[strategy_name]['en_name']
        return params

    # 策略连接参数数据
    def get_start_task(self, elements):
        # elements: id, vt_symbol, diyform_val, exch_id, sid, account_id
        strategy_no = str(elements[0])  # 策略任务号
        trade_symbol = elements[1]
        strategy_setting = elements[2]
        exchange_id = elements[3]
        strategy_name_id = elements[4]
        account_id = elements[5]

        result_api = self.query_api_setting(account_id=account_id)
        api_key = result_api[0][0]
        api_value = result_api[0][1]
        api_setting = self.get_api_setting(exchange=exchange_id, api=result_api)
        exchange_name = self.exchanges[exchange_id]['en_name'].lower()  # 交易所
        model = self.strategy_models[strategy_name_id]['en_name']  # 策略
        setting = self.get_strategy_setting(setting=strategy_setting, strategy_name=strategy_name_id)
        return {
            "strategy_no": strategy_no,   # 策略任务号
            "exchange": exchange_name,    # 交易所 BINANCE
            "symbol": trade_symbol,    # 币种 BCTUSDT
            "account": account_id,    # 用户api接口id
            "api": api_setting,    # 策略接口配置
            "strategy_model": model,    # 执行策略种类
            "strategy_setting": setting    # 策略参数连接配置
        }

    def get_process(self):
        try:
            sql = 'select process_name, strategy_list from heartbeat where status=1 and create_time >= NOW-30s'
            result = self.td.query(sql)
            return result
        except:
            print(f'获取进程错误 ', traceback.format_exc())
        return []

    # 进程调度
    def schedule(self):
        processes = self.get_process()  # [('1', '123,124,125'), ('2', '223,224,225')]
        try:
            processes.sort(key=lambda x: int(x[0]))
        except Exception as e:
            print('进程列表:', processes)
            return []
        # for process_ in processes:
        #     p = self.process_manager[int(process_[0])]
        #     p.update_strategy_dict(strategy_list=process_[1].split(','))
        #     print(f'<{p.name}> strategy_dict:', p.strategy_dict)
        for process_ in processes:
            if process_[1] == '' or len(process_[1].split(',')) < TASK_SIZE:
                p = self.process_manager[int(process_[0])]
                yield p
        return []

    # 添加启动任务
    @task_log
    @task_save
    def add_start_task(self, task):
        for process_ in self.schedule():
            if process_:
                result = process_.start(task)
                if result:
                    return result
        return False

    # 添加停止任务
    @task_log
    @task_save
    def add_stop_task(self, task):
        for p in self.process_manager.values():
            result = p.stop(task)
            if result:
                return result
        return False

    # 添加更新策略配置任务
    @task_log
    @task_save
    def add_update_setting_task(self, task, setting):
        for p in self.process_manager.values():
            result = p.update_setting(task, setting)
            if result:
                return result
        return False

    # 添加重启任务
    @task_log
    @task_save
    def add_restart_task(self, task):
        for p in self.process_manager.values():
            result = p.restart(task)
            if result:
                return result
        return False

    # 添加撤单任务
    @task_log
    @task_save
    def add_cancel_all_task(self, task):
        for p in self.process_manager.values():
            result = p.cancel_all(task)
            if result:
                return result
        return False

    # 添加平仓任务
    @task_log
    @task_save
    def add_cover_position_task(self, task, rate):
        for p in self.process_manager.values():
            result = p.cover_position(strategy_no=task, rate=rate)
            if result:
                return result
        return False

    @task_log
    @task_save
    def add_pause_task(self, task):
        for p in self.process_manager.values():
            result = p.pause(task)
            if result:
                return result
        return False

    @task_log
    @task_save
    def add_resume_task(self, task):
        for process_ in self.schedule():
            if process_:
                result = process_.resume(task)
                if result:
                    return result
        return False

    def starter(self):
        self.start()
        self.start_and_load_data()

    # 启动
    def start(self):
        result_strategies = self.query_strategy_for_running(status=1)  # print('需要运行的策略：', result_strategies)
        for each_strategy in result_strategies:
            # result: id, vt_symbol, diyform_val, exch_id, sid, account_id, status
            strategy_no = str(each_strategy[0])
            t = self.get_start_task(each_strategy)
            # 创建任务
            self.logger.info(msg=f'启动任务：策略号--{strategy_no}')
            result = self.add_start_task(task=t)
            if result:
                # 更新策略为已启动
                self.update_strategy_starting_status(strategy_no=strategy_no)
                self.strategy_list.append(strategy_no)
                self.logger.info(msg=f'分配启动任务完成，策略号--{strategy_no}')

    def start_and_load_data(self):
        # 查询暂停中待启动的策略任务
        result_strategies = self.query_strategy_for_resuming(status=5)
        for each_strategy in result_strategies:
            strategy_no = str(each_strategy[0])
            t = self.get_start_task(each_strategy)
            self.logger.info(msg=f'恢复任务：策略号--{strategy_no}')
            result = self.add_resume_task(task=t)
            if result:
                self.update_strategy_starting_status(strategy_no=strategy_no)
                self.strategy_list.append(strategy_no)
                self.logger.info(msg=f'分配恢复任务完成，策略号--{strategy_no}')

    # 停止
    def stop(self):
        for strategy_no in self.strategy_list:
            status = self.query_strategy_for_stopping(strategy_no=strategy_no)
            if status:
                # if 策略任务为 待终止 执行终止操作
                if int(status[0][0]) == -1:
                    self.logger.info(msg=f'停止任务：策略号--{strategy_no}')
                    result = self.add_stop_task(task=strategy_no)
                    if result:
                        self.strategy_list.remove(strategy_no)
                        self.update_strategy_stopping_status(strategy_no=strategy_no, status=0)
                        self.logger.info(msg=f'分配停止任务完成，策略号--{strategy_no}')
                # if 策略任务为 待暂停 执行终止操作
                elif int(status[0][0]) == 3:
                    self.logger.info(msg=f'暂停任务：策略号--{strategy_no}')
                    result = self.add_pause_task(task=strategy_no)
                    if result:
                        self.strategy_list.remove(strategy_no)
                        self.update_strategy_stopping_status(strategy_no=strategy_no, status=4)
                        self.logger.info(msg=f'分配暂停任务完成，策略号--{strategy_no}')

    # 更新策略配置
    def update_strategy_setting(self):
        results = self.query_strategy_new_setting()
        for result in results:
            strategy_no = str(result[0])
            if strategy_no not in self.strategy_list:
                continue
            self.logger.info(msg=f'策略配置更新任务：策略号--{strategy_no}')
            try:
                strategy_setting = result[1]
                strategy_name_id = result[2]
                setting = self.get_strategy_setting(setting=strategy_setting, strategy_name=strategy_name_id)
                result = self.add_update_setting_task(task=strategy_no, setting=setting)
            except:
                print('更新策略配置错误：', traceback.format_exc())
            self.update_strategy_new_setting_status(strategy_no=strategy_no)
            if result:
                self.logger.info(msg=f'分配策略配置更新任务完成，策略号--{strategy_no}')

    # 重启
    def restart(self, strategy_no):
        # self.add_restart_task(task=strategy_no)
        return

    # 撤单
    def cancel_all(self, strategy_no):
        # self.add_cancel_all_task(task=strategy_no)
        return

    # 平仓
    def cover_position(self, strategy_no, rate):
        self.logger.info(msg=f'平仓任务：策略号--{strategy_no}')
        if not rate:
            rate = 1
        else:
            rate = float(rate) / 100
        result = self.add_cover_position_task(task=strategy_no, rate=rate)
        if result:
            self.logger.info(msg=f'分配策略平仓任务完成，策略号--{strategy_no}')

    # 策略操作
    def control(self):
        # 用户策略操作
        result_controls = self.query_strategy_for_controling()
        for result in result_controls:
            strategy_no = str(result[1])
            if strategy_no in self.strategy_list:
                try:
                    if result[2] == 1:
                        self.cancel_all(strategy_no=strategy_no)
                    elif result[2] == 2:
                        self.cover_position(strategy_no=strategy_no, rate=result[4])
                    elif result[2] == 3:
                        self.restart(strategy_no=strategy_no)
                except:
                    print('策略操作错误：', traceback.format_exc())
                self.update_strategy_control_status(id=result[0])

    def run(self):
        while True:
            try:
                self.starter()
                self.stop()
                self.control()
                self.update_strategy_setting()
            except:
                print('run error:', traceback.format_exc())
            sleep(5)


def main():
    # 进程
    process_manager = {i+1: Processor(name=i+1) for i in range(PROCESS_SIZE)}

    tm = TaskSchedule(process_manager=process_manager)
    tm.init()
    tm.run()


if __name__ == '__main__':
    main()
