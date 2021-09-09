#  -*- coding:utf-8 -*-
import sys
import time
import json
import hashlib
import datetime
import traceback
from logging import INFO
from pathlib import Path

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
# from vnpy.trader.test_engine import MainEngine
from vnpy.app.cta_strategy import CtaStrategyApp, CtaEngine

from __init__ import import_gateway_model
from library import get_logger, convert_single_to_double, base64_encode, base64_decode, compress_string, decompress_string
from tdengine import DBTDengine
from vnpy.trader.event import EVENT_LOG
from vnpy.trader.setting import SETTINGS

SETTINGS["log.level"] = INFO

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = str(BASE_DIR) + '/log'
with open('./config.json', 'r', encoding='utf-8') as f:
    config_ = json.loads(f.read())
CONFIG_TD = config_['database']['TDengine']
# 进程名，策略号，执行操作，操作参数
# create database ctastrategy;
# create table strategy_plan_super (create_time TIMESTAMP, process_name NCHAR(5), strategy_no NCHAR(11), action INT(2), setting NCHAR(4000)) tags (ProcessName NCHAR(5));
# create table heartbeat (create_time TIMESTAMP, process_name NCHAR(5), strategy_list NCHAR(150));
# create table strategy_data (create_time TIMESTAMP, strategy_no NCHAR(11), trade_data NCHAR(4000));
# insert into strategy_plan_p1 using strategy_plan_super tags ('p1') values ('2020-08-01 12:00:00.123', 'p1', '123', 1, '{"strategy_no": "1","exchange": "bitmex","symbol": "XBTUSD","account": 1,"api": {"ID": "zkQBM2qVvZymlctc5shHC2AJ","Secret": "eyLjpKP1eZQiCBB2vUc_OfTpA-2lclfG-s3MrQIsrSe5sAbl","\u4f1a\u8bdd\u6570": 3,"\u670d\u52a1\u5668": ["REAL", "TESTNET"],"\u4ee3\u7406\u5730\u5740": "127.0.0.1","\u4ee3\u7406\u7aef\u53e3": "10800"},"strategy_model": "TestGridStrategy7","strategy_setting": {"head_fix_long": "100","profit_long": "50","profit_limit_long": "12","loss_long": "20","supply_fix_long": "100","supply_step_long": "200","supply_count_long": "20","head_fix_short": "100","profit_short": "50","profit_limit_short": "8","loss_short": "20","supply_fix_short": "100","supply_step_short": "100","supply_count_short": "10","class_name": "TestGridStrategy7"}}');


class TaskExecutor(object):

    def __init__(self, name):
        self.name = name
        self.strategy_list = []
        self.main_engine = None
        self.event_engine = None
        self.cta_engine = None
        self.logger = None
        self.start_time = None
        self.subscriber = None
        self.log_name = f'进程({self.name})'
        self.log_dir = LOG_DIR + '/process_log'
        self.config_td = CONFIG_TD
        self.td = DBTDengine(
            host=self.config_td['host'],
            port=self.config_td['port'],
            database=self.config_td['db']
        )
        self.controls = {1: self.start_strategy, 2: self.stop_strategy, 3: self.update_strategy_setting, 4: self.cancel_all,
                         5: self.cover_position, 7: self.pause_strategy, 8: self.resume_strategy}

    @staticmethod
    def localtime():
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    def init(self):
        self.init_logger()
        self.logger.info(msg=f'进程[{self.name}]初始化中...')

        self.event_engine = EventEngine()
        self.main_engine = MainEngine(self.event_engine)
        # 主引擎创建成功
        self.cta_engine: CtaEngine = self.main_engine.add_app(CtaStrategyApp)
        # self.cta_engine: CtaEngine = self.main_engine.add_cta_app(CtaStrategyApp, database='trade')
        self.cta_engine.set_log(log_name='cta', log_file=LOG_DIR + r'/cta_log/cta_{}.log'.format(self.name))

        # 注册日志
        log_engine = self.main_engine.get_engine("log")
        self.event_engine.register(EVENT_LOG, log_engine.process_log_event)

        # CTA策略初始化完成
        self.cta_engine.init_engine()
        self.td.connect()
        self.start_time = self.localtime()
        self.hb_time = datetime.datetime.now()
        self.subscriber = self.td.subscribe_topic(topic=f'strategy_plan_p{self.name}',
            sql=f'select strategy_no, action, setting from strategy_plan_super where process_name="{self.name}" and create_time>"{self.start_time}"')  # 订阅
        self.logger.info(msg=f'进程[{self.name}]初始化完成')

    def init_logger(self):
        self.logger = get_logger(log_name=self.log_name, file=self.log_dir + '/process_{}.log'.format(self.name))

    def heartbeat(self):
        curr_time = datetime.datetime.now()
        temp = curr_time - self.hb_time
        if temp.seconds >= 30:
            sql = 'insert into heartbeat values (NOW, "{}", 1, "{}")'.format(
                self.name, ','.join([each['strategy_no'] for each in self.strategy_list])
            )
            self.td.nonquery(sql)
            self.hb_time = curr_time

    def query_history_order(self, strategy_no):
        sql = 'select last(*) from strategy_data where strategy_no="{}" and create_time>now-1d'.format(strategy_no)
        result = self.td.query(sql)
        return result

    def start_strategy(self, task):
        _, strategy = self.unpack_task(task)
        self.strategy_list.append(strategy)
        strategy_no = str(strategy['strategy_no'])
        api_key = list(strategy['api'].values())[0]
        api_secret = list(strategy['api'].values())[1]
        md5_gateway_api = hashlib.md5(f'{api_key}_{api_secret}'.encode('utf-8')).hexdigest()
        self.logger.info(msg=f'启动策略号--{strategy_no}--中...')
        # 添加交易接口
        self.main_engine.add_gateway(gateway_class=import_gateway_model(strategy['exchange']), gateway_key=md5_gateway_api)
        # 连接交易所
        self.main_engine.connect(setting=strategy['api'], gateway_name=md5_gateway_api)
        self.logger.info(msg=f'连接交易接口：{strategy["exchange"]}')
        time.sleep(20)

        self.logger.info(msg=f'Cta引擎加载策略号--{strategy_no}')
        exchange_name = strategy['exchange'].upper()
        if exchange_name.endswith('S'):
            exchange_name = exchange_name[:-1]
        # 添加策略
        self.cta_engine.add_strategy(
            class_name=strategy['strategy_model'],  # 执行策略种类
            strategy_name=strategy_no,  # 名称
            vt_symbol='{}.{}'.format(strategy['symbol'], exchange_name),
            setting=strategy['strategy_setting'],
            strategy_no=strategy_no,
            gateway_key=md5_gateway_api
        )
        # 初始化策略
        self.cta_engine.init_strategy(strategy_name=strategy_no)
        # 启动策略
        self.cta_engine.start_strategy(strategy_name=strategy_no)
        self.logger.info(msg=f'启动策略号--{strategy_no}--完成')

    def stop_strategy(self, task):
        strategy_no, _ = self.unpack_task(task)
        self.logger.info(msg=f'停止策略号--{strategy_no}--中...')
        self.cta_engine.stop_strategy(strategy_name=strategy_no)
        self.cta_engine.remove_strategy(strategy_name=strategy_no)
        for each in self.strategy_list:
            if str(each['strategy_no']) == str(strategy_no):
                self.strategy_list.remove(each)
                break
        self.logger.info(msg=f'停止策略号--{strategy_no}--完成')

    def update_strategy_setting(self, task):
        strategy_no, setting = self.unpack_task(task)
        setting = setting.get('strategy_setting', None)
        self.logger.info(msg=f'更新策略号--{strategy_no}--配置中...')
        strategy = self.cta_engine.strategies.get(strategy_no, None)
        if strategy:
            strategy.update_setting(setting=setting)

            # _setting = convert_single_to_double(setting)
            # params = json.loads(_setting)
            # strategy.update_setting(setting=params)
            self.logger.info(msg=f'更新策略号--{strategy_no}--配置完成')

    def cancel_all(self, task):
        strategy_no, _ = self.unpack_task(task)
        strategy = self.cta_engine.strategies.get(strategy_no, None)
        if strategy:
            strategy.cancel_all()

    def cover_position(self, task):
        strategy_no, setting = self.unpack_task(task)
        self.logger.info(msg=f'策略号--{strategy_no}--平仓中...')
        strategy = self.cta_engine.strategies.get(strategy_no, None)
        if strategy:
            strategy.close_all_position(rate=setting.get('setting', 1))
            # strategy.trading = False
            self.logger.info(msg=f'策略号--{strategy_no}--平仓完成')

    def pause_strategy(self, task):
        strategy_no, _ = self.unpack_task(task)
        self.logger.info(msg=f'暂停策略号--{strategy_no}--中...')
        strategy = self.cta_engine.strategies.get(strategy_no, None)
        if strategy:
            data = strategy.record()
            str_data = base64_encode(compress_string(json.dumps(data))).decode('utf-8')
            sql = '''insert into strategy_data values (NOW, "{}", '{}')'''.format(strategy_no, str_data)
            self.td.nonquery(sql)
            self.stop_strategy(task)
            self.logger.info(msg=f'暂停策略号--{strategy_no}--完成')

    def resume_strategy(self, task):
        strategy_no, strategy = self.unpack_task(task)
        self.logger.info(msg=f'恢复策略号--{strategy_no}--中...')
        sql = 'select last(*) from strategy_data where strategy_no="{}" and create_time>now-1d'.format(strategy_no)
        result = self.td.query(sql)
        if result:
            self.start_strategy(task)
            strategy = self.cta_engine.strategies.get(strategy_no, None)
            if strategy:
                data = json.loads(decompress_string(base64_decode(result[0][2])).decode('utf-8')) # json.loads(result[0][2])
                strategy.load(data=data)
                self.logger.info(msg=f'恢复策略号--{strategy_no}--完成')
            else:
                self.logger.info(msg=f'恢复策略号--{strategy_no}--失败，CTA引擎无法获取加载的策略')
        else:
            self.logger.info(msg=f'恢复策略号--{strategy_no}--失败，无法获取历史委托数据')

    def unpack_task(self, task):
        # task: strategy_no, action, strategy
        return task[0], json.loads(decompress_string(base64_decode(task[2])).decode('utf-8'))

    def run(self):
        self.init()
        while True:
            try:
                tasks = self.subscriber.consume()
                for data in tasks:
                    # 策略操作
                    # 1 启动策略 2 停止策略 3 更新策略 4 取消策略 5 平仓 7 暂停策略 8 恢复策略
                    fn = self.controls[data[1]]
                    fn(data)
            except:
                print(f'进程[{self.name}]异常，', traceback.format_exc())
            try:
                self.heartbeat()
            except:
                print(f'进程[{self.name}]心跳失败！', traceback.format_exc())
            time.sleep(1)


def main():
    # param = sys.argv[1]
    param = 1
    t = TaskExecutor(name=param)
    t.run()


if __name__ == '__main__':
    main()
