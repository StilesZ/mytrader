import hashlib
import sys
from pathlib import Path
from time import sleep
from datetime import datetime, time
from logging import INFO

from vnpy_ctastrategy import CtaEngine, CtaStrategyApp
from vnpy.gateway.binance import BinanceGateway
from vnpy.gateway.binances import BinancesGateway

from vnpy.event import EventEngine
from vnpy.trader.event import EVENT_LOG
from vnpy.trader.setting import SETTINGS
from vnpy.trader.engine import MainEngine

# SETTINGS["log.active"] = True  #
SETTINGS["log.level"] = INFO
# SETTINGS["log.console"] = True  # 打印信息到终端.

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = str(BASE_DIR) + '/log'

# 现货的
binance_settings = {
    "key": "HkQucOjz9uGcE61Oesofyk01ZuppNVzxF0OYP8SDdNzZn8lE7Pwjuy1kVTwQ4OEp",
    "secret": "iLuamid6Gd52QP5Rf6ee7jNDsfmmilum2pjMPZL0we5jGocxmqMxEki5zy6gtyp3",
    "session_number": 3,
    "proxy_host": "127.0.0.1",
    "proxy_port": 10800
}

binances_settings = {
    "key": "HkQucOjz9uGcE61Oesofyk01ZuppNVzxF0OYP8SDdNzZn8lE7Pwjuy1kVTwQ4OEp",
    "secret": "iLuamid6Gd52QP5Rf6ee7jNDsfmmilum2pjMPZL0we5jGocxmqMxEki5zy6gtyp3",
    "会话数": 3,
    "服务器": "REAL",
    "合约模式": "正向",
    "代理地址": "127.0.0.1",
    "代理端口": 10800
}

if __name__ == "__main__":

    SETTINGS["log.file"] = True

    log_name = f'进程'
    log_dir = LOG_DIR + '/process_log'

    md5_gateway_api = hashlib.md5(f'{binance_settings["key"]}_{binance_settings["secret"]}_BINANCE'.encode('utf-8')).hexdigest()
    md5_gateway_apis = hashlib.md5(f'{binance_settings["key"]}_{binance_settings["secret"]}_BINANCES'.encode('utf-8')).hexdigest()

    event_engine = EventEngine()  # 初始化事件引擎
    main_engine = MainEngine(event_engine)  # 初始化主引擎

    cta_engine: CtaEngine = main_engine.add_app(CtaStrategyApp)  # 添加cta策略的app
    # # 添加cta引擎, 实际上就是初始化引擎。
    # cta_engine.set_log(log_name='cta', log_file=LOG_DIR + r'/cta_log/cta_{}.log'.format('1'))

    log_engine = main_engine.get_engine("log")
    event_engine.register(EVENT_LOG, log_engine.process_log_event)

    cta_engine.init_engine()

    main_engine.add_gateway(BinanceGateway)  # 加载币安现货的网关
    main_engine.add_gateway(BinancesGateway)  # 加载币安合约的网关

    # 连接到交易所
    main_engine.connect(binance_settings, gateway_name=md5_gateway_api)
    main_engine.connect(binances_settings, gateway_name=md5_gateway_apis)
    main_engine.write_log("连接BINANCE接口")

    sleep(10)  # 稍作等待策略启动完成。
    # main_engine.write_log("CTA策略初始化完成")

    # cta_engine.add_strategy(
    #     class_name='TestGridStrategy2',  # 执行策略种类
    #     strategy_name='181',  # 名称
    #     vt_symbol='btcusdt.BINANCE',
    #     setting={'head_fix_long': '0.012', 'head_fix_short': '0.012', 'profit_long': '105', 'profit_short': '102', 'supply_step_long': '204', 'supply_step_short': '210', 'supply_fix_long': '0.015', 'supply_fix_short': '0.015', 'supply_count_long': '19', 'supply_count_short': '19', 'profit_limit_long': '5', 'profit_limit_short': '5', 'loss_long': '1000', 'loss_short': '1000', 'class_name': 'TestGridStrategy'},
    #     strategy_no='181',
    #     gateway_key=md5_gateway_api
    # )

    cta_engine.add_strategy(
        class_name='SpotProfitGridStrategy',  # 执行策略种类
        strategy_name='181',  # 名称
        vt_symbol='btcusdt.BINANCE',
        setting={'grid_step': '2', 'profit_step': '2', 'head_fix': '0.001', 'max_pos': '7', 'profit_orders_counts': '4', 'trailing_stop_multiplier': '3'},
    )
    cta_engine.add_strategy(
        class_name='FutureGridStrategy',  # 执行策略种类
        strategy_name='230',  # 名称
        vt_symbol='BTCUSDT.BINANCE',
        setting={'grid_step': '2', 'profit_step': '2', 'head_fix': '0.001', 'max_pos': '7', 'profit_orders_counts': '4',
                 'trailing_stop_multiplier': '3'},
    )

    setting = {'grid_step': '342', 'profit_step': '2', 'head_fix': '0.001', 'max_pos': '7', 'profit_orders_counts': '4',
               'trailing_stop_multiplier': '3'}

    cta_engine.strategies.get('230').update_setting(setting=setting)

    # cta_engine.init_strategy(strategy_name='181')
    cta_engine.init_all_strategies()  # 初始化所有的策略, 具体启动的哪些策略是来自于配置文件的

    sleep(60)  # 预留足够的时间让策略去初始化.

    main_engine.write_log("CTA策略全部初始化")

    data = {'pos': 1, 'avg_price': 290,
            'profit': 230, 'buy_list': [], 'sell_list': [], 'profit_list': [],
            'stop_list': []}

    # 启动策略
    # cta_engine.start_strategy(strategy_name='181')
    # strategy = cta_engine.strategies.get('181', None)
    # strategy.load(data=data)
    cta_engine.start_all_strategies()  # 开启所有的策略.

    main_engine.write_log("CTA策略全部启动")

    while True:
        sleep(10)

# shell nohub