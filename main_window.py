from vnpy.app.cta_strategy.base import EVENT_CTA_LOG
from vnpy.app.paper_account import PaperAccountApp
from vnpy.event import EventEngine

from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy.app.cta_strategy.engine import CtaEngine
from vnpy.app.cta_backtester.engine import BacktesterEngine
from vnpy.app.cta_strategy.backtesting import BacktestingEngine

from vnpy.gateway.binance import BinanceGateway  # 现货
from vnpy.gateway.binances import BinancesGateway  # 合约
from vnpy.gateway.bitmex import BitmexGateway  # 合约

from vnpy.app.cta_strategy import CtaStrategyApp  # CTA策略
from vnpy.app.data_manager import DataManagerApp  # 数据管理, csv_data
from vnpy.app.data_recorder import DataRecorderApp  # 录行情数据
from vnpy.app.algo_trading import AlgoTradingApp  # 算法交易
from vnpy.app.cta_backtester import CtaBacktesterApp  # 回测研究
from vnpy.app.risk_manager import RiskManagerApp  # 风控管理
from vnpy.app.spread_trading import SpreadTradingApp  # 价差交易

from pathlib import Path


def main():
    """"""
    BASE_DIR = Path(__file__).resolve().parent
    LOG_DIR = str(BASE_DIR) + '/log'

    log_name = f'进程'
    log_dir = LOG_DIR + '/process_log'

    qapp = create_qapp()

    event_engine = EventEngine()

    main_engine = MainEngine(event_engine)

    # main_engine.add_gateway(BinanceGateway, gateway_key='f4a52d262c05cae89bc388119e0ca9df')
    # main_engine.add_gateway(BinancesGateway, gateway_key='f4a52d262c05cae89bc388119e0ca9df')

    main_engine.add_gateway(BinanceGateway, gateway_key='f4a52d262c05cae89bc388119e0ca9df')
    cta_engine: CtaEngine = main_engine.add_app(CtaStrategyApp)
    # back_engine: BacktesterEngine = main_engine.add_app(CtaBacktesterApp)
    # backer_engine: BacktesterEngine = main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(DataManagerApp)
    main_engine.add_app(AlgoTradingApp)
    main_engine.add_app(DataRecorderApp)
    main_engine.add_app(RiskManagerApp)
    main_engine.add_app(SpreadTradingApp)
    main_engine.add_app(PaperAccountApp)

    cta_engine.set_log(log_name='cta', log_file=LOG_DIR + r'/cta_log/cta_{}.log'.format('1'))
    # backer_engine.set_log(log_name='cta', log_file=LOG_DIR + r'/cta_log/cta_{}.log'.format('1'))
    # engine = BacktesterEngine()
    # engine.set_log(log_name='cta', log_file=LOG_DIR + r'/cta_log/cta_{}.log'.format('1'))

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    """
     howtrader main window demo
     howtrader 的图形化界面

     we have binance gate way, which is for spot, while the binances gateway is for contract or futures.
     the difference between the spot and future is their symbol is just different. Spot uses the lower case for symbol,
     while the futures use the upper cases.

     币安的接口有现货和合约接口之分。 他们之间的区别是通过交易对来区分的。现货用小写，合约用大写。 btcusdt.BINANCE 是现货的symbol,
     BTCUSDT.BINANCE合约的交易对。 BTCUSD.BINANCE是合约的币本位保证金的交易对.
     
     ETHUSD.BITMEX

     BTCUSDT, BTCUSDT
    """

    main()

