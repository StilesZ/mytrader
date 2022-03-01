import datetime
from decimal import Decimal
from typing import Union

from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager
)

from vnpy.trader.event import EVENT_TIMER, EVENT_ACCOUNT
from vnpy.event import Event

from vnpy.trader.constant import Offset, Direction, Exchange, Status


class oTrade(object):
    def __init__(self, vt_orderid='', vt_tradeid=None, price=0, trade_list=list(), volume=0, status=-1, trade_price=0):
        self.vt_orderid = vt_orderid
        self.vt_tradeid = vt_tradeid
        self.price = price
        self.trade_price = trade_price
        self.trade_list = trade_list
        self.volume = volume
        self.status = status    # -2撤销、拒单 -1生成 0提交 0.5未成交 1全部成交
        self.create_time = datetime.datetime.now()


class OrderManager(object):
    def __init__(self):
        self.orders = []
        self.canceled_orders = []
        self.status_canceled = [Status("已撤销"), Status("拒单")]

    def __getitem__(self, item: int):
        return self.orders[item]

    def __len__(self):
        return len(self.orders)

    def list_all(self):
        print('委托情况：', [(order.vt_orderid, order.price, order.volume, order.status) for order in self.orders])
        print('取消情况：', [(order.vt_orderid, order.price, order.volume, order.status) for order in self.canceled_orders])

    def list(self):
        return self.orders

    def append(self, o: oTrade):
        self.orders.append(o)

    def remove(self, o: OrderData):
        for order in self.orders:
            if order.vt_orderid == o.vt_orderid:
                self.orders.remove(order)

    def empty(self):
        if not self.orders:
            return True
        return False

    def exist(self, o: OrderData):
        for order in self.orders:
            if order.vt_orderid == o.vt_orderid:
                return True
        return False

    # 订单撤销
    def event_canceled(self, o: OrderData):
        for order in self.orders:
            if order.vt_orderid == o.vt_orderid:
                order.status = -2
                self.orders.remove(order)
                self.canceled_orders.append(order)
                break

    def event_untraded(self, o: OrderData):
        for order in self.orders:
            if order.vt_orderid == o.vt_orderid:
                order.status = 0
                break

    def event_uncompleted_trade(self, o: OrderData):
        for order in self.orders:
            if order.vt_orderid == o.vt_orderid:
                order.status = 0.5
                break

    # 判断是否全部成交
    def event_traded(self, t: TradeData):
        completed_trade = False
        for order in self.orders:
            if order.vt_orderid == t.vt_orderid:
                order.volume -= t.volume
                order.trade_price = t.price
                if Decimal(order.volume).quantize(Decimal('0.00000')) <= Decimal('0'):
                    order.status = 1
                    completed_trade = True
                else:
                    order.status = 0.5
                break
        return completed_trade

    def untraded_orders(self):
        for order in self.orders:
            # 生成 -1 提交 0 未成交 0.5
            if -2 < order.status < 1:
                yield order

    # 查询最后一笔全部完成的订单
    def last_trade(self):
        index_ = -1
        while True:
            try:
                last_ = self.orders[index_]
            except Exception as e:
                print('委托搜索失败，', e)
                break
            #  1 该笔订单全部成交 否则继续往前找订单
            if last_.status == 1:
                return last_
            index_ -= 1
        return False


class MartingaleSpotStrategy(CtaTemplate):
    """
    1. 马丁策略.
    币安邀请链接: https://www.binancezh.pro/cn/futures/ref/51bitquant
    币安合约邀请码：51bitquant
    """

    """
    1. 开仓条件是 最高价回撤一定比例 4%
    2. 止盈2%
    3. 加仓: 入场后, 价格最低下跌超过5%， 最低点反弹上去1%, 那么就可以加仓. 均价止盈2%.
    """

    # 策略的核心参数. 唐奇安
    donchian_window = 2880  # 2 days
    donchian_open = 4  # 最高值回撤4%时开仓.

    dump_down_pct = 4  # 跌4%
    bounce_back_pct = 1  # 反弹 %1

    exit_profit_pct = 2  # 出场平仓百分比 2%
    initial_trading_value = 1000  # 首次开仓价值 1000USDT.
    head_fix = 0.05  # 头寸
    trading_value_multiplier = 1.3  # 加仓的比例.
    max_increase_pos_times = 7  # 最大的加仓次数
    trading_fee = 0.00075  # 手续费 0.075%

    # 变量
    avg_price = 0.0  # 当前持仓的平均价格.
    last_entry_price = 0.0  # 上一次入场的价格.
    current_pos = 0.0  # 当前的持仓的数量.
    current_increase_pos_times = 0  # 当前的加仓的次数.

    upband = 0.0
    downband = 0.0
    entry_lowest = 0.0  # 进场之后的最低价.
    total_profit = 0.0

    parameters = ["donchian_window", "donchian_open", "dump_down_pct", "bounce_back_pct",
                  "exit_profit_pct", "head_fix",
                  "trading_value_multiplier", "max_increase_pos_times", "trading_fee"]

    variables = ["last_entry_price", "current_pos", "current_increase_pos_times",
                 "upband", "downband", "entry_lowest", "total_profit"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager(3000)

        self.holds = []
        self.buy_orders = OrderManager()
        self.sell_orders = OrderManager()

        self.tSymbol, self.tExchange = self.vt_symbol.split('.')
        self.lastTick = TickData(
            symbol=self.tSymbol,
            exchange=Exchange(self.tExchange),
            datetime=datetime.datetime.now(),
            gateway_name=self.tExchange
        )

        self.timer_interval = 0
        self.current_increase_pos_times = 0
        self.manually_close_pos_flag = False

        self.min_notional = 11  # 最小的交易金额.


    def init_setting(self):
        self.rdonchian = self.donchian_window
        self.rdonchian_open = float(self.donchian_open) / 100

        self.rdown_pct = float(self.dump_down_pct) / 100
        self.rback_pct = float(self.bounce_back_pct) / 100

        self.rhead_fix = self.head_fix
        self.rprofit = float(self.exit_profit_pct) / 100  # 盈利2%
        self.rinitial = self.initial_trading_value  # 开仓金额
        self.rmultiplier = self.trading_value_multiplier  # 加仓
        self.rmax_times = self.max_increase_pos_times  # 持仓数量

        self.rtrading_fee = self.trading_fee

        # 当前有仓位 且 (策略启动 或 更新策略)
        if self.pos and self.signal_update_paramters > 1:
            self.cancel_all()  # 撤销

        # 策略启动 状态减一
        if self.signal_update_paramters > 0:
            self.signal_update_paramters -= 1

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.print_log(msg=f'{self.logmsg_template()}: 策略初始化')
        self.load_bar(3)  # 加载3天的数据.

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.print_log(msg=f'{self.logmsg_template()}: 策略启动')
        self.signal_update_paramters = 2

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.print_log(msg=f'{self.logmsg_template()}: 策略停止')

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        if self.signal_update_paramters > 0:
            self.init_setting()  # 更新策略配置
        if self.lastTick.last_price != tick.last_price:
            self.lastTick = tick
        self.bg.update_tick(tick)
        # self.put_event()

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        self.init_setting()
        current_close = am.close_array[-1]
        current_low = am.low_array[-1]

        self.upband, self.downband = am.donchian(self.rdonchian, array=False)  # 返回最新的布林带值.

        # 行情跌值百分比
        dump_pct = self.upband / current_low - 1

        # 上次成交价与当前行情价对比
        if self.entry_lowest > 0:
            self.entry_lowest = min(self.entry_lowest, bar.low_price)

        # 回调一定比例的时候.
        if self.current_pos * current_close < self.min_notional:
            # 每次下单要大于等于10USDT, 为了简单设置11USDT.
            if dump_pct >= self.rdonchian_open and self.buy_orders.empty():
                # 这里没有仓位.
                # 重置当前的数据.
                self.cancel_all()
                self.current_increase_pos_times = 0
                self.avg_price = 0
                self.entry_lowest = 0

                price = current_close
                vol = self.rhead_fix
                self.buy_order(price=price, volume=vol)
        else:

            if self.sell_orders.empty() and self.avg_price > 0:
                # 有利润平仓的时候
                # 清理掉其他买单.

                profit_percent = bar.close_price / self.avg_price - 1
                if profit_percent >= self.rprofit:
                    self.cancel_all()
                    self.sell_order(price=bar.close_price, volume=abs(self.current_pos))
                self.print_log(
                    msg=f'{self.logmsg_template()}: 到达止盈点平仓，金额: {bar.close_price}')

            if self.entry_lowest > 0 and self.buy_orders.empty():
                # 考虑加仓的条件:
                # 1）当前有仓位,且仓位值要大于11USDT以上
                # 2）加仓的次数小于最大的加仓次数
                # 3）当前的价格比上次入场的价格跌了一定的百分比

                dump_down_pct = self.last_entry_price / self.entry_lowest - 1
                bounce_back_pct = bar.close_price / self.entry_lowest - 1

                if self.current_increase_pos_times <= self.rmax_times and dump_down_pct >= self.rdown_pct and bounce_back_pct >= self.rback_pct:
                    # ** 表示的是乘方.
                    self.cancel_all()  # 清理其他卖单.
                    # increase_pos_value = self.rinitial * self.rmultiplier ** self.current_increase_pos_times
                    price = bar.close_price
                    # vol = increase_pos_value / price
                    vol = self.rhead_fix * self.rmultiplier ** self.current_increase_pos_times
                    self.buy_order(price=price, volume=vol)

                    self.print_log(
                        msg=f'{self.logmsg_template()}: 第 {self.current_increase_pos_times} 次补单，金额: {price}，数量: {vol}')
        # self.put_event()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        if order.status == Status.ALLTRADED:
            if order.direction == Direction.LONG:
                # 买单成交.
                self.current_increase_pos_times += 1
                self.last_entry_price = order.price  # 记录上一次成绩的价格.
                self.entry_lowest = order.price
                self.buy_orders.remove(order)

        # 未成交
        elif order.status == Status.NOTTRADED:
            self.process_untraded(order)
        # 部分成交
        elif order.status == Status.PARTTRADED:
            self.process_uncompleted_trade(order)
        # 撤销
        elif order.status == Status.CANCELLED or order.status == Status.REJECTED:
            self.process_cancel(order)
            status_name = "撤单"
            status = 3
            if order.status == Status("拒单"):
                status_name = "拒单"
                status = 4
            # 更改数据库订单状态 撤销 或 拒单
            self.save_cancel_data(vt_orderid=order.vt_orderid, trade_status=status_name, status=status,
                                  cost=self.avg_price)

        if not order.is_active():
            if order.vt_orderid in self.sell_orders:
                self.sell_orders.remove(order.vt_orderid)

            elif order.vt_orderid in self.buy_orders:
                self.buy_orders.remove(order.vt_orderid)

        # self.put_event()  # 更新UI使用.

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        if trade.direction == Direction.LONG:
            total = self.avg_price * self.current_pos + trade.price * trade.volume
            self.current_pos += trade.volume
            self.avg_price = total / self.current_pos
        elif trade.direction == Direction.SHORT:
            self.current_pos -= trade.volume

            # 计算统计下总体的利润.
            self.total_profit += (trade.price - self.avg_price) * trade.volume - trade.volume * trade.price * 2 * self.rtrading_fee

        self.current_pos = float(Decimal(self.current_pos).quantize(Decimal('0.00000')))

        self.save_trade_data(vt_orderid=trade.vt_orderid, vt_tradeid=trade.vt_tradeid, trade_price=trade.price,
                             trade_volume=trade.volume, cost=self.avg_price)
        # self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass

    def buy_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        if not self.validate_number(price) or not self.validate_number(volume):
            return
        vt_orderids = self.buy(price=price, volume=volume)
        for vt_orderid in vt_orderids:
            self.buy_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 开仓: {vt_orderid}，金额: {price}，数量: {volume}')
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=1,
                                 direction=1, symbol=self.tSymbol, exchange=self.tExchange,
                                 cost=self.avg_price)

    def sell_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        if not self.validate_number(price) or not self.validate_number(volume):
            return
        vt_orderids = self.sell(price=price, volume=volume)
        for vt_orderid in vt_orderids:
            self.sell_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 平仓: {vt_orderid}，金额: {price}，数量: {volume}')
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=-1,
                                 direction=-1, symbol=self.tSymbol, exchange=self.tExchange,
                                 cost=self.avg_price)

    def process_untraded(self, order):
        self.buy_orders.event_untraded(order)
        self.print_log(msg=f'{self.logmsg_template()}: 未成交: {order.orderid}')

    def process_uncompleted_trade(self, order):
        self.buy_orders.event_uncompleted_trade(order)
        self.print_log(msg=f'{self.logmsg_template()}: 部分成交: {order.orderid}')

    def process_cancel(self, order):
        self.buy_orders.event_canceled(order)
        self.sell_orders.event_canceled(order)
        self.print_log(msg=f'{self.logmsg_template()}: 撤单成功: {order.orderid}')

    def close_all_position(self, **kwargs):
        price = kwargs.get('price', None)
        v_rate = kwargs.get('rate', 1)
        if not price:
            if self.current_pos > 0:
                price = self.lastTick.ask_price_1
                self.sell_order(price=price, volume=abs(self.current_pos) * v_rate)
            if self.current_pos < 0:
                price = self.lastTick.bid_price_1
                self.buy_order(price=price, volume=abs(self.current_pos) * v_rate)
        self.manually_close_pos_flag = True
        return True

    def record(self):
        data = {'pos': self.current_pos, 'cost': self.avg_price, 'last_price': self.last_entry_price, 'min_price': self.entry_lowest, 'supply_times': self.current_increase_pos_times, 'buy_list': [], 'sell_list': []}

        for each in self.buy_orders.untraded_orders():
            data['buy_list'].append({
                'price': each.price, 'volume': each.volume, 'direction': 1, 'offset': 1
            })
        for each in self.sell_orders.untraded_orders():
            data['sell_list'].append({
                'price': each.price, 'volume': each.volume, 'direction': -1, 'offset': -1
            })
        return data

    def load(self, **kwargs):
        data = kwargs.get('data', None)
        if not data:
            return
        self.pos = data.get('pos', 0)
        self.avg_price = data.get('cost', 0)
        self.current_pos = self.pos
        self.previous_pos = self.pos
        self.current_increase_pos_times = data.get('supply_times', 0)
        self.last_entry_price = data.get('last_price', 0)  # 记录上一次成绩的价格.
        self.entry_lowest = data.get('min_price', 0)
        for each in data.get('buy_list', []):
            self.buy_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))
        for each in data.get('sell_list', []):
            self.sell_orders.append(
                oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))
        return True

    def validate_number(self, number):
        if Decimal(str(number)).quantize(Decimal('0.00000')) <= Decimal('0'):
            self.print_log(msg=f'{self.logmsg_template()}: 下单参数 {number} 异常')
            return False
        return True