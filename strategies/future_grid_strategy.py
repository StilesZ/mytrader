# -*- coding:utf-8 -*-
import datetime
from decimal import Decimal

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

    def remove(self, o: TradeData):
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


class GridPositionCalculator(object):
    """
    用来计算网格头寸的平均价格
    Use for calculating the grid position's average price.

    :param grid_step: 网格间隙.
    """

    def __init__(self, grid_step=1.0):
        self.pos = 0
        self.avg_price = 0
        self.profit = 0
        self.grid_step = grid_step

    def update_kwargs(self, **kwargs):
        self.pos = kwargs.get('pos', 0)
        self.avg_price = kwargs.get('avg_price', 0)
        self.profit = kwargs.get('profit', 0)

    def update_position(self, order: OrderData):
        if order.status != Status.ALLTRADED:
            return

        previous_pos = self.pos
        previous_avg = self.avg_price

        if order.direction == Direction.LONG:
            self.pos += order.volume
            self.pos = float(Decimal(self.pos).quantize(Decimal('0.00000')))

            if self.pos == 0:
                self.avg_price = 0
            else:

                if previous_pos == 0:
                    self.avg_price = order.price

                elif previous_pos > 0:
                    self.avg_price = (previous_pos * previous_avg + order.volume * order.price) / abs(self.pos)

                elif previous_pos < 0 and self.pos < 0:
                    self.avg_price = (previous_avg * abs(self.pos) - (
                            order.price - previous_avg) * order.volume - order.volume * self.grid_step) / abs(
                        self.pos)

                elif previous_pos < 0 < self.pos:
                    self.avg_price = order.price

        elif order.direction == Direction.SHORT:
            self.pos -= order.volume
            self.pos = float(Decimal(self.pos).quantize(Decimal('0.00000')))

            if self.pos == 0:
                self.avg_price = 0
            else:

                if previous_pos == 0:
                    self.avg_price = order.price

                elif previous_pos < 0:
                    self.avg_price = (abs(previous_pos) * previous_avg + order.volume * order.price) / abs(self.pos)

                elif previous_pos > 0 and self.pos > 0:
                    self.avg_price = (previous_avg * self.pos - (
                            order.price - previous_avg) * order.volume + order.volume * self.grid_step) / abs(
                        self.pos)

                elif previous_pos > 0 > self.pos:
                    self.avg_price = order.price


class FutureGridStrategy(CtaTemplate):

    grid_step = 2.0  # 网格间隙. 买一价格 * 手续费 * 5
    profit_step = 2.0  # 获利的间隔.
    head_fix = 0.05  # 每次下单的头寸.  # 0.001， 0.001,  BNB 0.01, 10 USDT.
    max_pos = 7.0  # 最大的头寸数.
    profit_orders_counts = 3  # 出现多少个网格的时候，会考虑止盈.
    trailing_stop_multiplier = 2.0  # 止损 650USDT - 2 * 2  = 646, 650最后成交价  - 网格间隙 * 止损

    parameters = [
        'grid_step', 'profit_step', 'head_fix', 'max_pos', 'profit_orders_counts', 'trailing_stop_multiplier'
    ]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # K线合成器：从Tick合成分钟K线用
        self.bg = BarGenerator(self.on_bar)
        # 时间序列容器：计算技术指标用
        self.am = ArrayManager()

        # 币种 - 交易所 载入Tick数据
        self.tSymbol, self.tExchange = self.vt_symbol.split('.')
        self.lastTick = TickData(
            symbol=self.tSymbol,
            exchange=Exchange(self.tExchange),
            datetime=datetime.datetime.now(),
            gateway_name=self.tExchange
        )

        self.buy_orders = OrderManager()    # 买单委托管理
        self.sell_orders = OrderManager()   # 卖单委托管理
        self.profit_orders = OrderManager()  # 止盈
        self.stop_orders = OrderManager()  # 止损

        # 计算仓位用的对象
        self.position_calculator = GridPositionCalculator(grid_step=self.grid_step)
        self.current_pos = self.position_calculator.pos
        self.avg_price = self.position_calculator.avg_price

        self.manually_close_pos_flag = False    # 手动平仓标志
        self.trigger_stop_loss = False  # 是否触发止损。

        self.last_filled_order: OrderData = None

    def init_setting(self):

        self.rgrid_step = abs(float(self.grid_step))
        self.rhead_fix = abs(float(self.head_fix))

        self.rprofit_point = float(self.profit_step)
        self.rprofit_count = float(self.profit_orders_counts)

        self.rloss_point = float(self.trailing_stop_multiplier)

        self.rprofit_limit = int(self.max_pos)

        # 当前有仓位 且 (策略启动 或 更新策略)
        if self.pos and self.signal_update_paramters > 1:
            # self.cancel_untraded()  # 撤销补单
            # self.op_supply_order()  # 重新补单
            # self.cancel_untraded_sell()  # 撤销平仓
            # self.op_profit_order()  # 重新平仓
            self.cancel_all()

        # 策略启动 状态减一
        if self.signal_update_paramters > 0:
            self.signal_update_paramters -= 1
#        self.print_log(msg=f'{self.logmsg_template()}: 初始化策略参数')

    def on_init(self):
        # self.init_setting()
        self.print_log(msg=f'{self.logmsg_template()}: 策略初始化')
        # 加载5天的历史数据用于初始化回放
        self.load_tick(1)

    def on_start(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略启动')
        self.avg_price = self.position_calculator.avg_price
        self.current_pos = self.position_calculator.pos
        self.signal_update_paramters = 2

    def on_stop(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略停止')

    def on_tick(self, tick: TickData):
        """
        通过该函数收到Tick推送。
        :param tick:
        :return:
        """
        if self.signal_update_paramters > 0:
            self.init_setting()  # 更新策略配置

        if self.lastTick.last_price != tick.last_price:
            self.lastTick = tick

        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        # 通过该函数收到新的1分钟K线推送。
        self.am.update_bar(bar)  # 更新K线到时间序列容器中
        # 若缓存的K线数量尚不够计算技术指标 或 未选择方向 ，则直接返回
        if not self.am.inited:
            return

        self.init_setting()

        if not self.trading:
            return

        # 仓位为零的时候
        if abs(self.position_calculator.pos) < self.rhead_fix:

            if self.buy_orders.empty() and self.sell_orders.empty():

                if self.trigger_stop_loss:
                    # # 如果触发了止损就需要休息一段时间.
                    # if self.stop_order_interval < self.stop_minutes * 60:
                    #     return
                    # else:
                    #     self.stop_order_interval = 0
                    #     self.trigger_stop_loss = False
                    self.cancel_all()
                    self.trigger_stop_loss = False
                    return

                buy_price = self.lastTick.bid_price_1 - self.rgrid_step / 2
                sell_price = self.lastTick.bid_price_1 + self.rgrid_step / 2

                self.buy_order(price=buy_price, volume=self.rhead_fix)
                self.sell_order(price=sell_price, volume=self.rhead_fix)

                self.print_log(msg=f"开启网格交易，双边下单：LONG: {buy_price}, SHORT:{sell_price}")

            elif self.buy_orders.empty() or self.sell_orders.empty():
                print(f"仓位为零且单边网格没有订单, 先撤掉所有订单")
                self.cancel_all()

        # 有仓位 不存在买卖单 加仓
        elif self.rprofit_limit * self.rhead_fix > abs(self.position_calculator.pos) >= self.rhead_fix:

            if not self.buy_orders.empty() and not self.sell_orders.empty():
                return

            if self.last_filled_order:
                price = self.last_filled_order.price
            else:
                price = self.lastTick.bid_price_1

            buy_step = self.get_step()
            sell_step = self.get_step()

            buy_price = price - buy_step * self.rgrid_step
            sell_price = price + sell_step * self.rgrid_step

            buy_price = min(self.lastTick.bid_price_1, buy_price)
            sell_price = max(self.lastTick.ask_price_1, sell_price)

            self.buy_order(price=buy_price, volume=self.rhead_fix)
            self.sell_order(price=sell_price, volume=self.rhead_fix)

            self.print_log(msg=f"仓位不为零, 根据上个订单下双边网格.LONG:{buy_price}, SHORT:{sell_price}")

        # 添加止盈单
        if abs(self.position_calculator.pos) >= self.rprofit_count * self.rhead_fix and self.profit_orders.empty():

            self.print_log(msg=f"单边网格出现超过{self.rprofit_count}个订单以上,头寸为:{self.position_calculator.pos}, 考虑设置止盈的情况")

            if self.position_calculator.pos > 0:
                price = max(self.lastTick.ask_price_1 * (1 + 0.0001), self.position_calculator.avg_price + self.rprofit_point)

                self.profit_order(price=price, volume=abs(self.position_calculator.pos), pos=1)
                self.print_log(msg=f"多头止盈情况: {self.position_calculator.pos}@{price}")
            elif self.position_calculator.pos < 0:
                price = min(self.lastTick.bid_price_1 * (1 - 0.0001), self.position_calculator.avg_price - self.rprofit_point)

                self.profit_order(price=price, volume=abs(self.position_calculator.pos), pos=-1)
                self.print_log(msg=f"空头止盈情况: {self.position_calculator.pos}@{price}")

        # 添加止损单
        if abs(self.position_calculator.pos) >= self.rprofit_limit * self.rhead_fix:

            self.trigger_stop_loss = True

            for order in self.stop_orders:
                self.cancel_order(order.vt_orderid)

            if self.last_filled_order:
                if self.position_calculator.pos > 0:
                    if self.lastTick.bid_price_1 <= self.last_filled_order.price - self.rloss_point * self.rgrid_step:
                        self.stop_order(price=self.lastTick.bid_price_1,
                                        volume=abs(self.position_calculator.pos), pos=1)

                elif self.position_calculator.pos < 0:
                    if self.lastTick.ask_price_1 >= self.last_filled_order.price + self.rloss_point * self.rgrid_step:
                        self.stop_order(price=self.lastTick.ask_price_1,
                                        volume=abs(self.position_calculator.pos), pos=-1)
            else:
                if self.position_calculator.pos > 0:
                    if self.lastTick.bid_price_1 < self.position_calculator.avg_price - self.rprofit_limit * self.rgrid_step:
                        self.stop_order(price=self.lastTick.bid_price_1,
                                        volume=abs(self.position_calculator.pos), pos=1)

                elif self.position_calculator.pos < 0:
                    if self.lastTick.ask_price_1 > self.position_calculator.avg_price + self.rprofit_limit * self.rgrid_step:
                        self.stop_order(price=self.lastTick.ask_price_1,
                                        volume=abs(self.position_calculator.pos), pos=-1)

    def on_order(self, order: OrderData):
        """收到委托变化推送（必须由用户继承实现）"""
        print(f'Order推送 Strategy No<{self.stra_no}>:', order)

        self.position_calculator.update_position(order)

        self.current_pos = self.position_calculator.pos
        self.avg_price = self.position_calculator.avg_price

        # 未成交
        if order.status == Status("未成交"):
            self.process_untraded(order)
        # 部分成交
        elif order.status == Status("部分成交"):
            self.process_uncompleted_trade(order)
        # 撤销
        elif order.status == Status("已撤销") or order.status == Status("拒单"):
            self.process_cancel(order)
            status_name = "撤单"
            status = 3
            if order.status == Status("拒单"):
                status_name = "拒单"
                status = 4
            # 更改数据库订单状态 撤销 或 拒单
            self.save_cancel_data(vt_orderid=order.vt_orderid, trade_status=status_name, status=status, cost=self.avg_price)

    def on_trade(self, trade: TradeData):
        """收到成交推送（必须由用户继承实现）"""
        print(f'Trade推送 Strategy No<{self.stra_no}>:', trade)

        if self.buy_orders.event_traded(trade) or self.sell_orders.event_traded(trade):

            self.buy_orders.remove(trade)
            self.sell_orders.remove(trade)

            self.cancel_all()
            self.print_log(msg=f'{self.logmsg_template()}: 订单买卖单完全成交, 先撤销所有订单')

            self.last_filled_order = trade
            if abs(self.position_calculator.pos) < self.rhead_fix:
                print("仓位为零， 需要重新开始.")
                return

            if self.manually_close_pos_flag:
                print("手动平仓， 需要重新开始.")
                self.manually_close_pos_flag = False
                return

            # tick 存在且仓位数量还没有达到设置的最大值.
            if self.lastTick and abs(self.position_calculator.pos) < self.rprofit_limit * self.rhead_fix:
                buy_step = self.get_step()
                sell_step = self.get_step()

                # 解决步长的问题.
                buy_price = trade.price - buy_step * self.rgrid_step
                sell_price = trade.price + sell_step * self.rgrid_step

                buy_price = min(self.lastTick.bid_price_1 * (1 - 0.0001), buy_price)
                sell_price = max(self.lastTick.ask_price_1 * (1 + 0.0001), sell_price)

                self.buy_order(price=buy_price, volume=self.rhead_fix)
                self.sell_order(price=sell_price, volume=self.rhead_fix)

                self.print_log(msg=f'{self.logmsg_template()}: 订单完全成交, 分别下双边网格: LONG: {buy_price}, SHORT: {sell_price}')

        elif self.profit_orders.event_traded(trade):
            self.profit_orders.remove(trade)
            if abs(self.position_calculator.pos) < self.rhead_fix:
                self.cancel_all()
                self.print_log(msg=f'{self.logmsg_template()}: 止盈单子成交,且仓位为零, 先撤销所有订单，然后重新开始')

        elif self.stop_orders.event_traded(trade):
            self.stop_orders.remove(trade)
            if abs(self.position_calculator.pos) < self.rhead_fix:
                self.trigger_stop_loss = False
                self.cancel_all()
                self.print_log(msg=f'{self.logmsg_template()}: 止损单子成交,且仓位为零, 先撤销所有订单，然后重新开始')

        self.save_trade_data(vt_orderid=trade.vt_orderid, vt_tradeid=trade.vt_tradeid, trade_price=trade.price,
                             trade_volume=trade.volume, cost=self.avg_price)

    # 买入开仓
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
            # 订单记录到数据库
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=1,
                                 direction=1, symbol=self.tSymbol, exchange=self.tExchange, cost=self.avg_price)

    # 卖出平仓
    def sell_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        if not self.validate_number(price) or not self.validate_number(volume):
            return
        vt_orderids = self.short(price=price, volume=volume)
        for vt_orderid in vt_orderids:
            self.sell_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 平仓: {vt_orderid}，金额: {price}，数量: {volume}')
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=1,
                                 direction=-1, symbol=self.tSymbol, exchange=self.tExchange, cost=self.avg_price)

    # 止盈
    def profit_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        ma_trend = kwargs['pos']
        if not self.validate_number(price) or not self.validate_number(volume):
            return

        if ma_trend > 0:
            vt_orderids = self.short(price=price, volume=volume)
        elif ma_trend < 0:
            vt_orderids = self.buy(price=price, volume=volume)

        for vt_orderid in vt_orderids:
            self.profit_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 止盈: {vt_orderid}，金额: {price}，数量: {volume}')
            # 订单记录到数据库
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=-1,
                                 direction=-ma_trend, symbol=self.tSymbol, exchange=self.tExchange, cost=self.avg_price)

    # 止损
    def stop_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        ma_trend = kwargs['pos']

        if not self.validate_number(price) or not self.validate_number(volume):
            return

        if ma_trend > 0:
            vt_orderids = self.short(price=price, volume=volume)
        elif ma_trend < 0:
            vt_orderids = self.buy(price=price, volume=volume)

        for vt_orderid in vt_orderids:
            self.stop_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 开仓: {vt_orderid}，金额: {price}，数量: {volume}')
            # 订单记录到数据库
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=-1,
                                 direction=-ma_trend, symbol=self.tSymbol, exchange=self.tExchange, cost=self.avg_price)

    # 未成交 设置订单状态 0
    def process_untraded(self, order):
        self.buy_orders.event_untraded(order)
        self.sell_orders.event_untraded(order)
        self.profit_orders.event_untraded(order)
        self.stop_orders.event_untraded(order)
        self.print_log(msg=f'{self.logmsg_template()}: 未成交: {order.orderid}')

    # 部分成交 设置订单状态 0.5
    def process_uncompleted_trade(self, order):
        self.buy_orders.event_uncompleted_trade(order)
        self.sell_orders.event_uncompleted_trade(order)
        self.profit_orders.event_uncompleted_trade(order)
        self.stop_orders.event_uncompleted_trade(order)
        self.print_log(msg=f'{self.logmsg_template()}: 部分成交: {order.orderid}')

    def process_submitting(self, order):
        return

    # 撤销
    def process_cancel(self, order):
        self.buy_orders.event_canceled(order)
        self.sell_orders.event_canceled(order)
        self.profit_orders.event_canceled(order)
        self.stop_orders.event_canceled(order)
        self.print_log(msg=f'{self.logmsg_template()}: 撤单成功: {order.orderid}')

    # 平仓 用户操作策略号
    def close_all_position(self, **kwargs):
        price = kwargs.get('price', None)
        v_rate = kwargs.get('rate', 1)
        if not price:
            if self.position_calculator.pos > 0:
                price = self.lastTick.bid_price_1
                self.sell_order(price=price, volume=abs(self.position_calculator.pos) * v_rate)
            elif self.position_calculator.pos < 0:
                price = self.lastTick.ask_price_1
                self.buy_order(price=price, volume=abs(self.position_calculator.pos) * v_rate)

        self.manually_close_pos_flag = True
        return True

    # 暂停记录策略信息
    def record(self):
        data = {'pos': self.position_calculator.pos, 'avg_price': self.position_calculator.avg_price, 'profit': self.position_calculator.profit, 'order': self.last_filled_order, 'buy_list': [], 'sell_list': [], 'profit_list': [], 'stop_list': []}
        for each in self.buy_orders.untraded_orders():
            data['buy_list'].append({
                'price': each.price, 'volume': each.volume, 'offset': 1
            })
        for each in self.sell_orders.untraded_orders():
            data['sell_list'].append({
                'price': each.price, 'volume': each.volume, 'offset': -1
            })
        for each in self.profit_orders.untraded_orders():
            data['profit_list'].append({
                'price': each.price, 'volume': each.volume, 'offset': -1
            })
        for each in self.stop_orders.untraded_orders():
            data['stop_list'].append({
                'price': each.price, 'volume': each.volume, 'offset': -1
            })
        return data

    # 加载记录策略信息
    def load(self, **kwargs):
        # self.print_log(msg=f'{self.logmsg_template()}: 加载历史委托: {data}')
        data = kwargs.get('data', None)
        if not data:
            return
        self.pos = data.get('pos', 0)
        avg_price = data.get('avg_price', 0)
        profit = data.get('profit', 0)
        self.previous_pos = self.pos
        self.position_calculator.update_kwargs(pos=self.pos, avg_price=avg_price, profit=profit)
        self.last_filled_order = data.get('order', None)

        for each in data.get('buy_list', []):
            self.buy_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))

        for each in data.get('sell_list', []):
            self.sell_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))

        for each in data.get('profit_list', []):
            self.profit_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))

        for each in data.get('stop_list', []):
            self.stop_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))

    def validate_number(self, number):
        if Decimal(str(number)).quantize(Decimal('0.00000')) <= Decimal('0'):
            self.print_log(msg=f'{self.logmsg_template()}: 下单参数 {number} 异常')
            return False
        return True

    def get_step(self):
        return 1
