# -*- coding:utf-8 -*-
import datetime
import traceback
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

    def append(self, o: oTrade):
        self.orders.append(o)

    def empty(self):
        if not self.orders:
            return True
        return False

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
            if -2 < order.status < 1:
                yield order

    def last_trade(self):
        index_ = -1
        while True:
            try:
                last_ = self.orders[index_]
            except Exception as e:
                print('委托搜索失败，', e)
                break
            if last_.status == 1:
                return last_
            index_ -= 1
        return False

class TestGridStrategy2(CtaTemplate):

    order_direction = '0'
    # 多头
    head_fix_long = '100'   # 头寸
    profit_long = '50'  # 止盈
    loss_long = '9' # 止损%
    profit_limit_long = '8' # 止盈变更
    supply_data_long = ''
    # 空头
    head_fix_short = '100'
    profit_short = '50'
    loss_short = '9'
    profit_limit_short = '5'
    supply_data_short = ''

    parameters = [
        'order_direction',
        'head_fix_long', 'profit_long', 'loss_long', 'profit_limit_long', 'supply_data_long',
        'head_fix_short', 'profit_short', 'loss_short', 'profit_limit_short', 'supply_data_short'
    ]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting, strategy_no, gateway_key):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting, strategy_no, gateway_key)
        supply_data_long = [{'supply_num': '1', 'supply_step': '200', 'supply_fix': '200'},
                            {'supply_num': '2', 'supply_step': '200', 'supply_fix': '200'}]
        supply_data_short = [{'supply_num': '1', 'supply_step': '200', 'supply_fix': '200'},
                             {'supply_num': '2', 'supply_step': '200', 'supply_fix': '200'}]

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()
        self.bg15 = BarGenerator(self.on_bar, 15, self.on_15min_bar)
        self.am15 = ArrayManager()

        self.tSymbol, self.tExchange = self.vt_symbol.split('.')
        self.lastTick = TickData(
            symbol=self.tSymbol,
            exchange=Exchange(self.tExchange),
            datetime=datetime.datetime.now(),
            gateway_name=self.tExchange
        )
        self.supply_price = []
        self.buy_orders = OrderManager()    # 买单委托管理
        self.sell_orders = OrderManager()   # 卖单委托管理
        self.holds = [] # 持仓管理
        self.ma_trend = None
        self.count = 0
        self.fast_ma = None
        self.slow_ma = None
        self.fast_window = 5
        self.slow_window = 20

    def init_setting(self):
        if self.ma_trend == 1:
            self.rhead_fix = abs(float(self.head_fix_long))
            self.rprofit_point = float(self.profit_long)
            self.rloss_point = float(self.loss_long) / 100
            self.rsupply_data = self.supply_data_long
            self.rprofit_limit = int(self.profit_limit_long)
        elif self.ma_trend == -1:
            self.rhead_fix = abs(float(self.head_fix_short))
            self.rprofit_point = float(self.profit_short)
            self.rloss_point = float(self.loss_short) / 100
            self.rsupply_data = self.supply_data_short
            self.rprofit_limit = int(self.profit_limit_short)
        if self.pos and self.signal_update_paramters > 1:
            self.cancel_untraded()  # 撤销补单
            self.op_supply_order()  # 重新补单
            self.cancel_untraded_sell() # 撤销平仓
            self.op_profit_order()  # 重新平仓
        if self.signal_update_paramters > 0:
            self.signal_update_paramters -= 1
#        self.print_log(msg=f'{self.logmsg_template()}: 初始化策略参数')

    def on_init(self):
        # self.init_setting()
        self.print_log(msg=f'{self.logmsg_template()}: 策略初始化')
        self.load_bar(5)

    def on_start(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略启动')
        self.signal_update_paramters = 2

    def on_stop(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略停止')

    def on_tick(self, tick: TickData):
        if self.signal_update_paramters > 0:
            self.init_setting()  # 更新策略配置
        if self.lastTick.last_price != tick.last_price:
            self.lastTick = tick
            last_price = tick.last_price
            if Decimal(self.pos).quantize(Decimal('0.00000')) != Decimal('0') and self.count >= self.rprofit_limit:
                cost = self.cost()
                if self.ma_trend == 1 and cost <= last_price:
                    self.sell_order(price=last_price, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，超过最大补仓数')
                elif self.ma_trend == -1 and cost >= last_price:
                    self.sell_order(price=last_price, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，超过最大补仓数')
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.make_direction()
        self.bg15.update_bar(bar)
        self.am.update_bar(bar)
        if not self.am.inited or not self.ma_trend:
            return
        self.init_setting()
        self.op()

    def on_15min_bar(self, bar: BarData):
        self.am15.update_bar(bar)
        if int(self.order_direction) or not self.am15.inited or self.pos or int(self.order_direction) == 2:
            return

        self.fast_ma = self.am15.sma(self.fast_window)
        self.slow_ma = self.am15.sma(self.slow_window)
        if self.fast_ma >= self.slow_ma:
            # self.print_log(msg=f'{self.logmsg_template()}: 选择方向 -> 做多')
            self.set_long_direction()
        else:
            # self.print_log(msg=f'{self.logmsg_template()}: 选择方向 -> 做空')
            self.set_short_direction()

    def make_direction(self):
        if self.pos or not self.buy_orders.empty():
            return
        if int(self.order_direction) == 1:
            self.set_long_direction()
        elif int(self.order_direction) == -1:
            self.set_short_direction()

    def on_order(self, order: OrderData):
        print(f'Order推送 Strategy No<{self.stra_no}>:', order)
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
            self.save_cancel_data(vt_orderid=order.vt_orderid, trade_status=status_name, status=status, cost=self.cost())
        # 拒单
        # elif order.status == Status("拒单"):
        #     self.process_rejected(order)
        #     self.save_Cancel_data(vt_orderid=order.vt_orderid, trade_status='拒单')

    def on_trade(self, trade: TradeData):
        print(f'Trade推送 Strategy No<{self.stra_no}>:', trade)
        trade.offset = self.position_to_offset()
        if trade.offset == Offset("开"):
            self.process_traded(trade)
            self.print_log(msg=f'{self.logmsg_template()}: 开仓成交: {trade.orderid} -- {trade.tradeid}，'
                               f'成交价: {trade.price}， 数量: {trade.volume}, 方向: {trade.direction}')
        else:
            self.print_log(msg=f'{self.logmsg_template()}: 平仓后，仓位 {self.pos}，订单 {trade}')
            if Decimal(self.pos).quantize(Decimal('0.00000')) == Decimal('0'):
                self.cancel_all()
                self.holds = []
                self.supply_price = []
                self.buy_orders = OrderManager()
                self.sell_orders = OrderManager()
                self.ma_trend = None  # 方向重置
                self.trading = True
                self.count = 0
                self.pos = 0
                self.previous_pos = 0
                self.print_log(msg=f'{self.logmsg_template()}: 平仓成交: {trade.orderid} -- {trade.tradeid}，'
                                   f'成交价: {trade.price}， 数量: {trade.volume}，方向: {trade.direction}，仓位: {self.pos}')
        self.save_trade_data(vt_orderid=trade.vt_orderid, vt_tradeid=trade.vt_tradeid, trade_price=trade.price,
                             trade_volume=trade.volume, cost=self.cost())

    def buy_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        vt_orderids = self.actions['entry'](price=price, volume=volume)
        for vt_orderid in vt_orderids:
            self.buy_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 开仓: {vt_orderid}，金额: {price}，数量: {volume}')
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=1,
                                 direction=self.ma_trend, symbol=self.tSymbol, exchange=self.tExchange, cost=self.cost())

    def sell_order(self, **kwargs):
        price = kwargs['price']
        volume = kwargs['volume']
        vt_orderids = self.actions['exit'](price=price, volume=volume)
        for vt_orderid in vt_orderids:
            self.sell_orders.append(
                oTrade(vt_orderid=vt_orderid, price=price, volume=volume)
            )
            self.print_log(msg=f'{self.logmsg_template()}: 平仓: {vt_orderid}，金额: {price}，数量: {volume}')
            self.save_order_data(vt_orderid=vt_orderid, order_price=price, order_volume=volume, offset=-1,
                                 direction=-(self.ma_trend), symbol=self.tSymbol, exchange=self.tExchange, cost=self.cost())

    def process_traded(self, trade):
        flag = False
        self.holds.append(
            oTrade(vt_orderid=trade.vt_orderid, vt_tradeid=trade.vt_tradeid, price=trade.price, volume=trade.volume)
        )
        result = self.buy_orders.event_traded(trade)
        if result:
            self.count += 1
            flag = True
        if self.count > 0 and flag:
            self.op_supply_order()  # 开一笔补单
            self.op_profit_order()  # 开一笔止盈单

    def process_untraded(self, order):
        self.buy_orders.event_untraded(order)
        self.print_log(msg=f'{self.logmsg_template()}: 未成交: {order.orderid}')

    def process_uncompleted_trade(self, order):
        self.buy_orders.event_uncompleted_trade(order)
        self.print_log(msg=f'{self.logmsg_template()}: 部分成交: {order.orderid}')

    def process_submitting(self, order):
        return

    def process_cancel(self, order):
        self.buy_orders.event_canceled(order)
        self.sell_orders.event_canceled(order)
        self.print_log(msg=f'{self.logmsg_template()}: 撤单成功: {order.orderid}')

    def process_rejected(self, order):
        self.buy_orders.event_canceled(order)
        if order.direction == Direction("多"):
            trend = 1
        else:
            trend = -1
        if trend == self.ma_trend:
            self.buy_order(price=order.price, volume=abs(order.volume))
        else:
            self.sell_order(price=order.price, volume=abs(order.volume))
        self.print_log(msg=f'{self.logmsg_template()}: 拒单: {order.orderid}')

    def cancel_untraded(self):
        for each in self.buy_orders.untraded_orders():
            self.cancel_order(vt_orderid=each.vt_orderid)
            self.print_log(msg=f'{self.logmsg_template()}: 撤单: {each.vt_orderid}')

    def cancel_untraded_sell(self):
        for each in self.sell_orders.untraded_orders():
            self.cancel_order(vt_orderid=each.vt_orderid)
            self.print_log(msg=f'{self.logmsg_template()}: 卖单撤销: {each.vt_orderid}')

    # 多方向的处理
    def op_long(self):
        if self.pos == 0:
            if self.buy_orders.empty():
                self.ropen_price = self.lastTick.ask_price_1
                self.buy_order(price=self.ropen_price, volume=self.rhead_fix)
                if self.trading:
                    self.print_log(msg=f'{self.logmsg_template()}: 方向：多，开仓价：{self.ropen_price}，'
                                       f'补单价：{self.supply_price}')
            else:
                self.cancel_untraded()
        else:
            if not self.holds:
                return
            cost = self.cost()
            last_price = self.lastTick.last_price
            last_order = self.buy_orders[-1]
            if last_order.status == 0.5:
                t = datetime.datetime.now() - last_order.create_time
                if t.seconds / 60 >= 5:
                    self.cancel_order(vt_orderid=last_order.vt_orderid)
                    self.sell_order(price=self.lastTick.bid_price_1, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，{last_order.vt_orderid}超过5分钟未完全成交')
            elif last_price < cost:
                temp = cost - last_price
                if float(temp/cost) >= self.rloss_point:    # 超过亏损率
                    self.sell_order(price=self.lastTick.bid_price_1, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，超过亏损率；行情: {last_price}，成本: {cost}')

    # 空方向的处理
    def op_short(self):
        if self.pos == 0:
            if self.buy_orders.empty():
                self.ropen_price = self.lastTick.bid_price_1
                self.buy_order(price=self.ropen_price, volume=self.rhead_fix)
                if self.trading:
                    self.print_log(msg=f'{self.logmsg_template()}: 方向：空，开仓价：{self.ropen_price}，'
                                       f'补单价：{self.supply_price}')
            else:
                self.cancel_untraded()
        else:
            if not self.holds:
                return
            cost = self.cost()
            last_price = self.lastTick.last_price
            last_order = self.buy_orders[-1]
            if last_order.status == 0.5:
                t = datetime.datetime.now() - last_order.create_time
                if t.seconds / 60 >= 5:
                    self.cancel_order(vt_orderid=last_order.vt_orderid)
                    self.sell_order(price=self.lastTick.ask_price_1, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，超过5分钟未完全成交')
            elif last_price > cost:
                temp = last_price - cost
                if float(temp/cost) >= self.rloss_point:    # 超过亏损率
                    self.sell_order(price=self.lastTick.ask_price_1, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，超过亏损率；行情: {last_price}，成本: {cost}')

    def op_profit_order(self):
        if not self.holds:
            return
        if self.ma_trend == 1:
            price = self.cost() + self.rprofit_point
            self.sell_order(price=price, volume=abs(self.pos))
        elif self.ma_trend == -1:
            price = self.cost() - self.rprofit_point
            self.sell_order(price=price, volume=abs(self.pos))

    def op_supply_order(self):
        try:
            data = self.rsupply_data[self.count-1]
            last_trade = self.buy_orders.last_trade()
            if not last_trade:
                return False
            if self.ma_trend == 1:
                price = last_trade.trade_price - float(data['supply_step'])
            else:
                price = last_trade.trade_price + float(data['supply_step'])
            self.buy_order(price=price, volume=float(data['supply_fix']))
        except Exception as e:
            print('补单失败, ', e)

    def trade_volume(self):
        volumes = [o.volume for o in self.holds]
        return sum(volumes)

    def trade_amount(self):
        amounts = [o.volume * o.price for o in self.holds]
        return sum(amounts)

    def cost(self):
        if not self.pos:
            return 0
        return self.trade_amount() / self.trade_volume()

    def set_long_direction(self):
        self.ma_trend = 1
        self.op = self.op_long
        self.actions = {
            'entry': self.buy,
            'exit': self.sell
        }

    def set_short_direction(self):
        self.ma_trend = -1
        self.op = self.op_short
        self.actions = {
            'entry': self.short,
            'exit': self.cover
        }

    def close_all_position(self, **kwargs):
        price = kwargs.get('price', None)
        if not price:
            if self.pos > 0:
                price = self.lastTick.bid_price_1
            elif self.pos < 0:
                price = self.lastTick.ask_price_1
        self.sell_order(price=price, volume=abs(self.pos))
        return True

    def record(self):
        try:
            data = {'pos': self.pos, 'direction': self.ma_trend, 'supply_times': self.count, 'untraded_list': [], 'traded_list': []}
            for each in self.buy_orders.untraded_orders():
                data['untraded_list'].append({
                    'price': each.price, 'volume': each.volume, 'direction': self.ma_trend, 'offset': 1
                })
            for each in self.sell_orders.untraded_orders():
                data['untraded_list'].append({
                    'price': each.price, 'volume': each.volume, 'direction': -self.ma_trend, 'offset': -1
                })
            if self.pos:
                data['traded_list'].append({
                    'price': self.cost(), 'volume': abs(self.pos), 'direction': self.ma_trend
                })
            self.print_log(msg=f'{self.logmsg_template()}: 记录持仓情况: {data}')
        except Exception as e:
            data = {}
            self.print_log(msg=f'{self.logmsg_template()}: 记录持仓失败, {e}')
        return data

    def load(self, **kwargs):
        data = kwargs.get('data', None)
        self.print_log(msg=f'{self.logmsg_template()}: 加载历史委托: {data}')
        if not data:
            return
        try:
            self.pos = data.get('pos', 0)
            self.count = data.get('supply_times', 0)
            self.ma_trend = data.get('direction', None)
            if self.ma_trend == 1:
                self.set_long_direction()
            elif self.ma_trend == -1:
                self.set_short_direction()
            for each in data.get('traded_list', []):
                self.holds.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))
                self.buy_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))
            for each in data.get('untraded_list', []):
                offset = each['offset']
                if offset == 1:
                    self.buy_order(price=each['price'], volume=each['volume'])
                elif offset == -1:
                    self.sell_order(price=each['price'], volume=each['volume'])
        except Exception as e:
            self.print_log(msg=f'{self.logmsg_template()}: 加载历史委托失败，{e}')
