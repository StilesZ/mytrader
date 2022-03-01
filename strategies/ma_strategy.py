#  -*- coding:utf-8 -*-
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


class MaStrategy(CtaTemplate):

    head_fix = 0.001  # 头寸
    order_direction = 0
    profit = 2    # 止盈
    loss = 1    # 止损
    fast_window = 7    # 快速均线窗口
    slow_window = 25    # 慢速均线窗口

    fast_ma0 = 0.0
    fast_ma1 = 0.0

    slow_ma0 = 0.0
    slow_ma1 = 0.0

    parameters = ['fast_window', 'slow_window', 'head_fix', 'order_direction', 'loss', 'profit']

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        self.buy_orders = OrderManager()
        self.sell_orders = OrderManager()
        self.holds = []
        self.tSymbol, self.tExchange = self.vt_symbol.split('.')
        self.lastTick = TickData(
            symbol=self.tSymbol,
            exchange=Exchange(self.tExchange),
            datetime=datetime.datetime.now(),
            gateway_name=self.tExchange
        )
        self.manually_close_pos_flag = False

    def init_setting(self):
        self.rhead_fix = float(self.head_fix)
        self.rfast_window = int(self.fast_window)
        self.rslow_window = int(self.slow_window)
        self.rloss_point = float(self.loss) / 100
        self.rprofit_point = float(self.profit) / 100

    def on_init(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略初始化')
        self.load_tick(2)

    def on_start(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略启动')

    def on_stop(self):
        self.print_log(msg=f'{self.logmsg_template()}: 策略停止')

    def on_tick(self, tick: TickData):
        if self.lastTick.last_price != tick.last_price:
            self.lastTick = tick
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        if not self.pos and int(self.order_direction) != 0:
            return

        self.init_setting()
        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        fast_ma = am.sma(self.rfast_window, array=True)
        self.fast_ma0 = fast_ma[-1]
        self.fast_ma1 = fast_ma[-2]

        slow_ma = am.sma(self.rslow_window, array=True)
        self.slow_ma0 = slow_ma[-1]
        self.slow_ma1 = slow_ma[-2]

        cross_over = self.fast_ma0 > self.slow_ma0 and self.fast_ma1 < self.slow_ma1
        cross_below = self.fast_ma0 < self.slow_ma0 and self.fast_ma1 > self.slow_ma1

        if self.trading:
            self.print_log(msg=f'{self.logmsg_template()}: 参数 fast_ma0:{self.fast_ma0}, fast_ma1:{self.fast_ma1}, '
                               f'slow_ma0:{self.slow_ma0}, slow_ma1:{self.slow_ma1}, cross_over:{cross_over}, cross_below:{cross_below}')

        if self.pos == 0:
            if self.buy_orders.empty():
                if cross_over:
                    self.buy_order(price=bar.close_price, volume=self.rhead_fix)
            else:
                self.cancel_untraded()
        elif self.pos > 0:
            cost = self.cost()
            last_price = self.lastTick.last_price
            last_order = self.buy_orders[-1]
            if last_order.status == 0.5:
                t = datetime.datetime.now() - last_order.create_time
                if t.seconds / 60 >= 5:
                    self.cancel_order(vt_orderid=last_order.vt_orderid)
                    self.sell_order(price=self.lastTick.bid_price_1, volume=abs(self.pos))
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，{last_order.vt_orderid}超过5分钟未完全成交')
            elif cross_below or last_price >= cost * (1 + self.rprofit_point):
                # 最后成交价 大于 成本价 或 死线下跌
                for order in self.sell_orders:
                    self.cancel_order(vt_orderid=order.vt_orderid)
                price = max(last_price, cost)
                self.sell_order(price=price, volume=abs(self.pos))    # 止盈单
                self.print_log(msg=f'{self.logmsg_template()}: 止盈，行情: {last_price}，成本: {cost}')
            elif last_price < cost:
                # 亏损率达到设定值 止损
                temp = cost - last_price
                if float(temp / cost) >= self.rloss_point:  # 超过亏损率
                    for order in self.sell_orders:
                        self.cancel_order(vt_orderid=order.vt_orderid)
                    self.sell_order(price=self.lastTick.bid_price_1, volume=abs(self.pos))   # 止损单
                    self.print_log(msg=f'{self.logmsg_template()}: 强制平仓，超过亏损率；行情: {last_price}，成本: {cost}')

    def on_order(self, order: OrderData):
        print(f'Order推送 Strategy No<{self.stra_no}>:', order)
        if order.status == Status("未成交"):
            self.process_untraded(order)
        elif order.status == Status("部分成交"):
            self.process_uncompleted_trade(order)
        elif order.status == Status("已撤销") or order.status == Status("拒单"):
            self.process_cancel(order)
            status_name = "撤单"
            status = 3
            if order.status == Status("拒单"):
                status_name = "拒单"
                status = 4
            self.save_cancel_data(vt_orderid=order.vt_orderid, trade_status=status_name, status=status, cost=self.cost())

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
                self.buy_orders = OrderManager()
                self.sell_orders = OrderManager()
                self.trading = True
                self.pos = 0
                self.previous_pos = 0
                self.print_log(msg=f'{self.logmsg_template()}: 平仓成交: {trade.orderid} -- {trade.tradeid}，'
                                   f'成交价: {trade.price}， 数量: {trade.volume}，方向: {trade.direction}，仓位: {self.pos}')
        self.save_trade_data(vt_orderid=trade.vt_orderid, vt_tradeid=trade.vt_tradeid, trade_price=trade.price,
                             trade_volume=trade.volume, cost=self.cost())

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
                                 cost=self.cost())

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
                                 cost=self.cost())

    def process_traded(self, trade):
        self.holds.append(
            oTrade(vt_orderid=trade.vt_orderid, vt_tradeid=trade.vt_tradeid, price=trade.price, volume=trade.volume)
        )
        # self.buy_orders.append(oTrade(vt_orderid=trade.vt_orderid, price=trade.price, volume=trade.volume))
        self.buy_orders.event_traded(trade)

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
        return

    # 撤销买入订单
    def cancel_untraded(self):
        for each in self.buy_orders.untraded_orders():
            self.cancel_order(vt_orderid=each.vt_orderid)
            self.print_log(msg=f'{self.logmsg_template()}: 撤单: {each.vt_orderid}')

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

    def close_all_position(self, **kwargs):
        price = kwargs.get('price', None)
        v_rate = kwargs.get('rate', 1)
        if not price:
            if self.pos > 0:
                price = self.lastTick.bid_price_1
        self.sell_order(price=price, volume=abs(self.pos) * v_rate)
        self.manually_close_pos_flag = True
        return True

    def record(self):
        data = {'pos': self.pos, 'traded_list': []}
        if self.pos:
            for each in self.holds:
                data['traded_list'].append({'price': each.price, 'volume': each.volume})
        return data

    def load(self, **kwargs):
        data = kwargs.get('data', None)
        if not data:
            return
        self.pos = data.get('pos', 0)
        self.previous_pos = self.pos
        for each in data.get('traded_list', []):
            self.holds.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))
            self.buy_orders.append(oTrade(price=each['price'], trade_price=each['price'], volume=abs(each['volume']), status=1))
        return True

    def validate_number(self, number):
        if Decimal(str(number)).quantize(Decimal('0.00000')) <= Decimal('0'):
            if self.trading:
                self.print_log(msg=f'{self.logmsg_template()}: 下单参数 {number} 异常')
            return False
        return True
