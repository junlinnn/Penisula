import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pprint import pprint
import random
import time
import os
import requests
import socket
import datetime as dt
import bisect

np.random.seed(1)
random.seed(1)


def timenow():
    return dt.datetime.now().strftime(format="%Y%m%d%H%M%S%f")

# Server


class OrderBook:
    def __init__(self, secId, book=None):
        self.secId = secId
        if book is None:
            # Empty Order Book
            self.book = {
                'timestamp': timenow(),
                'bids': {
                    'orderId': [],
                    'timestamp': [],
                    'price': [],
                    'quantity': []
                },
                'asks': {
                    'orderId': [],
                    'timestamp': [],
                    'price': [],
                    'quantity': []
                }
            }
        else:
            self.book = book

    def handle_order(self, order):
        if order.orderType == 'L':
            res = self._handle_limit(order)
        elif order.orderType == 'M':
            res = self._handle_market(order)
        elif order.orderType == 'C':
            res = self._handle_cancel(order)
        else:
            res = {
                'code': 400,
                'message': f"orderType {order.orderType} unsupported"
            }
        return res

    def _handle_limit(self, order):
        if order.quantity > 0:
            idx = bisect.bisect_left(self.book['bids']['price'], order.price)
            if idx == len(self.book['bids']['price']) or order.price != self.book['bids']['price'][idx]:
                self.book['bids']['price'].insert(idx, order.price)
                self.book['bids']['orderId'].insert(idx, [order.orderId])
                self.book['bids']['timestamp'].insert(
                    idx, [order.set_timestamp()])
                self.book['bids']['quantity'].insert(idx, [order.quantity])
            else:
                self.book['bids']['orderId'][idx].append(order.orderId)
                self.book['bids']['timestamp'][idx].append(
                    order.set_timestamp())
                self.book['bids']['quantity'][idx].append(order.quantity)
        elif order.quantity < 0:
            idx = bisect.bisect_left(self.book['asks']['price'], order.price)
            if idx == len(self.book['asks']['price']) or order.price != self.book['asks']['price'][idx]:
                self.book['asks']['price'].insert(idx, order.price)
                self.book['asks']['orderId'].insert(idx, [order.orderId])
                self.book['asks']['timestamp'].insert(
                    idx, [order.set_timestamp()])
                self.book['asks']['quantity'].insert(idx, [-order.quantity])
            else:
                self.book['asks']['orderId'][idx].append(order.orderId)
                self.book['asks']['timestamp'][idx].append(
                    order.set_timestamp())
                self.book['asks']['quantity'][idx].append(-order.quantity)

        else:
            res = {'code': 400, 'timestamp': order.set_timestamp(),
                   'message': "Quantity can not be 0"}
            return res
        self.book['timestamp'] = timenow()
        res = {'code': 200, 'timestamp': order.timestamp,
               'message': "limit order ok"}
        return res

    def _handle_market(self, order):
        if order.quantity > 0:
            if order.price < self.book['asks']['price'][0]:
                res = {'code': 200, 'timestamp': timenow(
                ), 'message': "market order not fulfilled"}
            else:
                remainingQuantity = order.quantity
                value = 0
                completeFill = []
                partialFill = None
                i = 0
                while self.book['asks']['price'][i] <= order.price and remainingQuantity > 0:
                    for j, quantity in enumerate(self.book['asks']['quantity'][i]):
                        if quantity <= remainingQuantity:
                            completeFill.append(
                                self.book['asks']['orderId'][i][j])
                            remainingQuantity -= quantity
                            value += self.book['asks']['price'][i] * quantity
                        else:
                            partialFill = {'orderId': self.book['asks']['orderId'][i][j],
                                           'quantity': remainingQuantity}
                            remainingQuantity = 0
                            value += self.book['asks']['price'][i] * \
                                partialFill['quantity']
                        if remainingQuantity == 0:
                            break
                    i += 1

                vwap = round(value / (order.quantity - remainingQuantity), 4)

                del self.book['asks']['orderId'][:i-1]
                del self.book['asks']['timestamp'][:i-1]
                del self.book['asks']['price'][:i-1]
                del self.book['asks']['quantity'][:i-1]

                if partialFill is None:
                    if j == len(self.book['asks']['orderId'][0]):
                        del self.book['asks']['orderId'][0]
                        del self.book['asks']['timestamp'][0]
                        del self.book['asks']['price'][0]
                        del self.book['asks']['quantity'][0]
                    else:
                        del self.book['asks']['orderId'][0][:j+1]
                        del self.book['asks']['timestamp'][0][:j+1]
                        del self.book['asks']['quantity'][0][:j+1]
                else:
                    del self.book['asks']['orderId'][0][:j]
                    del self.book['asks']['timestamp'][0][:j]
                    del self.book['asks']['quantity'][0][:j]
                    self.book['asks']['quantity'][0][0] -= partialFill['quantity']

                self.book['timestamp'] = timenow()

                if remainingQuantity > 0:
                    res = {'code': 200,
                           'timestamp': timenow(),
                           'message': f"Partial fill: {order.quantity - remainingQuantity} units at an average price of {vwap}"
                           }
                else:
                    res = {'code': 200,
                           'timestamp': timenow(),
                           'message': f"Complete fill: {order.quantity} units at an average price of {vwap}"
                           }

                return res

        elif order.quantity < 0:
            if order.price > self.book['bids']['price'][-1]:
                res = {'code': 200, 'timestamp': timenow(
                ), 'message': "market order not fulfilled"}
            else:
                remainingQuantity = -order.quantity
                value = 0
                completeFill = []
                partialFill = None
                i = len(self.book['bids']['price']) - 1
                while self.book['bids']['price'][i] >= order.price and remainingQuantity > 0:
                    for j, quantity in enumerate(self.book['bids']['quantity'][i]):
                        if quantity <= remainingQuantity:
                            completeFill.append(
                                self.book['bids']['orderId'][i][j])
                            remainingQuantity -= quantity
                            value += self.book['bids']['price'][i] * quantity
                        else:
                            partialFill = {'orderId': self.book['bids']['orderId'][i][j],
                                           'quantity': remainingQuantity}
                            remainingQuantity = 0
                            value += self.book['bids']['price'][i] * \
                                partialFill['quantity']
                        if remainingQuantity == 0:
                            break
                    i -= 1

                vwap = round(value / (order.quantity - remainingQuantity), 4)

                del self.book['bids']['orderId'][i+1:]
                del self.book['bids']['timestamp'][i+1:]
                del self.book['bids']['price'][i+1:]
                del self.book['bids']['quantity'][i+1:]

                if partialFill is None:
                    if j == len(self.book['bids']['orderId'][-1]):
                        del self.book['bids']['orderId'][-1]
                        del self.book['bids']['timestamp'][-1]
                        del self.book['bids']['price'][-1]
                        del self.book['bids']['quantity'][-1]
                    else:
                        del self.book['bids']['orderId'][-1][:j+1]
                        del self.book['bids']['timestamp'][-1][:j+1]
                        del self.book['bids']['quantity'][-1][:j+1]
                else:
                    del self.book['bids']['orderId'][-1][:j]
                    del self.book['bids']['timestamp'][-1][:j]
                    del self.book['bids']['quantity'][-1][:j]
                    self.book['bids']['quantity'][-1][0] -= partialFill['quantity']

                if remainingQuantity > 0:
                    res = {'code': 200,
                           'timestamp': timenow(),
                           'message': f"Partial fill: {-order.quantity - remainingQuantity} units at an average price of {vwap}"
                           }
                else:
                    res = {'code': 200,
                           'timestamp': timenow(),
                           'message': f"Complete fill: {-order.quantity} units at an average price of {vwap}"
                           }

                return res

    def _handle_cancel(self, order):
        res = {'code': 501, 'timestamp': timenow(), 'message': "not implemented"}
        return res

    def flat_book(self):
        flat = {'bids': {'price': [], 'quantity': []},
                'asks': {'price': [], 'quantity': []}}
        for side in ['bids', 'asks']:
            flat[side]['price'] = self.book[side]['price']
            flat[side]['quantity'] = [sum(qs)
                                      for qs in self.book[side]['quantity']]

        return flat

    def __repr__(self):
        return str(self.book)


class Queue:
    pass

# Client


class Order:
    def __init__(self, usrId, secId, orderType, orderId=None, price=None, quantity=None):
        # Validate usrId
        assert type(usrId) == str, "usrId must be of type str"
        assert len(usrId) == 8, "length of usrId must be 8"
        assert usrId.isupper(), "usrId must be in upper caps"
        assert usrId.isalpha(), "usrId must be alpha"
        # Validate secId
        assert type(secId) == str, "secId must be of type str"
        assert len(secId) == 8, "length of secId must be 8"
        assert secId.isupper(), "secId must be in upper caps"
        assert secId.isalpha(), "secId must be alpha"
        # Validate orderType
        assert type(orderType) == str, "orderType must be of type sr"
        assert len(orderType) == 1, "length of orderType must be 1"

        if orderType == 'L':
            assert price is not None, "price is required for orderType L"
            assert type(price) == str, "price must be of type str"
            assert quantity is not None, "quantity is required for orderType L"
            assert type(quantity) == str, "quantity must be of type str"
        elif orderType == 'M':
            assert price is not None, "price is required for orderType L"
            assert type(price) == str, "price must be of type str"
            assert quantity is not None, "quantity is required for orderType L"
            assert type(quantity) == str, "quantity must be of type str"
        elif orderType == 'C':
            assert orderId is not None, "orderId is required for orderType C"
            assert type(orderId) == str, "orderId must be of type str"
            assert len(orderId) == 24, "length of orderId must be 24"
            assert orderId.isalnum(), "orderId must be alphanumeric"
            assert orderId.isupper(), "orderId must be in upper caps"
        else:
            assert False, "orderType unsupported"

        self.usrId = usrId
        self.secId = secId
        self.orderType = orderType
        self.price = float(price)
        self.quantity = int(quantity)
        self.orderId = orderId

        if orderType == 'L' or orderType == 'M':
            if '.' in price:
                dollar, decima = price.split('.')
            else:
                dollar = price
                decima = ''
            price = '0'*(12-len(dollar)) + dollar + \
                decima + '0'*(4-len(decima))

            quantity = int(quantity)
            sign = '1' if quantity < 0 else '0'
            quantity = sign + '0' * \
                (15-len(str(abs(quantity)))) + str(abs(quantity))

            self.message = 'O' + usrId + secId + orderType + price + quantity

        elif orderType == 'C':
            self.message = 'C' + usrId + secId + orderId

    def set_timestamp(self):
        self.timestamp = timenow()
        return self.timestamp

    def gen_orderId(self):
        orderId = random.randint(0, 1E12)
        self.orderId = '0'*(12-len(str(orderId))) + str(orderId)

    def __repr__(self):
        return self.message


# initialize order book
orderbookCollection = {secId: OrderBook(secId) for secId in [
    'XXXXAAPL', 'XXXXMSFT', 'XXXXTSLA']}

SEC_IDS = ['XXXXMSFT', 'XXXXAAPL', 'XXXXTSLA']
USR_IDS = ['XXJUNLIN', 'XXXHENDY', 'CHINKEON']
PRICE = {'XXXXMSFT': 100, 'XXXXAAPL': 300, 'XXXXTSLA': 700}

orders = []
for i in range(500):
    sec_id = random.choice(SEC_IDS)
    usr_id = random.choice(USR_IDS)
    price = str(random.randint(int(PRICE[sec_id] * 0.95),
                               int(PRICE[sec_id] * 1.05)))
    quantity = str(random.randint(-100, 100))
    o = Order(usr_id, sec_id, 'L', price=price, quantity=quantity)
    o.gen_orderId()
    orders.append(o)

for order in orders:
    orderbookCollection[order.secId].handle_order(order)

print(orderbookCollection['XXXXAAPL'])
print(orderbookCollection['XXXXAAPL'].flat_book())
