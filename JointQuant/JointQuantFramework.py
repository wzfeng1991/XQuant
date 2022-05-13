from kuanke.wizard import *
from jqdata import *

import numpy as np
import pandas as pd
import talib
import datetime
import time
import math


# 初始化：设定要操作的股票、基准等
def initialize(context):
    set_benchmark('000300.XSHG')
    set_slippage(FixedSlippage(0.01))
    set_option('use_real_price', True)
    set_option('order_volume_ratio', 1)
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    # 初始化
    check_container_initialize()
    check_dynamic_initialize()
    check_stocks_initialize()
    sell_initialize()
    buy_initialize()
    log.set_level('order', 'info')

    # 运行函数
    run_daily(check_stocks, '9:15')
    run_daily(main_stock_pick, '9:16')
    run_daily(sell_every_day,'open')
    run_daily(trade, 'open')
    run_daily(selled_security_list_count, 'after_close')


def check_dynamic_initialize():
    g.security_max_proportion = 1
    g.check_stocks_refresh_rate = 30
    g.max_hold_stocknum = 1
        
    g.buy_refresh_rate = 1    
    g.sell_refresh_rate = 1    
    g.check_stocks_days = 0
    g.days = 0    
    g.buy_trade_days=0
    g.sell_trade_days=0


# 股票池初筛设置函数
def check_stocks_initialize():
    g.filter_paused = True
    g.filter_delisted = True
    g.only_st = False
    g.filter_st = True
    g.security_universe_index = ['all_a_securities']
    g.security_universe_user_securities = []    
    g.industry_list = ["801010","801020","801030","801040","801050","801080","801110","801120","801130","801140","801150","801160","801170","801180","801200","801210","801230","801710","801720","801730","801740","801750","801760","801770","801780","801790","801880","801890"]    
    g.concept_list = []    
    g.blacklist=['300268.XSHE','600035.XSHG','300028.XSHE']
   
# 筛选函数：买入、卖出股票
def main_stock_pick(context):
    if g.days % g.check_stocks_refresh_rate != 0:
        g.days +=1
        return
    g.sell_stock_list=[]
    g.buy_stock_list = []


    #外资策略
    date = context.current_dt.strftime("%Y-%m-%d")
    today = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    yesterday = shifttradingday(today ,shift = -1)
    print('前一个交易日:',yesterday) 
    q = query(finance.STK_EL_TOP_ACTIVATE).filter(finance.STK_EL_TOP_ACTIVATE.day == yesterday )
    df = finance.run_query(q)  
    
    df['net'] = df.buy - df.sell
    df = df.sort_values('net' , ascending = False)
    df = df[(df.link_id != 310003 ) & (df.link_id != 310004 )]
    a=0
    while df.empty == True:
        a=a+1
        yesterday = shifttradingday(today ,shift = -a)
        print('前一个交易日:',yesterday) 
        q = query(finance.STK_EL_TOP_ACTIVATE).filter(finance.STK_EL_TOP_ACTIVATE.day == yesterday )
        df = finance.run_query(q)
        df['net'] = df.buy - df.sell
        df = df.sort_values('net' , ascending = False)
        df = df[(df.link_id != 310003 ) & (df.link_id != 310004 )]          
    
    stockset = list(df['code'])
    
    g.sell_stock_list1 = list(context.portfolio.positions.keys())
    
    current_data = get_current_data()
    
    paused_list = [stock for stock in g.sell_stock_list1 if  current_data[stock].paused]

    for stock in g.sell_stock_list1:
        if stock in paused_list:
            continue
        elif stock not in stockset[:g.max_hold_stocknum]:
                g.sell_stock_list.append(stock)

    for stock in stockset[:g.max_hold_stocknum]:
        if stock in g.sell_stock_list:
            pass
        else:
            g.buy_stock_list.append(stock)
    log.info('卖出列表:',g.sell_stock_list)
    log.info('购买列表:',g.buy_stock_list)
    g.days =1
    return g.sell_stock_list,g.buy_stock_list


# 容器初始化
def check_container_initialize():    
    g.sell_stock_list=[]    
    g.buy_stock_list = []    
    g.open_sell_securities = []    
    g.selled_security_list={}    
    g.ZT=[]    
    

def sell_initialize():    
    g.sell_will_buy = True    
    g.sell_by_amount = None
    g.sell_by_percent = None

def buy_initialize():
    g.filter_holded = False
    g.order_style_str = 'by_cap_mean'
    g.order_style_value = 100


def check_stocks(context):
    if g.check_stocks_days%g.check_stocks_refresh_rate != 0:
        g.check_stocks_days += 1
        return
    g.check_out_lists = get_security_universe(context, g.security_universe_index, g.security_universe_user_securities)
    g.check_out_lists = industry_filter(context, g.check_out_lists, g.industry_list)
    g.check_out_lists = concept_filter(context, g.check_out_lists, g.concept_list)
    g.check_out_lists = st_filter(context, g.check_out_lists)
    g.check_out_lists = paused_filter(context, g.check_out_lists)
    g.check_out_lists = delisted_filter(context, g.check_out_lists)
    g.check_out_lists = [s for s in g.check_out_lists if s not in g.blacklist]
    g.check_stocks_days = 1
    return


def sell_every_day(context):
    g.open_sell_securities = list(set(g.open_sell_securities))
    open_sell_securities = [s for s in context.portfolio.positions.keys() if s in g.open_sell_securities]
    if len(open_sell_securities)>0:
        for stock in open_sell_securities:
            order_target_value(stock, 0)
    g.open_sell_securities = [s for s in g.open_sell_securities if s in context.portfolio.positions.keys()]
    return


def trade(context):
    buy_lists = []
    if g.buy_trade_days%g.buy_refresh_rate == 0:
        buy_lists = g.buy_stock_list
        buy_lists = high_limit_filter(context, buy_lists)
        log.info('购买列表最终',buy_lists)

    if g.sell_trade_days%g.sell_refresh_rate != 0:
        g.sell_trade_days += 1
    else:
        sell(context, buy_lists)
        g.sell_trade_days = 1


    if g.buy_trade_days%g.buy_refresh_rate != 0:
        g.buy_trade_days += 1
    else:
        buy(context, buy_lists)
        g.buy_trade_days = 1


##################################  交易函数 ##################################
def sell(context, buy_lists):
    init_sl = context.portfolio.positions.keys()
    sell_lists = context.portfolio.positions.keys()

    if not g.sell_will_buy:
        sell_lists = [security for security in sell_lists if security not in buy_lists]
    sell_lists = g.sell_stock_list

    if len(sell_lists)>0:
        for stock in sell_lists:
            sell_by_amount_or_percent_or_none(context,stock, g.sell_by_amount, g.sell_by_percent, g.open_sell_securities)

    selled_security_list_dict(context,init_sl)

    return

def buy(context, buy_lists):    
    buy_lists = holded_filter(context,buy_lists)    
    Num = g.max_hold_stocknum - len(context.portfolio.positions)
    buy_lists = buy_lists[:Num]
    
    if len(buy_lists)>0:        
        result = order_style(context,buy_lists,g.max_hold_stocknum, g.order_style_str, g.order_style_value)
        for stock in buy_lists:
            if len(context.portfolio.positions) < g.max_hold_stocknum:                
                Cash = result[stock]                
                value = judge_security_max_proportion(context,stock,Cash,g.security_max_proportion)                
                amount = max_buy_value_or_amount(stock,value,None,None)                
                order(stock, amount, MarketOrderStyle())
    return


def filter_n_tradeday_not_buy(security, n=0):
    try:
        if (security in g.selled_security_list.keys()) and (g.selled_security_list[security]<n):
            return False
        return True
    except:
        return True

def holded_filter(context,security_list):
    if not g.filter_holded:
        security_list = [stock for stock in security_list if stock not in context.portfolio.positions.keys()]
    return security_list

def selled_security_list_dict(context,security_list):
    selled_sl = [s for s in security_list if s not in context.portfolio.positions.keys()]
    if len(selled_sl)>0:
        for stock in selled_sl:
            g.selled_security_list[stock] = 0

def paused_filter(context, security_list):
    if g.filter_paused:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if not current_data[stock].paused]

    return security_list

def delisted_filter(context, security_list):
    if g.filter_delisted:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if not (('退' in current_data[stock].name) or ('*' in current_data[stock].name))]

    return security_list


def st_filter(context, security_list):
    if g.only_st:
        current_data = get_current_data()
        security_list = [stock for stock in security_list if current_data[stock].is_st]
    else:
        if g.filter_st:
            current_data = get_current_data()
            security_list = [stock for stock in security_list if not current_data[stock].is_st]
    return security_list

def high_limit_filter(context, security_list):
    current_data = get_current_data()
    security_list = [stock for stock in security_list if not (current_data[stock].day_open >= current_data[stock].high_limit)]
    return security_list

def get_security_universe(context, security_universe_index, security_universe_user_securities):
    temp_index = []
    for s in security_universe_index:
        if s == 'all_a_securities':
            temp_index += list(get_all_securities(['stock'], context.current_dt.date()).index)
        else:
            temp_index += get_index_stocks(s)
    for x in security_universe_user_securities:
        temp_index += x
    return  sorted(list(set(temp_index)))

def industry_filter(context, security_list, industry_list):
    if len(industry_list) == 0:
        return security_list
    else:
        securities = []
        for s in industry_list:
            temp_securities = get_industry_stocks(s)
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        return security_list

def concept_filter(context, security_list, concept_list):
    if len(concept_list) == 0:
        return security_list
    else:
        securities = []
        for s in concept_list:
            temp_securities = get_concept_stocks(s)
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        return security_list
        
def selled_security_list_count(context):
    if len(g.selled_security_list)>0:
        for stock in g.selled_security_list.keys():
            g.selled_security_list[stock] += 1

def shifttradingday(date,shift):
    tradingday = get_all_trade_days()
    shiftday_index = list(tradingday).index(date)+shift
    return tradingday[shiftday_index]