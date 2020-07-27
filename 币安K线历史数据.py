"""
币安K线历史数据: 抓取币安交易所的历史数据，构建数据库
Author: Shampool
Created: 2020-07-19
Modified: 2020-07-27
Type: Python

抓取起止时间：2019-07-01 -> 2020-06-30
抓取交易对：['BTC/USDT', 'ETH/USDT', 'EOS/USDT', 'LTC/USDT', 'BNB/USDT', 'XRP/USDT']
抓取时间周期：['5m', '15m']
"""
import pandas as pd
import ccxt
import time
import os
import datetime

# 计算程序运行时长
program_start_time = datetime.datetime.now()  # 记录此次程序运行开始时间

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


def save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path):
    """
    将某个交易所在指定日期指定交易对的K线数据，保存到指定文件夹
    :param exchange: ccxt交易所
    :param symbol: 指定交易对，例如'BTC/USDT'
    :param time_interval: K线的时间周期
    :param start_time: 指定日期，格式为'2020-01-01 00:00:00'
    :param path: 文件保存根目录
    :return:
    """

    # ===对火币的limit做特殊处理
    limit = None
    if exchange.id == 'huobipro':
        limit = 2000

    # ===开始抓取数据
    df_list = []
    start_time_since = exchange.parse8601(start_time)
    end_time = pd.to_datetime(start_time) + datetime.timedelta(days=1)

    while True:
        # 获取数据
        df = exchange.fetch_ohlcv(symbol=symbol, timeframe=time_interval, since=start_time_since, limit=limit)
        # 整理数据
        df = pd.DataFrame(df, dtype=float)  # 将数据转换为dataframe
        # 合并数据
        df_list.append(df)
        # 新的since
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        start_time_since = exchange.parse8601(str(t))
        # 如果获取的数据为空，则跳出循环，不保存该数据
        # ···
        # 判断是否挑出循环
        if t >= end_time or df.shape[0] <= 1:
            break
        # 抓取间隔需要暂停2s，防止抓取过于频繁
        time.sleep(1)

    # ===合并整理数据
    df = pd.concat(df_list, ignore_index=True)  # 将分开的几个DataFrame合并成一个大的DataFrame
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序

    # 选取数据时间段
    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]
    # 去重、排序
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ===保存数据到文件
    # 创建交易所文件夹
    path = os.path.join(path, exchange.id)
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建spot文件夹
    path = os.path.join(path, 'spot')
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建日期文件夹
    path = os.path.join(path, str(pd.to_datetime(start_time).date()))
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 拼接文件目录
    file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
    path = os.path.join(path, file_name)
    # 保存数据
    df.to_csv(path, index=False)


# 设定输入参数
# start_time = '2019-07-1 00:00:00'
path = r'D:\Desktop\Binance_history'
error_list = []  # 没有抓取成功、报错的交易数据

# 批量抓取历史数据：在现有程序基础上，遍历日期即可抓取历史数据。产生日期列表的代码

begin_date = '2019-07-01'  # 手工设定开始时间
end_date = '2019-07-02'  # 手工设定结束时间

date_list = []  # 要抓取的日期范围
date = pd.to_datetime(begin_date)
while date <= pd.to_datetime(end_date):
    date_list.append(str(date))
    date += datetime.timedelta(days=1)
print(date_list)

for start_time in date_list:
    # 遍历交易所
    for exchange in [ccxt.binance()]:

        # 获取交易所需要的数据
        market = exchange.load_markets()  # 获取交易所提供的所有交易对
        market = pd.DataFrame(market).T  # 转换成DataFrame的格式，并且转置一下
        symbol_list = list(market['symbol'])  # 获取到的所有交易对

        required_symbol_list = ['BTC/USDT', 'ETH/USDT', 'EOS/USDT', 'LTC/USDT', 'BNB/USDT', 'XRP/USDT']  # 需要抓取的交易对

        # 遍历交易对
        for symbol in symbol_list:
            if (symbol in required_symbol_list) is False:  # 不保留不在交易对列需求表中的交易对
                continue

            # 遍历时间周期
            for time_interval in ['5m', '15m']:
                print(start_time, exchange.id, symbol, time_interval)

                # 抓取数据并且保存
                try:  # 容错机制
                    save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path)  # 调用抓取函数
                except Exception as e:
                    print(e)  # 打印报错信息
                    error_list.append('_'.join([exchange.id, symbol, time_interval]))
                    # 一些下架的币种可能会报错

print(error_list)

# 计算程序运行时长
program_end_time = datetime.datetime.now()  # 记录此次程序运行结束时间
print('运行时长 ', program_end_time - program_start_time)  # 打印此次程序运行时长
