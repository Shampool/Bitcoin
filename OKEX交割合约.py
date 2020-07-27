"""
OKEX交割合约: 每天实时收集 OKEX交易所 交割合约K线数据
Author: Shampool
Created: 2020-07-20
Modified: 2020-07-27
Type: Python

交割合约
币本位保证金合约 + USDT保证金合约
当周，次周，当季，次季
交易对：BTC
时间周期：5min
"""
import pandas as pd
import ccxt
import time
import os
import datetime

# ===预处理
program_start_time = datetime.datetime.now()  # 计算程序运行时长, 记录此次程序运行开始时间
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行


# ===使用私有函数获取OKEX交割合约K线数据
# 设定参数
ids = ['BTC-USD-200717', 'BTC-USD-200724', 'BTC-USD-200925', 'BTC-USD-201225',  # 币本位 当周 次周 当季 次季
       'BTC-USDT-200717', 'BTC-USDT-200724', 'BTC-USDT-200925', 'BTC-USDT-201225']  # USDT保证金 当周 次周 当季 次季
ids_name = ['当周', '次周', '当季', '次季',
            '当周', '次周', '当季', '次季']  # 与ids的ids_index顺序一致

# 对ids[]中的交易对循环抓取; 循环次数=列表长度
for ids_index in range(len(ids)):
    path = r'D:\Desktop\OKEX交割'
    params = {
        'instrument_id': ids[ids_index],  # 该次循环所对应的交易对的 instrument_id
        'granularity': '300',  # 时间粒度，以秒为单位，默认值60
        'start': '2020-07-11T00:00:00.000Z',  # 开始时间
        'end': '2020-07-12T00:00:00.000Z',  # 结束时间
    }
    data = ccxt.okex().futuresGetInstrumentsInstrumentIdCandles(params=params)  # futures=交割, spot=现货, swap=永续
    print(ids[ids_index], data)
    # 返回参数：timestamp开始时间; open; high; low; close; volume; currency_volume按币种折算的交易量

    # ===合并整理数据
    df = pd.DataFrame(data, dtype=float)  # 将数据转换为DataFrame
    df.rename(columns={0: 'timestamp', 1: 'open', 2: 'high', 3: 'low',
                       4: 'close', 5: 'volume', 6: 'currency_volume'}, inplace=True)  # 重命名列名
    # df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'currency_volume']]  # 整理列的顺序
    print(df)

    # ===保存数据到文件
    # 创建交易所文件夹
    path = os.path.join(path, 'OKEX')
    if os.path.exists(path) is False:  # 若该文件夹不存在，就创建一个
        os.mkdir(path)
    # 创建spot文件夹
    path = os.path.join(path, '交割合约')
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 创建日期文件夹
    path = os.path.join(path, str(pd.to_datetime(params['start']).date()))
    if os.path.exists(path) is False:
        os.mkdir(path)
    # 拼接文件目录
    file_name = '_'.join([ids[ids_index].replace('/', '-'), str(int(int(params['granularity'])/60))])\
                + 'm_' + (ids_name[ids_index]) + '.csv'
    path = os.path.join(path, file_name)
    # 保存数据
    df.to_csv(path, index=False)

time.sleep(1)  # 每抓取8次，暂停1秒！！！限速20次/2s

# 计算程序运行时长
program_end_time = datetime.datetime.now()  # 记录此次程序运行结束时间
print('运行时长 ', program_end_time - program_start_time)  # 打印此次程序运行时长
