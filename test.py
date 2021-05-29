#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import sys
import getopt


if __name__ == '__main__':

    #Входные параметры
    unixOptions = 'sdt:edt:'
    gnuOptions = ['start_dt=', 'end_dt=']
    
    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:] #excluding script name
    
    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:
        print (str(err))
        sys.exit(2)
    
    start_dt = ''
    end_dt = ''
    for currentArgument, currentValue in arguments:
        if currentArgument in ('-sdt', '--start_dt'):
            start_dt = currentValue
        elif currentArgument in ('-edt', '--end_dt'):
            end_dt = currentValue

#Импортируем google sheets
gc = gspread.service_account(filename='testtask-314915-9d01d59d7961.json') # ключ в папке проекта
sh = gc.open_by_key('1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ')
# TRANSACTIONS
worksheet = sh.get_worksheet(1) #-> 0 - first sheet, 1 - second sheet etc. 
trans =  pd.DataFrame.from_dict(worksheet.get_all_records())
trans['created_at'] = pd.to_datetime(trans['created_at'])
trans = trans.drop_duplicates()

# CLIENT
worksheet = sh.get_worksheet(2) 
cl =  pd.DataFrame.from_dict(worksheet.get_all_records())
cl = cl.drop_duplicates()

# MANAGERS
worksheet = sh.get_worksheet(3)
man =  pd.DataFrame.from_dict(worksheet.get_all_records())
man = man.drop_duplicates()

# LEADS
worksheet = sh.get_worksheet(4) 
leads =  pd.DataFrame.from_dict(worksheet.get_all_records())
leads['created_at'] = pd.to_datetime(leads['created_at'])

# Обработка двойных названий и пропусков
leads['d_utm_source'] = leads['d_utm_source'].replace('vk', 'vkontakte')
leads['d_utm_source'] = leads['d_utm_source'].replace('insta', 'instagram')
leads['d_utm_source'] = leads['d_utm_source'].replace('ycard#!/tproduct/225696739-1498486363994', 'ycard')
leads['d_utm_source'] = leads['d_utm_source'].replace('', 'unknown')
leads = leads.drop_duplicates()

# Основной датафрейм
# Срез по датам
df = leads.query('@start_dt <= created_at <= @end_dt')
# Определение минимальных дат заявок и транзакций
min_trans_dt = trans.groupby('l_client_id').agg({'created_at':'min'}).reset_index()
min_trans_dt.columns = ['l_client_id','min_trans_dt']
min_lead_dt = leads.groupby('l_client_id').agg({'created_at':'min'}).reset_index()
min_lead_dt.columns = ['l_client_id','min_lead_dt']
df = df.merge(min_trans_dt, how='left')\
        .merge(min_lead_dt, how='left')

# Расчет количества новых и мусорных заявок
df['new_lead_num'] = 0
df['new_lead_num'] = df['new_lead_num'].where(df['min_lead_dt'] < start_dt, 1)

df['bed_lead_num'] = 0
df['bed_lead_num'] = df['bed_lead_num'].where(df['l_client_id'] != '00000000-0000-0000-0000-000000000000', 1)

# Добавление данных по клубам и менеджерам
df = df.merge(man, how='left', left_on = 'l_manager_id', right_on = 'manager_id')

# Сопоставление заявок и транзакций
df = df.sort_values(by = ['l_client_id','created_at'])
df['fwd_lead_dt'] = df.groupby('l_client_id')['created_at'].\
                            apply(lambda x: x.shift(-1)).\
                            fillna('2099-01-01 00:00:00')
df['fwd_lead_dt'] = pd.to_datetime(df['fwd_lead_dt'])

tmp = df.merge(trans, how='left', on = 'l_client_id')
tmp = tmp[(tmp['created_at_y']>=tmp['created_at_x']) & (tmp['created_at_y']<=tmp['fwd_lead_dt'])]

# Определение транзакций произошедших в течение недели после заявки
tmp['days_lead_trans'] = tmp['created_at_y'] - tmp['created_at_x']
tmp1 = tmp[tmp['days_lead_trans'] <= '7days']                       
tmp1 = tmp1.groupby(['lead_id', 'l_client_id']).agg({'transaction_id':'nunique'}).reset_index()
tmp2 = tmp[(tmp['days_lead_trans'] <= '7days')&(tmp['new_lead_num'] > 0)]
tmp2 = tmp2.groupby(['lead_id', 'l_client_id']).agg({'m_real_amount':'sum'}).reset_index()

df = df.merge(tmp1, how='left', on = 'lead_id')
df = df.merge(tmp2, how='left', on = 'lead_id')

# Удаление лишнего и наведение порядка
df = df[['lead_id', 'created_at', 'd_utm_source', 'l_client_id_x', 'new_lead_num', 'bed_lead_num', 'd_manager', 'd_club', 'l_client_id_y', 'l_client_id', 'm_real_amount']]
df.columns = ['lead_id', 'dt', 'source', 'client_id', 'new_lead_num', 'bed_lead_num', 'manager', 'club', '7days_client_id', 'new_7days_client_id', 'new_7days_real_amount']
df['manager'] = df['manager'].fillna('unknown')
df['club'] = df['club'].fillna('unknown')
df['source'] = df['source'].fillna('unknown')

# Экспорт в (https://docs.google.com/spreadsheets/d/13gtDTG_NLRwTtGXNhlJk0Gn-xx1MmuSpu8RjEFVizpA/edit?usp=sharing)
sh = gc.open_by_key('13gtDTG_NLRwTtGXNhlJk0Gn-xx1MmuSpu8RjEFVizpA')
worksheet = sh.get_worksheet(1) 
sh.values_clear("'Лист1'!A2:M100000") #Очистим диапозон
set_with_dataframe(worksheet, df)



