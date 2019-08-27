from pytrends.request import TrendReq
import pandas as pd
import os
from datetime import datetime as dt
import pymysql
import pandas as pd
from pytrends.request import TrendReq
from sqlalchemy import create_engine

def search_7(keywords,time,countrylist):

    pytrends = TrendReq(hl = 'en-US', tz=360)
    
    if 'global' in countrylist:
        countrylist[countrylist.index('global')]=''
    
    dictFrame=dict()
    search_list=[]
    for country in countrylist:
        if country=='RU':
            pytrends.build_payload(keywords['RU'], cat=0, timeframe= time, geo= country, gprop ='')
        elif country=='VN':
            pytrends.build_payload(keywords['VN'], cat=0, timeframe= time, geo= country, gprop ='')
        else:
            pytrends.build_payload(keywords['NN'], cat=0, timeframe= time, geo= country, gprop ='')

        df=pytrends.interest_over_time()
        df=df.drop(columns='isPartial')
        if country=='':
            df['Country']='Global'
        else:
            df['Country']=country
#         df.index = df.index.strftime('%m/%d/%Y')
        df=df.rename(columns={df.columns[0]:'Nanocell',df.columns[1]:'Qled'})
        df.reset_index(level=0, inplace=True)
        df=df.rename(columns={'index':'Date'})

        search_list.append(df)
    final_table=pd.concat(search_list)
    return final_table


def search_accu(keywords,time,countrylist):

    pytrends = TrendReq(hl = 'en-US', tz=360)
    
    if 'global' in countrylist:
        countrylist[countrylist.index('global')]=''
    
    dictFrame=dict()
    search_list=[]
    for country in countrylist:
        if country=='RU':
            pytrends.build_payload(keywords['RU'], cat=0, timeframe= time, geo= country, gprop ='')
        elif country=='VN':
            pytrends.build_payload(keywords['VN'], cat=0, timeframe= time, geo= country, gprop ='')
        else:
            pytrends.build_payload(keywords['NN'], cat=0, timeframe= time, geo= country, gprop ='')

        df=pytrends.interest_over_time()
        df=df.drop(columns='isPartial')
        if country=='':
            df['Country']='Global'
        else:
            df['Country']=country
        df.index = df.index.strftime('%m/%d/%Y')
        df=df.rename(columns={df.columns[0]:'Nanocell',df.columns[1]:'Qled'})
        df.reset_index(level=0, inplace=True)
        df=df.rename(columns={'index':'Date'})

        search_list.append(df)
    final_table=pd.concat(search_list)
    return final_table
    
from datetime import datetime

Today=datetime.today().strftime("%Y-%m-%d")

keywords={'VN':['nanocell tivi + nano cell tivi + nanocell tv + nano cell tv','qled tivi + qled tv',],
         'RU':['nanocell tv + nano cell tv + nanocell Телевизор + nano cell Телевизор','qled Телевизор + qled tv'],
         'NN':['nanocell tv + nano cell tv','qled tv']}

countrylist=['global','US','ES','FR','RU','PE','PL','GB','TR']
time_7='now 7-d'
accumulated_time='2019-01-01 '+ Today

x=search_7(keywords,time_7,countrylist)
y=search_accu(keywords,accumulated_time,countrylist)

# os.chdir('D:/download/trends')
# writer=pd.ExcelWriter('Google Trend.xlsx',engine='xlsxwriter')
# x.to_excel(writer,sheet_name='Past 7 Days')
# y.to_excel(writer,sheet_name='누적 검색 지수')

# writer.save()

conn = pymysql.connect(host='localhost', user='root', password='',
                       db='test', charset='utf8')
curs = conn.cursor()
engine = create_engine('mysql+mysqldb://root:@localhost/test?charset=utf8', convert_unicode=False)
conn = engine.connect()

x.to_sql(name='past_7days',if_exists='replace',index=False,con=engine)

y.to_sql(name="19년 누적 점유율",if_exists='replace',index=False,con=engine)
