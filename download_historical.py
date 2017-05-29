#This program will get the historical data for a number of tickers
import urllib
import pandas as pd
import StringIO
import datetime
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

#Get todays date
year = datetime.date.today().year
day = datetime.date.today().day
#Month
month = datetime.datetime.now()
month2=month.strftime('%B')
myfile = ''
bigdata = pd.DataFrame()

for i in gdxj_ticker:
    try:#Develop the text string that can get all the data
        stringone='https://www.google.com/finance/historical?output=csv&q='
        ticker='MSFT'
        startdate='&startdate=May+22%2C+2015'
        enddate='&enddate='+str(month2)+'+'+str(day)+'%2C+'+str(year)
        text2=stringone+ticker+startdate+enddate      
        data = pd.read_csv(text2)
        data['close_lag1']=data['Close'].shift(1)
        data['changepos']=np.where(data['Close']>data['close_lag1'], 1, 0)
        data['changeneg']=np.where(data['Close']<data['close_lag1'], 1, 0)
        data['changenone']=np.where(data['Close']==data['close_lag1'], 1, 0)
        bigdata = bigdata.append(data, ignore_index=False)
    except:
        print 



