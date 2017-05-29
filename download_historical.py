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
for i in gdxj_ticker:
    try:#Develop the text string that can get all the data
      stringone='https://www.google.com/finance/historical?output=csv&q='
      ticker='MSFT'
      startdate='&startdate=May+22%2C+2015'
      enddate='&enddate='+str(month2)+'+'+str(day)+'%2C+'+str(year)
      text2=stringone+ticker+startdate+enddate      
      df = pd.read_csv(text2)



#https://www.google.com/finance/historical?output=csv&q=AAPL&startdate=May+22%2C+2015&enddate=May+21%2C+2017
#https://www.google.com/finance/historical?output=csv&q=mbo&startdate=May+22%2C+201&enddate=May+29%2C+2017
#stringone='https://www.google.com/finance/historical?output=csv&q='
#ticker='MBO'
#startdate='&startdate=May+22%2C+2015'
#enddate='&enddate='+str(month2)+'+'+str(day)+'%2C+'+str(year)
#text2=stringone+ticker+startdate+enddate

for i in gdxj_ticker:
    try:#Develop the text string that can get all the data
      stringone='https://www.google.com/finance/historical?output=csv&q='
      ticker='MSFT'
      startdate='&startdate=May+22%2C+2015'
      enddate='&enddate='+str(month2)+'+'+str(day)+'%2C+'+str(year)
      text2=stringone+ticker+startdate+enddate      
      link=text2
      f = urllib.urlopen(link)
      myfile += f.readline()     
    except:
      print i
     
TESTDATA=stio(myfile)
