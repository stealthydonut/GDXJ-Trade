#This program will get the historical data for a number of tickers
import datetime

#Get todays date
year = datetime.date.today().year
day = datetime.date.today().day
#Month
month = datetime.datetime.now()
month.strftime('%B')


https://www.google.com/finance/historical?output=csv&q=AAPL&startdate=May+22%2C+2015&enddate=May+21%2C+2017

stringone='https://www.google.com/finance/historical?output=csv&q='
ticker
startdate='&startdate=May+22%2C+201'
enddate='&enddate='+month+'+'+day+'%2C+'+year

stringtwo='&startdate=May+22%2C+2015&enddate=May+21%2C+2017
