import pandas as pd
import StringIO
import datetime
import numpy as np
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

###########################
#Get the ticker file with %
###########################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('GDXJ_TICKERS.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
ticker=pd.read_csv(inMemoryFile, low_memory=False)    
    
#######################
#Get the daily holdings
#######################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('gdxj_holding.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
holdings=pd.read_csv(inMemoryFile, low_memory=False)
#holdings.drop_duplicates(cols='Date', take_last=True)

############################
#Get the short and float positions
############################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('all_float_gold.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
floatchanges=pd.read_csv(inMemoryFile, low_memory=False)

############################
#Get the historical prices
############################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('gdxj_historical.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
pricechanges=pd.read_csv(inMemoryFile, low_memory=False)
pricechanges['date']=pd.to_datetime(pricechanges['Date'], errors='coerce')


###############################
#Merge files by date and ticker
###############################
out1=pd.merge(floatchanges, ticker, how='left', left_on=['VG Ticker_y'], right_on=['VG Ticker'])
out2=pd.merge(out1, holdings, how='right', left_on=['VG Ticker','date'], right_on=['Ticker','Date'])
out2['date2']=pd.to_datetime(out2['date'], errors='coerce')
outputfile=pd.merge(pricechanges, out2, how='right', left_on=['VG Ticker','date'], right_on=['VG Ticker','date2'])

######################
#Begin to do Analytics
######################

outputfile.fillna(0)
#Percentage of Shares
outputfile['Shares']=pd.to_numeric(outputfile['Shares'], errors='coerce') #the coerce creates 0's for broken values (i.e. NaN)
outputfile['Net Asset Per']=pd.to_numeric(outputfile['Net Asset Per'], errors='coerce') 
outputfile['share per']=outputfile['Shares']/outputfile['Float Shares']
outputfile['asset ch']=outputfile['Net Asset Per']-outputfile['New Value']
outputfile['asset per ch']=outputfile['asset ch']/outputfile['Net Asset Per']
outputfile['float ch']=outputfile['share per']*outputfile['asset per ch']
outputfile['share sell']=outputfile['float ch']*outputfile['Float Shares']


file1 = outputfile.sort(['Ticker','Date'], ascending=True)
file1['Shares_ch']=file1['Shares'].shift(1)
file1['Share Change']=file1['Shares']-file1['Shares_ch']

bucket2 = client.get_bucket('gdxjtrade')
df_out = pd.DataFrame(file1)
df_out.to_csv('gdxj_afile.csv', index=False)
blob2 = bucket2.blob('gdxj_afile.csv')
blob2.upload_from_filename('gdxj_afile.csv')
