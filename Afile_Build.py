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

############################
#Get the daily price changes
############################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('daily_prices2017_5_23.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
pricechanges1=pd.read_csv(inMemoryFile, low_memory=False)
# Then do other things...
blob = bucket.get_blob('daily_prices2017_5_24.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
pricechanges2=pd.read_csv(inMemoryFile, low_memory=False)
blob = bucket.get_blob('daily_prices2017_5_25.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
pricechanges3=pd.read_csv(inMemoryFile, low_memory=False)


pricechangesx = pricechanges1.append(pricechanges2, ignore_index=True)
pricechanges = pricechangesx.append(pricechanges3, ignore_index=True)
###############################
#Merge files by date and ticker
###############################
out1=pd.merge(pricechanges, ticker, how='left', left_on=['VG Ticker'], right_on=['VG Ticker'])
outputfile=pd.merge(out1, holdings, how='right', left_on=['VG Ticker','date'], right_on=['Ticker','Date'])

######################
#Begin to do Analytics
######################

outputfile.fillna(0)
#Percentage of Shares
outputfile['Shares']=pd.to_numeric(outputfile['Shares'], errors='coerce') #the coerce creates 0's for broken values (i.e. NaN)
outputfile['Per net assets']=pd.to_numeric(outputfile['Per net assets'], errors='coerce') 
outputfile['share per']=outputfile['Shares']/outputfile['Float Shares']
outputfile['asset ch']=outputfile['Per net assets']-outputfile['New Value']
outputfile['asset per ch']=outputfile['asset ch']/outputfile['Per net assets']
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
