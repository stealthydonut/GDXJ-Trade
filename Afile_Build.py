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
holdings['del1'] = np.where(holdings['Date']=='5/2017-05-29 12:53:07.004990/2017', 1, 0)
holdings['del2'] = np.where(holdings['Date']=='5/27/2017', 1, 0)
holdings['del3'] = np.where(holdings['Date']=='5/30/2017', 1, 0)
holdings2 = holdings[holdings['del1'] != 1]
holdings3 = holdings2[holdings2['del2'] != 1]
holdings4 = holdings3[holdings3['del3'] != 1]

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
floatchanges['VG Ticker_y'] = floatchanges.loc[floatchanges['VG Ticker_y'].index, 'VG Ticker_y'].map(lambda x: x.strip())
out1=pd.merge(floatchanges, ticker, how='left', left_on=['VG Ticker_y'], right_on=['VG Ticker'])
out2=pd.merge(out1, holdings4, how='right', left_on=['VG Ticker','date'], right_on=['Ticker','Date'])
out2['date2']=pd.to_datetime(out2['Date'], errors='coerce')
#out2['VG Ticker_y'] = out2.loc[out2['VG Ticker_y'].index, 'VG Ticker_y'].map(lambda x: x.strip())
pricechanges['VG Ticker'] = pricechanges.loc[pricechanges['VG Ticker'].index, 'VG Ticker'].map(lambda x: x.strip())
outputfile=pd.merge(pricechanges, out2, how='right', left_on=['VG Ticker','date'], right_on=['Ticker','date2'])

######################
#Begin to do Analytics
######################

outputfile.fillna(0)
#Percentage of Shares
outputfile['Shares']=pd.to_numeric(outputfile['Shares'], errors='coerce') #the coerce creates 0's for broken values (i.e. NaN)
outputfile['Net Asset Per']=pd.to_numeric(outputfile['Net Asset Per'], errors='coerce') 
outputfile['Market Value']=pd.to_numeric(outputfile['Market Value'], errors='coerce') 
outputfile['share per']=outputfile['Shares']/outputfile['Float Shares']
outputfile['asset ch']=outputfile['Net Asset Per']-outputfile['New Value']
outputfile['asset per ch']=outputfile['asset ch']/outputfile['Net Asset Per']
outputfile['float ch']=outputfile['share per']*outputfile['asset per ch']
outputfile['share sell']=outputfile['float ch']*outputfile['Float Shares']


file1 = outputfile.sort(['Holding Name','Date_y'], ascending=True)
file1['Shares_ch']=file1['Shares'].shift(1)
file1['Share Change']=file1['Shares']-file1['Shares_ch']

#Begin to delete columns
file1.__delitem__('del1')
file1.__delitem__('del2')
file1.__delitem__('del3')
file1.__delitem__('date2')
file1.__delitem__('Per net assets')
file1.__delitem__('NAP')
file1.__delitem__('Market Value2')
file1.__delitem__('Ticker_y')
file1.__delitem__('Name_y')
file1.__delitem__('Google_Ticker')
file1.__delitem__('Name_x')


bucket2 = client.get_bucket('gdxjtrade')
df_out = pd.DataFrame(file1)
df_out.to_csv('gdxj_afile.csv', index=False)
blob2 = bucket2.blob('gdxj_afile.csv')
blob2.upload_from_filename('gdxj_afile.csv')
