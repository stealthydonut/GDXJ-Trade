import pandas as pd
import StringIO
import datetime
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

#######################
#Get the daily holdings
#######################
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('GDX_HOLDINGS.csv')
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
pricechanges=pd.read_csv(inMemoryFile, low_memory=False)

###############################
#Merge files by date and ticker
###############################
outputfile=pd.merge(pricechanges, holdings, how='right', left_on=['VG Ticker','date'], right_on=['Ticker','Date'])

######################
#Begin to do Analytics
######################

outputfile.fillna(0)
#Percentage of Shares
outputfile['Shares']=pd.to_numeric(outputfile['Shares'], errors='coerce') #the coerce creates 0's for broken values (i.e. NaN)
outputfile['share per']=outputfile['Shares']/outputfile['Float Shares']
