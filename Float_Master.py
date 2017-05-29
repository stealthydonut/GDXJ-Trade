import StringIO
import pandas as pd
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
blob = bucket.get_blob('daily_prices2017_5_25.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
pricechanges4=pd.read_csv(inMemoryFile, low_memory=False)

pricechangesx  = pricechanges1.append(pricechanges2, ignore_index=True)
pricechangesxx = pricechangesx.append(pricechanges3, ignore_index=True)
pricechanges   = pricechangesxx.append(pricechanges4, ignore_index=True)

bucket2 = client.get_bucket('gdxjtrade')
df_out = pd.DataFrame(pricechanges)
df_out.to_csv('float_all.csv', index=False)
blob2 = bucket2.blob('float_all.csv')
blob2.upload_from_filename('float_all.csv')
