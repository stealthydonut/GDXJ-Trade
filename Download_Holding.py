import datetime
import xlrd
import urllib
import lxml
import pandas as pd
import StringIO
import datetime
import numpy as np
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio
#############################
#Read in an html from the web
#############################
dls = "https://www.vaneck.com/FundHoldings.aspx?ticker=GDXJ"
urllib.urlretrieve(dls, "test.xls")
data = pd.read_html('test.xls', skiprows=2)
df = data[0]
df.columns = ['number','Holding Name','Ticker','Shares','Market Value2','NAP']

#loc has to be on the original dataframe and not the reference dataframe because the reference points back to the original
df['Net Asset Per'] = df.loc[df['NAP'].index, 'NAP'].map(lambda x: x.replace('%',''))
df['Market Value'] = df.loc[df['Market Value2'].index, 'Market Value2'].map(lambda x: x.replace('$','').replace(',',''))
###############################
#add a time stamp as a variable
###############################
from datetime import datetime, timedelta
timevalue = datetime.today() - timedelta(days=1)
day = timevalue.day
month = timevalue.month
year = timevalue.year
#year = datetime.date.today().year
#month = datetime.date.today().month
day = timevalue.day
#day = datetime.date.today().day 
#day = '25'
stamp=str(month)+'/'+str(day)+'/'+str(year)
df['Date']=stamp
df.__delitem__('number')

####################################
#Get the daily holdings form storage
####################################
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
#holdings['del1'] = np.where(holdings['Date']=='5/29/2017', 1, 0)
#holdings2 = holdings[holdings['del1'] != 1]

##########################
#Add the correct variables
##########################

#holdings['Per net assets']=holdings['% of Net Assets']
#holdings['Market Value']=holdings['Market Value (US$)']
#holdings.__delitem__('% of Net Assets')
#holdings.__delitem__('Market Value (US$)')

###################################
#Set the data on top of one another
###################################
gdxj_holding = df.append(holdings, ignore_index=True)

##################################
#Export the file to google storage
##################################

bucket2 = client.get_bucket('gdxjtrade')
df_out = pd.DataFrame(gdxj_holding)
df_out.to_csv('gdxj_holding.csv', index=False)
blob2 = bucket2.blob('gdxj_holding.csv')
blob2.upload_from_filename('gdxj_holding.csv')

############################
#Keep the old holdings file#
############################
#name of file
stampname='GDXhistoricalJholdings.csv'


df_out = pd.DataFrame(holdings)
df_out.to_csv(stampname, index=False)
blob2 = bucket2.blob(stampname)
blob2.upload_from_filename(stampname)

