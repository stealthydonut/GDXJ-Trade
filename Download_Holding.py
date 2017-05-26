import datetime
import xlrd
import urllib
import lxml
dls = "https://www.vaneck.com/FundHoldings.aspx?ticker=GDXJ"
urllib.urlretrieve(dls, "test.xls")
data = pd.read_html('test.xls', skiprows=2)
df = data[0]
df.columns = ['number','holding','Ticker','Shares','Market Value','Per net assets']
