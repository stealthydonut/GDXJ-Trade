import urllib
dls = "https://www.vaneck.com/FundHoldings.aspx?ticker=GDXJ"
urllib.urlretrieve(dls, "test.xls")

print dls
