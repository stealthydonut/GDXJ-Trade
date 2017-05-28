#This program parces out information from the shortdata website
from lxml import html
import requests

page = requests.get('http://shortdata.ca/?c=guyana-goldfields%20inc.-short-sales&sym=GUY.TO')
tree = html.fromstring(page.content)

#This will create a list of buyers:
values = tree.xpath('<td>"data"</td>')
<td>"data"</td>
