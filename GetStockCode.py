# -*- coding: utf-8 -*-

#http://www.twse.com.tw/ch/products/stock_code2.php
#http://isin.twse.com.tw/isin/C_public.jsp?strMode=2
#http://isin.twse.com.tw/isin/C_public.jsp?strMode=4
from types import *
from lxml.html import parse
import csv
tse_url = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
otc_url = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=4"

def getCode(_url):
	page = parse(_url)
	rows = page.xpath("body/table")[1].findall("tr")
	data = list()
	content = []
	for row in rows:
		v = [c.text for c in row.getchildren()]
		if len(v) > 4 and type(v[4]) is not NoneType: #沒有分類的代表非股票
			code = v[0].encode('latin1').decode('cp950')
			content.append('%s,%s'%(code[:11].strip(),code[11:]))
	return content

def ToCsv(CSVData, filename):
	with open(filename, 'wb') as csvfile:
		content = '\r\n'.join(row for row in CSVData)
		csvfile.write(content.encode('cp950'))
		print("write %s ok."%(filename))

if __name__ == '__main__':
	TSE_Code = getCode(tse_url)
	ToCsv(TSE_Code,"TSECode.csv")

	OTC_Code = getCode(otc_url)
	ToCsv(OTC_Code, "OTCCode.csv")
