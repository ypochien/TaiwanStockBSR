# -*- coding: utf-8 -*-
import os
import csv
import re
import pandas as pd
from pandas import Series, DataFrame
import chardet

#force decode for single string
def force_decode(string, codecs=['utf8', 'cp950']):
	for i in codecs:
		try:
			return string.decode(i)
		except:
			pass
		return 'none'

siteDict = {}
for dirPath, dirNames, fileNames in os.walk('BSR'):
	print '#file:' + str(len(fileNames))
	for i,f in enumerate(fileNames[:]):
		if f[0] == '.':
			continue
		else:
			fullfile = os.path.join(dirPath, f)
			#print i
			
			#check for csv encoding
			rawdata = open(fullfile, "r").read()
			result = chardet.detect(rawdata)
			charenc = result['encoding']

			#decode cp950 csv file to unicode with pandas
			if charenc != 'utf-8':
				df = pd.read_csv(fullfile, encoding='cp950')
				df.to_csv(fullfile, encoding='utf-8', index=False)
				print 're-file @' + f

