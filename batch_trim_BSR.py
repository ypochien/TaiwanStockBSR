# -*- coding: utf-8 -*-
import os
import csv
import re
import pandas as pd
from pandas import Series, DataFrame

siteDict = {}
for dirPath, dirNames, fileNames in os.walk('BSR'):
	print '#file:' + str(len(fileNames))
	for i,f in enumerate(fileNames[:]):
		if f[0] == '.':
			continue
		else:
			fullfile = os.path.join(dirPath, f)
			print i
			
			df = pd.read_csv(fullfile)
			if len(df.columns) == 5: #file contain '序'
				df = df.drop( df.columns[0], axis=1 ) #drop original index
			df.columns = ['securities', 'price', 'buy', 'sell']
			#only retain id of securities
			for index in df.index:
				df.ix[index, 'securities'] = df.ix[index, 'securities'].split(' ')[0]
			df.to_csv(fullfile, encoding='utf-8', index=False)

