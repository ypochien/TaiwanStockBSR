# -*- coding: utf-8 -*-

#透過分公司進出表，取得分公司對照表

import os
import csv
import re

import pandas as pd
from pandas import Series, DataFrame

if __name__ == '__main__' :

	siteDict = {}
	for dirPath, dirNames, fileNames in os.walk('BSR'):
		for f in fileNames:
			if f[0] == '.':
				continue
			else:
				fullfile = os.path.join(dirPath, f)
				#print fullfile
				with open(fullfile, 'rb') as csvfile:
					spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
					#print spamreader
					for row in spamreader:
						lstSite = row[1].split()
						if re.findall('^[\d{4}]',lstSite[0]):
							if siteDict.has_key(lstSite[0]):
								pass
							else:
								siteDict[lstSite[0]] = row[1][4:].rstrip().lstrip()

	lstSite = [ [k,v] for k,v in siteDict.items() ]
	lstSite = sorted(lstSite,key = lambda x : x[0])

	#output csv wiht big5 encoding for excel
	with open('SiteCode.csv','wb') as siteCsvFile:
		csvWriter = csv.writer(siteCsvFile)
		csvWriter.writerows(lstSite)


