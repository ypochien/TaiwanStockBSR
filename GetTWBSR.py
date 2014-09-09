# -*- coding: utf-8 -*-

import re
import urllib2,urllib
import sys
import csv
from datetime import datetime
import os
import threading
import Queue
from time import strftime
from time import sleep
from time import time

from types import *
import pandas as pd
from pandas import Series, DataFrame

# TSE : Taiwan Stock Exchange , 台灣證交所 （上市）
# OTC : Over-the-Counter , 櫃檯中心 （上櫃）
# BSR : Buy Sell Report , 分公司買賣進出表


class ThreadingDownloadBot(threading.Thread):
	def __init__(self, pid, queue):
		threading.Thread.__init__(self)
		self.queue = queue
		self.pid = pid
	def run(self):
		while(True):
			try:
				Code = self.queue.get()
			except self.queue.Empty:
				#pass
				print 'self.queue.Empty'
			else:
				if self.pid < 5: # fast otc thread
					sleep(0.5)
				retry = 0
				if ',' in Code:
					retry = int(Code.split(',')[1])
					Code = Code.split(',')[0]
				print 'thread: id=%d, code=%s, queue_size=%d, retry=%d' % (self.pid, Code, self.queue.qsize(), retry)
				res = self.RunImp(Code)
				if res < 0:
					retry +=1
					if retry >= 3:
						print 'fail3, ' + Code #print u'%s 下載三次失敗'%(Code)
					else:
						retryCode = '%s,%d'%(Code,retry)
						#print 'retry: ' + retryCode
						sleep(1) #有錯誤停1秒, sleep before putting into queue
						self.queue.put(retryCode)
			self.queue.task_done() #used by consumer thread, tells the queue that task is completed.
        
class DownloadTSEBot(ThreadingDownloadBot):
    def __init__(self,pid,queue):
        super(DownloadTSEBot, self).__init__(pid,queue)
        self.name = "TSE BSR Download Bot."
			
    def RunImp(self, Code):
		
		# step1. GetMaxPage and POST data
		def GetDateAndspPage(Code):
			try:
				base_url = 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx'
				req = urllib2.Request(base_url)
				response = urllib2.urlopen(req)
				html = response.read()
				__VIEWSTATE = re.findall(u'id="__VIEWSTATE" value="(.*)" />',html)[0]
				__EVENTVALIDATION = re.findall(u'id="__EVENTVALIDATION" value="(.*)" />',html)[0]
				HiddenField_spDate = re.findall(u'id="sp_Date" name="sp_Date" style="display: none;">(.*)</span>',html)[0]
				
				PostDataDict = {'__EVENTTARGET':''
				, '__EVENTARGUMENT':''
				,'HiddenField_page':'PAGE_BS'
				,'txtTASKNO':Code
				,'hidTASKNO':Code
				,'__VIEWSTATE': __VIEWSTATE
				,'__EVENTVALIDATION':__EVENTVALIDATION
				,'HiddenField_spDate':HiddenField_spDate
				,'btnOK':'%E6%9F%A5%E8%A9%A2'}
				
				postData = urllib.urlencode( PostDataDict)
				req = urllib2.Request( base_url , postData)
				response = urllib2.urlopen( req)
				html = response.read()
				sp_ListCount = re.findall(u'<span id="sp_ListCount">(.*)</span>',html)[0]
				return (HiddenField_spDate,sp_ListCount)
			except Exception, e:
				return (None,None)
		
		# step 2. GetRawData
		def GetBSRawData(Code,MaxPageNum):
			try:
				url = 'http://bsr.twse.com.tw/bshtm/bsContent.aspx?StartNumber=%s&FocusIndex=All_%s'%(Code,MaxPageNum)
				req = urllib2.Request(url)
				response = urllib2.urlopen(req)
				html = response.read()
				return html #html contains N pages' report xhtml data
			except Exception , e:
				return None
		
		# step 3. from xhtml data to csv file
		def BSRawToCSV(BSRaw):
			#取得資料表title
			'''
				<tr class='column_title_1'>
				<td>序</td>
				<td>證券商</td>
				<td>成交單價</td>
				<td>買進股數</td>
				<td>賣出股數</td>
				</tr>
				'''
			title_tr_pattern = u"<tr class='column_title_1'>(.*?)<\/tr>"
			title_tr = re.compile(title_tr_pattern)
			result_tr = title_tr.findall(BSRaw)
			title_td_pattern = u'<td *>\B(.*?)</td>'
			title_td = re.compile(title_td_pattern)
			result_td = title_td.findall(result_tr[0])
			#title = ','.join(title.decode('utf-8').encode('cp950') for title in result_td)
			title = ','.join(title for title in result_td)
			
			#取得各分公司買賣內容
			td = '''
				<td class='column_value_center'>               1</td>
				<td class='column_value_left'>                 1233  彰銀台中</td>
				<td class='column_value_right'>               8.65</td>
				<td class='column_value_right'>               0</td>
				<td class='column_value_right'>               20,000</td>
				'''
			content_tr_pattern = u"<tr class='column_value_price_[23]'>(.*?)<\/tr>"
			content_tr = re.compile(content_tr_pattern)
			result_tr_content = content_tr.findall(BSRaw)
			content_td_pattern = u"<td \S*>(.*?)</td>"
			content_td = re.compile(content_td_pattern)
			content_list = []
			for tr in result_tr_content:
				result_td = content_td.findall(tr)
				row =  ','.join(td.replace(',','').strip() for td in result_td if td.strip()[0] not in ['<','&'])
				if len(row) == 0:
					continue
				#content_list.append(row.decode('utf-8').encode('cp950'))
				content_list.append(row)
			sortedlist = sorted(content_list,key = lambda s: int(s.split(',')[0]))
			#將Title加入資料首列
			sortedlist.insert(0,title)
			return sortedlist
		
		def CSVToFile(CSVData,filename):
			with open('BSR/'+filename, 'wb') as csvfile:
				content = '\n'.join(row for row in CSVData)
				csvfile.write(content)

		self.RawBSR = "TSE"
		self.date, MaxPageNum = GetDateAndspPage(Code)
		if None == MaxPageNum or "" == MaxPageNum:
			return -1
		BSRawData = GetBSRawData(Code, MaxPageNum)
		if None == BSRawData:
			return -2
		filename = "%s_%s.csv"%(Code,self.date)
		CSVData = BSRawToCSV(BSRawData)
		CSVToFile(CSVData, filename)
		return 0
      
class DownloadOTCBot(ThreadingDownloadBot):
	def __init__(self, pid, queue, otcdate):
		super(DownloadOTCBot, self).__init__(pid, queue)
		self.name = "OTC BSR Download Bot."
		self.date = otcdate
    
	def RunImp(self,Code):
		def DownloadOTC(Code,filename,otcdate):
			try:
				base_url = r'http://www.gretai.org.tw/web/stock/aftertrading/broker_trading/download_ALLCSV.php?curstk={}&stk_date={}'.format(Code,otcdate)
				response = urllib2.urlopen(base_url)
				#html = response.read()
				html = response.read().decode('cp950').encode('utf-8')
			except Exception, e:
				return -1

			with open('BSR/'+filename, 'wb') as csvfile:
				content = '\n'.join(row for row in html.split(',,')[1:])
				csvfile.write(content)

			return 0

		#entity in RunImp() of OTC thread
		self.RawBSR = "OTC"
		#otcDate = getOTCDate(Code) #move this part outside the thread
		otcDate = self.date
		if otcDate == None:
			return -2
		filename = "%s_%d%s.csv"%(Code, int(otcDate[0:3])+1911, otcDate[3:])
		ret = DownloadOTC(Code, filename, otcDate)
		if ret < 0:
			return ret
		return 0

def getCodeListFromCSV(filename):
    CodeList = []
    with open(filename,'r') as f:
        for row in f:
            code = row.split(',')[0]
            CodeList.append(code)
    return CodeList

def getDateForOTC(CodeDict):
	for Code in CodeDict['OTC']:
		if Code[0].isdigit():
			#print Code
			baseUrl = "http://www.gretai.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php"
			postDataDict = {
				'stk_code' : Code
			}
			postData = urllib.urlencode( postDataDict)
			req = urllib2.Request( baseUrl , postData)
			response = urllib2.urlopen(req)
			html = response.read()
			
			date_list = re.findall(u'<input type="hidden" id="stk_date" name="stk_date" value=(.*)>',html)
			for date in date_list:
				print 'get date @' + date
				return date
	return None

if __name__ == '__main__':
	if not os.path.exists('BSR'):
		os.makedirs('BSR')

	CodeDict = {}
	CodeDict['TSE'] = getCodeListFromCSV('TSECode.csv')
	CodeDict['OTC'] = getCodeListFromCSV('OTCCode.csv')
	print 'TSE:%d, OTC:%d' % (len(CodeDict['TSE']), len(CodeDict['OTC']))

	num_thread_otc = 5
	num_thread_tse = 5

	#preprocessing for otc
	otcdate = getDateForOTC(CodeDict)

	starttime = time()
	OTCqueue = Queue.Queue()
	for i in range(num_thread_otc):
		t = DownloadOTCBot(i, OTCqueue, otcdate)
		t.setDaemon(True) #the thread t is terminated when the main thread ends.
		t.start()

	for Code in CodeDict['OTC'][:]:
		if Code[0].isdigit(): #some stock have character id at tail, like '2833A'
			OTCqueue.put(Code)

	OTCqueue.join()
	endtime = time()
	print 'end of otc, ' + 'time: ' + str(endtime - starttime)

	starttime = time()
	TSEqueue = Queue.Queue()
	for i in range(num_thread_tse):
		t = DownloadTSEBot(i+num_thread_otc, TSEqueue) #shifted thread id from otc group
		t.setDaemon(True)
		t.start()


	for Code in CodeDict['TSE'][:]:
		if Code[0].isdigit():
			TSEqueue.put(Code)
#	TSEqueue.put('1469')


	TSEqueue.join() #Blocks until all items in the queue have been gotten and processed.
	endtime = time()
	print 'end of tse, ' + 'time: ' + str(endtime - starttime)



