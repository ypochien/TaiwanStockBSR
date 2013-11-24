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

# TSE : Taiwan Stock Exchange , 台灣證交所 （上市）
# OTC : Over-the-Counter , 櫃檯中心 （上櫃）
# BSR : Buy Sell Report , 分公司買賣進出表


class ThreadingDownloadBot(threading.Thread):
    def __init__(self,pid,queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.pid = pid  
    def run(self):
        while(True):
            Code = self.queue.get()
            retry = 0
            if len(Code) >= 5:
                retry = int(Code[4])
                Code = Code[0:4]
        
            print '[%d]Process:[%s] Left:%d retry:%d'%(self.pid,Code,self.queue.qsize(),retry)
            ret = self.RunImp(Code)
            if None == ret:
                retry +=1
                if retry > 3:
                    print '%s 下載三次失敗'%(Code)
                else:
                    retryCode = Code+str(retry)
                    self.queue.put(retryCode)
                    print '********fail*******'
                    sleep( 1 ) #有錯誤停1秒
            else:
                print '\t(%d)Write %s Finish...'%(self.pid,Code)
            
            self.queue.task_done()
        
class DownloadTSEBot(ThreadingDownloadBot):
    def __init__(self,pid,queue):
        super(DownloadTSEBot, self).__init__(pid,queue)
        self.name = "TSE BSR Download Bot."
    def RunImp(self,Code):

        # step 1. GetMaxPage and POST data
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
            except Exception,e:
                #print e
                return (None,None)
        
        # step 2. GetRawData
        def GetBSRawData(Code,MaxPageNum):
            try:
                url = 'http://bsr.twse.com.tw/bshtm/bsContent.aspx?StartNumber=%s&FocusIndex=All_%s'%(Code,MaxPageNum)
                req = urllib2.Request(url)
                response = urllib2.urlopen(req)
                html = response.read()
                return html
            except Exception , e:
                return None
        
        # step 3. RawToCSV
        def BSRawToCSV(BSRaw):
            
            #取得資料表title 
            '''
            <tr class='column_title_1'>     <td>序</td>     <td>證券商</td>     <td>成交單價</td>     <td>買進股數</td>     <td>賣出股數</td>   </tr>
            '''
            title_tr_pattern = u"<tr class='column_title_1'>(.*?)<\/tr>"
            title_tr = re.compile(title_tr_pattern)
            result_tr = title_tr.findall(BSRaw)
            title_td_pattern = u'<td *>\B(.*?)</td>'
            title_td = re.compile(title_td_pattern)
            result_td = title_td.findall(result_tr[0])            
            title = ','.join(title.decode('utf-8').encode('cp950') for title in result_td)
            #取得各分公司買賣內容
            td = '''
                    <td class='column_value_center'>               1</td>        <td class='column_value_left'>                 1233  彰銀台中</td>        <td class='column_value_right'>               8.65</td>        <td class='column_value_right'>               0</td>        <td class='column_value_right'>               20,000</td>     
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
                content_list.append(row.decode('utf-8').encode('cp950'))
            sortedlist = sorted(content_list,key = lambda s: int(s.split(',')[0]))
            #將Title加入資料首列
            sortedlist.insert(0,title)
            return sortedlist
        
        def CSVToFile(CSVData,filename):
            with open('BSR/'+filename, 'wb') as csvfile:
                content = '\n'.join(row for row in CSVData)
                csvfile.write(content)
                
        self.RawBSR = "TSE"
        self.date,MaxPageNum = GetDateAndspPage(Code)
        print Code , self.date , MaxPageNum
        if None == MaxPageNum or "" == MaxPageNum:
            return None
        BSRawData = GetBSRawData(Code, MaxPageNum)
        if None == BSRawData:
            return None
        filename = "%s_%s.csv"%(Code,self.date) 
        CSVData = BSRawToCSV(BSRawData)
        CSVToFile(CSVData, filename)
        return True
      
class DownloadOTCBot(ThreadingDownloadBot):
    def __init__(self,pid,queue):
        super(DownloadOTCBot, self).__init__(pid,queue)
        self.name = "OTC BSR Download Bot."
    
    def RunImp(self,Code):
        
        def DownloadOTC(Code,filename,otcdate):
            try:
                base_url = 'http://www.gretai.org.tw/ch/stock/aftertrading/broker_trading/download_ALLCSV.php'
                PostDataDict = {'curstk':Code
                                , 'fromw':'0'
                                ,'numbern':'100'
                                ,'stk_date':otcdate
                                }
            
                postData = urllib.urlencode( PostDataDict)
                req = urllib2.Request( base_url , postData)
                response = urllib2.urlopen( req)
                html = response.read()
            except Exception , e:
                return None
            with open('BSR/'+filename, 'wb') as csvfile:
                content = '\n'.join(row for row in html.split(',,')[1:])
                csvfile.write(content)
            return True
        
        def getOTCDate(Code):
            baseUrl = "http://www.gretai.org.tw/ch/stock/aftertrading/broker_trading/brokerBS.php"
            postDataDict = {
                'stk_code' : Code
            }
            postData = urllib.urlencode( postDataDict)
            req = urllib2.Request( baseUrl , postData)
            response = urllib2.urlopen(req)
            html = response.read()    
            date_list = re.findall(u'<input type="hidden" name="stk_date" value=(.*)>',html)
            for date in date_list:
                return date
            return None
        
        self.RawBSR = "OTC"
        otcDate = getOTCDate(Code)
        if otcDate == None:
            return None
        
        filename = "%s_%d%s.csv"%(Code,int(otcDate[0:3])+1911,otcDate[3:]) 
        ret = DownloadOTC(Code,filename,otcDate)
        if None == ret:
            return None
        return True

def getCodeDict():
    CodeDict = {'TSE' : [] , 'OTC': [] } 
    with open('data/smast.dat','r') as f:
        for row in f:
            try:
                code = row[:6].strip()
                row = row.decode('utf-8').encode('cp950')
                if len(code)== 4 : #忽略權證,公司債
                    print row[:13].decode('cp950').encode('utf-8')
                    if row[12] == '0': #TSE_上市
                        CodeDict['TSE'].append(row[:4])
                    if row[12] == '1': #OTC_上櫃
                        CodeDict['OTC'].append(row[:4]) 
            except IndexError:
                print 'You have an empty row'    
        sleep(5)
    return CodeDict
        
if __name__ == '__main__':
    if not os.path.exists('BSR'):
        os.makedirs('BSR')    
    print 'Start...'
    CodeDict = getCodeDict()
    print 'TSE:%d OTC:%d'%(len(CodeDict['TSE']),len(CodeDict['OTC']))
    tStart = time()

    OTCqueue = Queue.Queue() 
    for i in range(20):
        t = DownloadOTCBot(i,OTCqueue)
        t.setDaemon(True)
        t.start()
    
    TSEqueue = Queue.Queue()
    for i in range(3):
        t = DownloadTSEBot(i,TSEqueue)
        t.setDaemon(True)
        t.start()        

    for Code in CodeDict['OTC']:
        OTCqueue.put(Code)
        
    for Code in CodeDict['TSE']:
        TSEqueue.put(Code)
    

    OTCqueue.join()
    TSEqueue.join()
    
    tEndTSE = time()

    print 'End...Total(%f)'%(tEndTSE-tStart)

    


