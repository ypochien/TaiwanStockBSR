# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import urllib2,urllib
import sys
import csv
from datetime import datetime
import os
import threading
from time import strftime
from time import sleep
from time import time

from types import *

# TSE : Taiwan Stock Exchange , 台灣證交所 （上市）
# OTC : Over-the-Counter , 櫃檯中心 （上櫃）
# BSR : Buy Sell Report , 分公司買賣進出表

class DownloadBotBase(object):
    def __init__(self):
        self.name = "Newbie Bot."
        self.date = strftime('%Y%m%d')
        self.lstCode = []

    def __str__(self):
        return self.name
    
    def Date(self):
        return str(self.date)
    
    def setDate(self,date):
        self.date = date
    
    def Code(self):
        return self.lstCode
         
    def showCode(self):
        print '[%s]' % ', '.join(map(str, self.lstCode ))
    
    def setCode(self,Code):
        if type(Code) in [ IntType , StringType ]:
            if len(str(Code).strip()) != 4: # 只接受4位數的商品
                return
            self.lstCode.append(str(Code))
            
        elif type(Code) == ListType:
            self.lstCode += Code
            
    def Run(self):
        count = 0
        retry = 0
        print 'Total:%d'%len(self.Code())
        lstRetry = []
        while(len(self.Code())):
            Code = self.Code().pop(0)
            count += 1
            filename = "%s_%s.csv"%(Code,self.Date())
            print 'Process:[%s] Download:%d Left:%d retry:%d'%(Code,count,len(self.Code()),retry)
            ret = self.RunImp(Code, filename)

            if None == ret:
                self.setCode(Code)
                if lstRetry.count(Code) > 3:
                    print '%s 下載三次失敗'%(Code)
                    lstRetry = [x for x in lstRetry if x != Code ]
                    continue
                lstRetry.append(Code)         
                retry += 1
                count -= 1
                print '********fail*******'
                sleep( 3 ) #有錯誤停3秒
                continue
            print '\tWrite %s Finish...'%(filename)
        
        
class DownloadTSEBot(DownloadBotBase):
    def __init__(self):
        super(DownloadTSEBot, self).__init__()
        self.name = "TSE BSR Download Bot."
        
    def RunImp(self,Code,filename):

        # step 1. GetMaxPage and POST data
        def GetMaxPageNum(Code):
            try:
                base_url = 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx'
                req = urllib2.Request(base_url)
                response = urllib2.urlopen(req)
                html = response.read()
                soup = BeautifulSoup(html)
                __VIEWSTATE  = soup.find(attrs={"id": "__VIEWSTATE"})['value']
                sp_Date  = soup.find(attrs={"id":"sp_Date"}).contents[0]
                print 'Trade date(%s)'%sp_Date
                __EVENTVALIDATION = soup.find(attrs={"id": "__EVENTVALIDATION"})['value']#.get('Value')
        
                PostDataDict = {'__EVENTTARGET':''
                                , '__EVENTARGUMENT':''
                                ,'HiddenField_page':'PAGE_BS'
                                ,'txtTASKNO':Code
                                ,'hidTASKNO':Code
                                ,'__VIEWSTATE':__VIEWSTATE
                                ,'__EVENTVALIDATION':__EVENTVALIDATION
                                ,'HiddenField_spDate':sp_Date
                                ,'btnOK':'%E6%9F%A5%E8%A9%A2'}
                
                postData = urllib.urlencode( PostDataDict)
                req = urllib2.Request( base_url , postData)
                response = urllib2.urlopen( req)
                html = response.read()
                soup = BeautifulSoup(html)
                MaxPageNum  = soup.find(attrs={"id":"sp_ListCount"}).contents[0]
                return MaxPageNum
            except Exception,e:
                #print e
                return None
        
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
            soup = BeautifulSoup(BSRaw)
            #取得資料表title 
            title_contents =  soup.find(attrs={ 'class': 'column_title_1'})
            title_list = title_contents.find_all('td')
            title = [title.get_text().encode('big5') for title in title_list]
            
            #取得各分公司買賣內容
            stock_info_content = soup.find_all(attrs={'class':['column_value_price_3','column_value_price_2']})
            CSVData = []
            for i in stock_info_content:
                row_list = i.find_all('td')
                row = [row.get_text().strip().encode('big5') for row in row_list]
                if len(row[0]) > 0:
                    #print '[%s]'%row[0]
                    CSVData.append(row)
            #使用序號排序,因為網頁奇偶數沒穿插
            CSVData.sort(key=lambda element: int(element[0]))
            #將Title加入資料首列
            CSVData.insert(0, title) 
            return CSVData
        
        def CSVToFile(CSVData,filename):
            with open('BSR/'+filename, 'wb') as csvfile:
                writer = csv.writer(csvfile,dialect='excel')
                writer.writerows(CSVData)            
        
        self.RawBSR = "TSE"
        MaxPageNum = GetMaxPageNum(Code)
        if None == MaxPageNum:
            return None
        BSRawData = GetBSRawData(Code, MaxPageNum)
        if None == BSRawData:
            return None
        CSVData = BSRawToCSV(BSRawData)
        CSVToFile(CSVData, filename)
        return True
      
class DownloadOTCBot(DownloadBotBase):
    def __init__(self):
        super(DownloadOTCBot, self).__init__()
        self.name = "OTC BSR Download Bot."
    
    def RunImp(self,Code,filename):
        
        def DownloadOTC(Code,filename,otcdate):
            try:
                #otcdate = str(otcyear)+g_date[4:]
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
            with open('BSR/'+filename, 'wb') as f:
                f.write(html)
                
            return True
        
        self.RawBSR = "OTC"
        ret = DownloadOTC(Code,filename,self.getOtcDate())
        if None == ret:
            return None
        return True
            
    def getOtcDate(self):
        otcyear = int(self.date)/int(10000) - 1911
        otcdate = str(otcyear) + self.date[4:]
        return otcdate
    
    def setOtcDate(self,date):
        self.date =  str(int(date[0:3]) + 1911) + date[3:] #1020101

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
    
    tradedate = '20131121'
    
    tStart = time()
    
    OTC = DownloadOTCBot()
    OTC.setDate(tradedate)
    OTC.setCode(CodeDict['OTC'])
    OTC.Run()
    
    tEndOTC = time()
    
    TSE = DownloadTSEBot()
    TSE.setDate(tradedate)
    TSE.setCode(CodeDict['TSE'])
    TSE.Run()
    tEndTSE = time()
    


    print 'End...TSE(%f) OTC(%f) Total(%f)'%(tEndTSE-tEndOTC,tEndOTC-tStart,tEndTSE-tStart)

    


