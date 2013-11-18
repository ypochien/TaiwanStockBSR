# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import urllib2,urllib
import sys
import csv
from datetime import datetime
import os

g_date = ""
# 取得頁數
def getPageNumber(Code):
    '''
    得取得每一檔股票的最大頁數
    '''
    try:
        base_url = 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx'
        req = urllib2.Request(base_url)
        response = urllib2.urlopen(req)
        html = response.read()
        soup = BeautifulSoup(html)
        __VIEWSTATE  = soup.find(attrs={"id": "__VIEWSTATE"})['value']
        sp_Date  = soup.find(attrs={"id":"sp_Date"}).contents[0]
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
        global g_date
        g_date = sp_Date
        
        postData = urllib.urlencode( PostDataDict)
        req = urllib2.Request( base_url , postData)
        response = urllib2.urlopen( req)
        html = response.read()
        soup = BeautifulSoup(html)
        pagenums  = soup.find(attrs={"id":"sp_ListCount"}).contents[0]
        return pagenums
       
    except Exception , e:
        print e

def getStockInfoByCodeAndPageNum(Code, pages):
    #http://bsr.twse.com.tw/bshtm/bsContent.aspx?StartNumber=2330&FocusIndex=All_14 # 可忽略&flg_Print=1
    url = 'http://bsr.twse.com.tw/bshtm/bsContent.aspx?StartNumber=%s&FocusIndex=All_%s'%(Code,pages)
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    html = response.read()
    return html

def ListToCSV(dataList,filename): # 寫入 CSV
    if not os.path.exists('../BSR'):
        os.makedirs('../BSR')    
    with open('../BSR/'+filename, 'wb') as csvfile:
        writer = csv.writer(csvfile,dialect='excel')
        writer.writerows(dataList)
                         
def getFilenameByCodeAndDate(Code,date): # 透過 stock number 和 日期建立檔名
    format = "%Y_%m_%d"
    dateString = date.strftime(format)
    filename = '%s_%s.csv'%(Code,dateString)
    return filename

def parsingHtmlToList(html):
    soup = BeautifulSoup(html)
    #取得資料表title 
    title_contents =  soup.find(attrs={ 'class': 'column_title_1'})
    title_list = title_contents.find_all('td')
    title = [title.get_text().encode('big5') for title in title_list]
    
    #取得各分公司買賣內容
    stock_info_content = soup.find_all(attrs={'class':['column_value_price_3','column_value_price_2']})
    stock_info_list = []
    for i in stock_info_content:
        row_list = i.find_all('td')
        row = [row.get_text().strip().encode('big5') for row in row_list]
        if len(row[0]) > 0:
            #print '[%s]'%row[0]
            stock_info_list.append(row)
    #使用序號排序,因為網頁奇偶數沒穿插
    stock_info_list.sort(key=lambda element: int(element[0]))
    #將Title加入資料首列
    stock_info_list.insert(0, title)
    return stock_info_list
    
def downloadCSV(Code):
    pagenums = getPageNumber(Code)
    if pagenums == None:
        print '(%s)非上市股票'%Code
        return
    print u'股票(%s)最大頁數取得(%s)...'%(Code,pagenums)
    html = getStockInfoByCodeAndPageNum(Code,pagenums)
    stock_list = parsingHtmlToList(html)
    print u'筆數(%d)'%(len(stock_list)-1)
    print u'list to CSV...(%s_%s.csv)'%(Code,g_date) 
    filename = '%s_%s.csv'%(Code,g_date)
    ListToCSV(stock_list,filename)
    # 寫入資料
    print u'complieted\n\n'
    

def getCodeDict():
    CodeDict = {'TSE' : [] , 'OTC': [] } 
    with open('../data/smast.dat','r') as f:
        for row in f:
            try:
                if len(row[:6].strip())== 4 : #忽略權證,公司債
                    print row[:15]
                    if row[14] == '0': #TSE_上市
                        CodeDict['TSE'].append(row[:4])
                    if row[14] == '1': #OTC_上櫃
                        CodeDict['OTC'].append(row[:4])
            except IndexError:
                print 'You have an empty row'    
    return CodeDict
    
if __name__ == '__main__':
    CodeDict = getCodeDict()
    #code_list = ['1101','1102','5483']
    for code in CodeDict['TSE']:
        downloadCSV(code)