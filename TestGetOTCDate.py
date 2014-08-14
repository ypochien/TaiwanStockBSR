# -*- coding: utf-8 -*-

import urllib2,urllib
import re

def getOTCDate(Code):
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
        return date
    return None

def DownloadOTC(Code,filename,otcdate):
    #print '[{}][{}][{}]'.format(Code,filename,otcdate)
    try:
        base_url_1 = r'http://www.gretai.org.tw/web/stock/aftertrading/broker_trading/download_ALLCSV.php?curstk={}&stk_date={}'.format(Code,otcdate)
        print base_url_1
        response = urllib2.urlopen(base_url_1)
        html = response.read()
        print html

    except Exception , e:
        print e
        return None

if __name__ == '__main__':
    print u'%s 下載三次失敗'%('1234')
    #otcdate =getOTCDate('1258')
    #DownloadOTC('1258','a',otcdate)