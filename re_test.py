import re
import urllib2,urllib

def getPostKey(key):
     wanted_value = ["__VIEWSTATE","__EVENTVALIDATION"]
     for wanted in wanted_value:
          if wanted in key:
               return True
     return None
try:
     base_url = 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx'
     req = urllib2.Request(base_url)
     response = urllib2.urlopen(req)
     html = response.read()
     #print html
     #soup = BeautifulSoup(html)
     #改用re模組取代BeautifulSoup          
     
     
     PostDataDict = {'__EVENTTARGET':''
                     , '__EVENTARGUMENT':''
                     ,'HiddenField_page':'PAGE_BS'
                     ,'txtTASKNO':'2384'
                     ,'hidTASKNO':'2384'
                     ,'__VIEWSTATE':re.findall(u'id="__VIEWSTATE" value="(.*)" />',html)[0]
                     ,'__EVENTVALIDATION':re.findall(u'id="__EVENTVALIDATION" value="(.*)" />',html)[0]
                     ,'HiddenField_spDate':re.findall(u'id="sp_Date" name="sp_Date" style="display: none;">(.*)</span>',html)[0]
                     ,'btnOK':'%E6%9F%A5%E8%A9%A2'}

     postData = urllib.urlencode( PostDataDict)
     req = urllib2.Request( base_url , postData)
     response = urllib2.urlopen( req)
     html = response.read()
     print re.findall(u'<span id="sp_ListCount">(.*)</span>',html)[0]
     
     

except Exception,e:
     print e