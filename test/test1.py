# encoding=gbk
import urllib
import urllib2
import cookielib
import webbrowser
url='http://192.168.0.98:9000/cv/login'
values={'user':'test','password':'test'}
data = urllib.urlencode(values)
headers ={"User-agent":"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1"}
req = urllib2.Request(url, data,headers)
cj=cookielib.CookieJar()
opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
response =opener.open(req)
the_page =response.read().decode("utf-8") #.encode("gbk")
#print the_page
#p2=urllib.urlopen("")
webbrowser.open("http://192.168.0.98:9000/cv/meta/CardValue/analyses/%E6%97%A5%E7%9B%91%E6%8E%A7%E6%8C%87%E6%A0%87/%E6%96%B0%E6%97%A7%E6%B5%81%E7%A8%8B%E7%94%A8%E6%88%B7%E6%95%B0%E6%AF%94%E5%AF%B9v2")
