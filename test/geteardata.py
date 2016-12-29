
import urllib
import urllib2
from bs4 import BeautifulSoup
import re
import os
import shutil
year = 1901
endYear = 1911
urlHead = 'http://ftp3.ncdc.noaa.gov/pub/data/noaa/'
while year < endYear:
    #根据年份建立文件夹
    if os.path.isdir(str(year)):
        shutil.rmtree(str(year))   
    os.mkdir(str(year))
    #下载页面   
    page = urllib2.urlopen(urlHead+str(year))
    soup = BeautifulSoup(page, from_encoding="gb18030")
    #提取文件名并下载
    for link in soup.findAll('a'):
        if link.getText().find('.gz') != -1:
            filename = link.getText()

            #下载文件到相应文件夹
      urllib.urlretrieve(urlHead+str(year)+'/'+filename, str(year)+'/'+filename)
    year += 1


