# -*- coding: utf-8 -*-
import urllib
import lxml.etree
import xml.etree.cElementTree as ET
import  xml.dom.minidom
import gzip
import StringIO
con=urllib.urlopen("http://wthrcdn.etouch.cn/WeatherApi?citykey=101190401")
#tree = ET.parse("http://wthrcdn.etouch.cn/WeatherApi?citykey=101190401")
data = StringIO.StringIO(con.read())
data  = gzip.GzipFile(fileobj=data).read()
data = StringIO.StringIO(data)
print data
tree = ET.parse(data)
root = tree.getroot()
print root.tag, "---", root.attrib
for child in root:
    print child.tag, "---", child.attrib
#doc = lxml.etree.parse(data)
#dom = xml.dom.minidom.parse(data)