import json
import subprocess
import time
#import confs
import os
from lxml import etree
import pandas as pd
from urllib.request import urlopen  
from urllib.request import Request    
from cons import conn
now = time.time()
def get_ipaddr(ip):  
     
    API = "http://ip.taobao.com/service/getIpInfo2.php?ip="
    url = API + ip
    """ 
    对于Request中的第二个参数headers，它是字典型参数，所以在传入时 
    也可以直接将个字典传入，字典中就是下面元组的键值对应 
    """  
    req =Request(url)  
    req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36")  
    req.add_header("GET",url)  
    req.add_header("Host","ip.taobao.com")  
    #req.add_header("Referer",url)  
    jsondata = json.loads(urlopen(req).read().decode('utf-8'))
    #print(jsondata)
    if jsondata['code'] == 1:
        nulls= {'area': 'NULL',
         'area_id': 'NULL',
         'city': 'NULL',
         'city_id': 'NULL',
         'country': 'NULL',
         'country_id': 'NULL',
         'county': 'NULL',
         'county_id': 'NULL',
         'isp': 'NULL',
         'isp_id': 'NULL',
         'region': 'NULL',
         'region_id': 'NULL'}
        nulls['ip']=ip
        return nulls
    else:
        ipdata=jsondata['data']  
        for k in ipdata.keys():
            if ipdata[k] in['','-1']:
                ipdata[k]='NULL'
        return  ipdata 

if __name__ == '__main__':  
    start = time.time()
    #start_time=get_last_time()
    #f=open('f:/1509622048.csv',mode='r',encoding='utf-8')
    #m=f.readlines()
    ips=pd.read_csv('e://ips.csv')
    #ips=ips[ips.index>=431279]
    f=open('f:/ip_parse.csv',mode='a+',encoding='utf-8')
    for i in ips.index:
        ip=ips.loc[i,'ip']
        try:
            load_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            ret = get_ipaddr(ip) #get_data(sys.argv[1])
            strs=ret['country']+','+ret['country_id']+','+ret['area']+','+ret['area_id']+','+ret['region']+','+ret['region_id']+','+ret['city']\
                 +','+ret['city_id']+','+ret['county']+','+ret['county_id']+','+ret['isp']+','+ret['isp_id']+','+ret['ip']+','+load_time
            f.write(strs)
            f.write('\n')
            print('解析中',i,load_time)
            #time.sleep(0.2)
        except Exception as e:
            print(ip,'error:',str(e))
    f.close() 
    
  
    
    
