import json
import subprocess
import time
import confs
import os
import pandas as pd
from urllib.request import urlopen  
from urllib.request import Request    
from cons import conn
now = time.time()
file_path=confs.main_path+'/data/ipaddr/'+str(int(now))+'.csv'
this_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(now)) 
def get_last_time(path='data/ipaddr'):
    times=[]
    for i in os.walk(confs.main_path+path):
        for p in i[2]:
            if p.startswith('1') and p.endswith('.csv'):
                try:
                    times.append(int(p.replace('.csv','')))
                except :
                    print('文件名不符合规则')
    times=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(max(times)-10))      
    return  times   
def get_ips(start_time,end_time):
    sql="""
        select DISTINCT t1.ip from collection.call_loan_device_info t1 
        where  t1.ip>'' and create_time between '{0}' and '{1}'
         union all 
        select distinct addip from java_xingfuqianzhuang.borrow t1  
        where  t1.addip>'' and t1.add_time between '{0}' and '{1}' 
        union all
        select distinct addip from java_xingfuqianzhuang.borrow_zt t1  
        where  t1.addip>'' and t1.add_time between '{0}' and '{1}'
        union all
        select distinct addip from java_xingfuqianzhuang.borrow_invest t1 
        where  t1.addip>'' and t1.create_time between '{0}' and '{1}' 
        union all
        select distinct addip from java_xingfuqianzhuang.borrow_invest_zt t1 
        where  t1.addip>'' and t1.create_time between '{0}' and '{1}' 
        union all
         select distinct reg_ip from java_xingfuqianzhuang.user t1 
        where  t1.reg_ip>'' and t1.create_time between '{0}' and '{1}'
        union all
        select distinct add_ip from  sljr_jrxj.real_user t1 
        where  t1.add_ip>'' and t1.create_time between '{0}' and '{1}' 
        union all
        select distinct add_ip from sljr_jrxj.recharge_log t1  
        where  t1.add_ip>'' and t1.create_time between '{0}' and '{1}' 
        union all
        select distinct reg_ip from sljr_jrxj.user t1  
        where  t1.reg_ip>'' and t1.create_time between '{0}' and '{1}' 
        union all
        select distinct t1.ip from  sljr_risk.approve_borrower_info t1 
        where t1.ip>'' and t1.create_time between '{0}' and '{1}'
        union all
        select distinct t1.ip from sljr_risk.user_device_info t1  
        where  t1.ip>'' and t1.create_time between '{0}' and '{1}' 
        union all
        select distinct add_ip from sljr_slsw.real_user t1
        where t1.add_ip>'' and t1.create_time between '{0}' and '{1}'
         union all
        select distinct add_ip from sljr_slsw.recharge_log t1 
        where  t1.add_ip>'' and t1.create_time between '{0}' and '{1}' 
        union all
        select distinct add_ip from sljr_slsw.slbusy_user_borrow_base_info t1 
        where  t1.add_ip>'' and t1.create_date between '{0}' and '{1}'
         union all
        select distinct reg_ip from  sljr_slsw.user t1  
        where  t1.reg_ip>'' and t1.create_time between '{0}' and '{1}' 
        """
    egine=conn.sljr_mysql()
    ips=pd.read_sql(sql.format(start_time,end_time),egine)
    return ips
        
    
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
    #load_time=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) 
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
        #nulls['load_time']=load_time
        return nulls
    else:
        ipdata=jsondata['data']  
        for k in ipdata.keys():
            if ipdata[k] in['','-1']:
                ipdata[k]='NULL'
        #ipdata['load_time']=load_time
        return  ipdata 
if __name__ == '__main__':  
    start = time.time()
    start_time=get_last_time()
    ips=get_ips(start_time,this_time)
    ips=ips.drop_duplicates()
    f=open(file_path,mode='a+',encoding='utf-8')
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
            time.sleep(0.2)
        except Exception as e:
            print(ip,'error:',str(e))
    f.close()
    #end = time.time()
    rerun_sh="""hive -e "load data local inpath '{0}' into table  sdd.swzn_ip_inc";""".format(file_path)
    print(rerun_sh)
    popen = subprocess.Popen(rerun_sh, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    popen.wait()
    if popen.poll()==0:
        re_run_flag='success'
    else:
        re_run_flag='error'
        confs.send_mail('IP增量导入失败',file_path)
   
