from datetime import datetime,timedelta
import pandas as pd
import urllib
def get_day_type(query):  
    """ 
    @query a single date: string eg."20160404" 
    @return day_type: 0 workday -1 holiday 
    20161001:2 20161002:2 20161003:2 20161004:1  
    """    
    url = 'http://www.easybots.cn/api/holiday.php?d=' + query   
    req = urllib.request.Request(url)  
    resp = urllib.request.urlopen(req)  
    content = resp.read()
    return content;
dim_date=pd.DataFrame()
dates=pd.date_range('2001/1/1','2099/12/31',freq='D') #data generation
dim_date['dt_dt']=dates

dim_date['dt_key']=dates.strftime('%Y%m%d')
dim_date['wk_str']=dim_date['dt_key'].apply(lambda x:get_day_type(x))
dim_date['dt_year']=dates.year
dim_date['dt_year_month']=dates.strftime('%Y%m')
dim_date['dt_month_frst_day']=''
dim_date['dt_month_last_day']=''
dim_date['dt_month_en_desc']=''
dim_date['dt_month_cn_desc']=''
dim_date['dt_day']=''
dim_date['dt_quarter']=''
dim_date['dt_week']=''
dim_date['dt_week_frst_day']=''
dim_date['dt_week_last_day']=''
dim_date['dt_week_en_desc']=''
dim_date['dt_week_cn_desc']=''
dim_date['dt_month_week']=''
dim_date['dt_year_week']=''
dim_date['dt_year_day']=''
dim_date['dt_wenkend_flag']=''
dim_date['dt_workday_flag']=''
dim_date['dt_hldy_flag']=''
dim_date['dt_hldy']=''




#date2 = pd.date_range(start='5/1/2017',end='6/1/2017',freq='W-MON') #从5月1日到6月1日，每周的周一
