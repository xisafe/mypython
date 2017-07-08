import math
from matplotlib.ticker import FuncFormatter
import numpy as np
import urllib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import gridspec
from sqlalchemy import create_engine
def getHost(url='www.google.cn'):
    if not url.startswith('http'):
        url="http://"+url
    proto, rest = urllib.parse.splittype(url)
    res, rest = urllib.parse.splithost(rest)   
    return res.replace('www.','')
def getRateRange(rate):
     return 0   
def to_percent(y, position):
    s = str(100 * y)
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%' 
def plotDist(X,title='',bins_k=40):
    X=np.array(X)
    print(title)
    formatter = FuncFormatter(to_percent)
    plt.figure(figsize=(10, 6)) 
    gs = gridspec.GridSpec(2, 1, height_ratios=[1,12]) 
    ax0 = plt.subplot(gs[0])
    ax0.boxplot(X,vert=False)
    plt.grid()
    ax1 = plt.subplot(gs[1])
    ax1.set_ylabel('hist distribution')
    ax1.hist(X, bins=bins_k, normed=True,color='g')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.grid()
    ax2 = ax1.twinx()  # this is the important function
    ax2.set_ylabel(' cumulative distribution')
    plt.gca().yaxis.set_major_formatter(formatter)
    acc=ax2.hist(X, bins=bins_k, normed=True,color='red',cumulative=True,histtype='step')
    plt.annotate('Mean : '+format(X.mean(),'.2f')+'', xy=(-90, 1), xytext=(-90, 1))
    plt.annotate(' Std :' + format(X.std(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.95))
    plt.annotate(' Var :' + format(X.var(),'.2f')+'', xy=(-90, 1), xytext=(-90, 0.90))
    plt.tight_layout()
    plt.show()
    plt.close()
    return acc
if __name__ == '__main__':
    filePath = "/users/hua/documents/temp/prime_orders/"
    if 'data' not in dir():
        data=pd.read_csv('/users/hua/prime_orders.csv')
        del data['productname'],data['webdomain']
        data['hosts']=data['producturl'].apply(getHost)
        data['max_weight']=data[['weight','volumweight']].max(axis=1)
        data['weight_or_vol']=data['max_weight']==data['weight']
        zero_data=data[(data['max_weight']==0)|(data['chargeweight']==0)]
        data['diff_weight']=(data[['weight','volumweight']].max(axis=1)-data['chargeweight']/1.1)/1000
        data['diff_rate']=(data[['weight','volumweight']].max(axis=1)*1.1-data['chargeweight'])*100/data['chargeweight']
        norm_data=data[(data['max_weight']>0)&(data['chargeweight']>0)].copy()#
    #data['delivery_month']=data['arrive_date'].apply(lambda x: str(x)[0:7].replace('-',''))
    data.loc[data['diff_rate']>500,'diff_rate']=500
    #data.loc[data['diff_weight']>5,'diff_weight']=5
    #data.loc[data['diff_weight']<-5,'diff_weight']=-5
    zero_data_st=zero_data.groupby('hosts')['hosts'].count()
    norm_data.loc[norm_data['diff_rate']>500,'diff_rate']=500
    outlier=norm_data[norm_data['diff_rate']>100] #离群样本
    outlier_st=outlier.groupby('hosts')['hosts'].count()
    #outlier_st['占比']=outlier_st['hosts']/outlier_st.shape[0]
    plt.figure()
    plt.title('distributed by all diff_rate')
    norm_data['diff_rate'].hist(bins=40,figsize=(10,7))
    print("1、volumWeight与Weight同为0或者chargeweight为0 视为无效,已经排除 无效值占比: %.2f%%" % (zero_data.shape[0]*100/data.shape[0]))
    print("2、diff_rate>=500%的占比1.85% diff_rate>=200%占比3.22% diff_rate>100%占比5.42%" )
    print("3、diff_rate>100%占比5.42% 视为离群值 或离群点outlier,大概是4倍标准误差的范围，mean+4*std,也确保95%的样本" )
    plt.show()
    plt.close()
    valid_data=norm_data[norm_data['diff_rate']<=100] #正常群体样本
    plotDist(valid_data['diff_rate'],title='',bins_k=40)
    """fig =plt.figure(figsize=(10,7))
    formatter = FuncFormatter(to_percent)
    ax1 = fig.add_subplot(111)
    ax1.set_ylabel('diff_rate distribution')
    ax1.hist(valid_data['diff_rate'], bins=40, normed=True,color='g')
    plt.gca().yaxis.set_major_formatter(formatter)
    ax2 = ax1.twinx()  # this is the important function
    ax2.set_ylabel('diff_rate cumulative distribution')
    acc=ax2.hist(valid_data['diff_rate'], bins=40, normed=True,color='red',cumulative=True,histtype='step')
    acc=pd.DataFrame(acc[0],acc[1][1:41])
    acc.columns=['累计百分比']
    print('排除无效值和离群值后的diff_rate分布，如下：')
    #valid_data['diff_rate'].hist(bins=40,figsize=(10,7))
    plt.grid(True, color='b' , linewidth='0.3' ,linestyle='--')
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.show()
    plt.close()"""
    print('1、diff_rate<=0% 累计占比60.4% diff_rate<=10% 累计占比88.5% -10%<diff_rate<10% 占比约62%')
    print('2、分布非常对称')

    x=valid_data[valid_data['weight_or_vol']==True]['diff_rate']
    plotDist(x,title='按重量',bins_k=40)
    plotDist(valid_data[valid_data['weight_or_vol']==False]['diff_rate'],title='按体积重量',bins_k=40)
    #plotDist(data['diff_weight'],title='',bins_k=40)
'''    
 select 
o.ord_nr OrderNumber,
o.pkg_nr packageNumber,
o.prod_url_txt ProductUrl,
o.vndr_shop_nm vendrName,
o.qty,
o.trnsn_wrhs_cd WhareHouse,
o.vol_wght volumWeight,
o.wght Weight,
o.ord_charge_wght chargeWeight,
o.est_wght EstWeight,
-- o.prod_url_dmn_nm webDomain, -- o.prod_nm productName,
o.cust_ctry_cd catalog_code,
o.shpmt_type_nm ShipmentTypeName,
o.ez_dis_prod_catg_lvl_1_nm,
o.ez_dis_prod_catg_lvl_2_nm,
o.ez_dis_prod_catg_lvl_3_nm,
o.send_to_trnsn_wrhs_way_bill_cd,
trunc(o.ord_arrive_trnsn_wrhs_ts) arrive_date
 from dw.rz_sls_ord_f o where o.ord_arrive_trnsn_wrhs_ts>='2017-01-01'  and o.prch_type_cd='Prime'

plt.figure() #建立图像
p = pd.DataFrame(data['diff_rate']).boxplot()
x = p['fliers'][0].get_xdata() # 'flies'即为异常值的标签
y = p['fliers'][0].get_ydata()
y.sort() #从小到大排序，该方法直接改变原对象
#用annotate添加注释
#其中有些相近的点，注解会出现重叠，难以看清，需要一些技巧来控制。
#以下参数都是经过调试的，需要具体问题具体调试。
#xy表示要标注的位置坐标，xytext表示文本所在位置
for i in range(len(x)): 
  if i>0:
    plt.annotate(y[i], xy = (x[i],y[i]), xytext=(x[i]+0.05 -0.8/(y[i]-y[i-1]),y[i]))
  else:
    plt.annotate(y[i], xy = (x[i],y[i]), xytext=(x[i]+0.08,y[i]))
plt.show() #展示箱线图
'''          

 