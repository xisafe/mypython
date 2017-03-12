#https://zhuanlan.zhihu.com/p/21401793?refer=loan-analytics
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(context='notebook',style="ticks",palette="GnBu_d",font_scale=1.5,font='ETBembo',rc={"figure.figsize": (10, 6)})
#plt.rcParams['figure.figsize']=(15,10)
import warnings
warnings.filterwarnings('ignore') #为了整洁，去除弹出的warnings
pd.set_option('precision', 5) #设置精度
pd.set_option('display.float_format', lambda x: '%.5f' % x) #为了直观的显示数字，不采用科学计数法
pd.options.display.max_rows = 200 #最多显示200行
data = pd.read_csv('D:/LoanStats_2016Q1.csv',skiprows=0,header=1)

