#!/usr/bin/env python2
#-*-encoding:utf-8-*-
import os
import pandas as pd
import os.path
import datetime
import re
saveDays=[1,8,15,22,28]
def getAllFiles(rootPath):
    fileList=[]
    regex = r"\d{8}"
    for root,dirs,files in os.walk(rootPath):
        for f in files:
            if re.search(regex,f):  
			fileList.append((f,os.path.join(root,f)))
    return fileList
    
if __name__ == '__main__':
    fdf=pd.DataFrame(getAllFiles(r'e:\myfirst'),columns=['fname','fpath'])
    fdf['varDate']=fdf['fname'].apply(lambda x:x.split('-')[1].replace('\r',''))
    fdf['dates']=pd.to_datetime(fdf['varDate'],format='%Y%m%d')
    del fdf['varDate'],fdf['fname']
    #fdf['now']=datetime.datetime.now()
    fdf['diffdays']=(datetime.datetime.now()-fdf['dates']).apply(lambda x: x.days)
    fdf['inflag']=fdf['dates'].dt.day.apply(lambda x: x in saveDays)
    #fdf['delflag1']=fdf['diffdays']<91
    #fdf['delflag2']=(fdf['diffdays']<181) & (fdf['inflag']==True)
    #fdf['delflag3']=(fdf['diffdays']>181) & (fdf['dates'].dt.day==1)
    fdf['delflag']=(fdf['diffdays']<91) | ((fdf['diffdays']<181) & (fdf['inflag']==True)) | ((fdf['diffdays']>181) & (fdf['dates'].dt.day==1))
    print fdf 
    for rp in fdf[fdf['delflag']==False]['fpath']:
        print rp
        os.remove(rp)
