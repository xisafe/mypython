__author__ = 'cardvalue'
# encoding:gbk
import cv_dw
import jieba
con=cv_dw.MSSQL()
numMap={}
for row in con.ExecQuery("SELECT content FROM t_fact_mobileApp_FeedBack"):
     if(row[0]>''):
         temp=list(jieba.cut(row[0],cut_all = True))  #.encode("utf-8")
         for words in temp:
             numMap[words]=numMap.get(words,0)+1

print numMap