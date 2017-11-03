# encoding=utf-8
import pandas as pd
from myworks import cv_dw
import jieba
import numpy
from wordcloud import WordCloud,STOPWORDS
import matplotlib.pyplot as plt
mssql = cv_dw.MSSQL()
sql2 = """select content from t_fact_mobileApp_FeedBack where content>''"""
#df = pd.read_sql(sql2, mssql.conn)
jieba.add_word(u'小企额')
stopwords= {}.fromkeys([line.rstrip() for line in open('d:\outwords.txt')]) #如果确保唯一可以直接用list
segments = []
for lines in mssql.ExecQuery(sql2):
    if  len(lines[0])>4:
        segs = jieba.cut(lines[0])
        for seg in segs:
            if len(seg)>1 and seg.encode('utf-8')  not in stopwords.keys():
               segments.append(seg)

segmentDF = pd.DataFrame({'segment':segments})
segStat = segmentDF.groupby(by=["segment"])["segment"]\
                .agg({"计数":numpy.size}).reset_index()\
                .sort_values(by=["计数"],ascending=False)
                #.sort(columns=["计数"],ascending=False);
#print segStat.head(50)
segStat=segStat[segStat["计数"]>2]
wordcloud = WordCloud(font_path='c:\windows\\fonts\STCAIYUN.TTF',background_color="white",margin=5, width=1800, height=1000)#background_color="white"
#必须要加载文字体不然中文乱码
wordcloud = wordcloud.fit_words(segStat.head(1000).itertuples(index=False))
plt.figure(num=None, figsize=(1800, 1000), dpi=800, facecolor='w', edgecolor='k')
plt.imshow(wordcloud)
plt.axis("off")
plt.show()
plt.close()