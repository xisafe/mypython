# coding=utf-8
from lxml import etree
import urllib2
import urllib
import jieba
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import datetime
#url='http://guba.eastmoney.com'

def reportLoanCustStat():
    db = md. mongo_db14_rpt()
    col_app_data = db.applicationData
    col_credit = db.applicationCreditScore
    col_fraud = db.applicationFraudScore
    col_rpt_stat = db.reportLoanCustStat
    col_rpt_stat.drop()
    statList= []
    i = 0
    for rec in col_app_data.find():
        data = rec["data"]
        if "cashadvStatus" in data and (data["cashadvStatus"] == u"还款清算" or data['cashadvStatus'] == u"关闭"):
            stat = {}
            stat["cashadvStatus"] = data["cashadvStatus"]
            stat["cashadvId"] = data["cashadvId"]
            stat["cashadvCreateDate"] = data["cashadvCreateDate"]
            stat["creditDate"] = data["creditDate"]
            stat["isRenew"] = data["isRenew"]

            if "paymentResult" in rec:
                payback = rec["paymentResult"]
                stat["totalPayback"] = payback["totalPayback"]
                stat["actualPayback"] = payback["actualPayback"]
                stat["paybackDays"] = payback["paybackDays"]
                stat["isDefault"] = payback["isDefault"]
                stat["isLost"] = payback["isLost"]
                stat["isNoPayback"] = payback["isNoPayback"]

            fraudScore = col_fraud.find_one({"cashadvId":rec["cashadvId"] })
            if fraudScore:
                fraudScoreList = fraudScore["data"]
                for fs in fraudScoreList:
                    vStr = "fraudScore_V"+fs["version"]
                    vStr = vStr.replace('.','_') # mongo doesnot support "."
                    stat[vStr] = fs["score"]

            creditScore = col_credit.find_one({"leadId":rec["leadId"]})
            if creditScore:
                creditScoreList = creditScore["data"]
                for cs in creditScoreList:
                    vStr = "creditScore_V"+cs["version"]
                    vStr = vStr.replace('.','_') # mongo doesnot support "."
                    stat[vStr] = cs["score"]
                    vStr = "creditLevel_V"+cs["version"]
                    vStr = vStr.replace('.','_') # mongo doesnot support "."
                    stat[vStr] = cs["level"]

            col_rpt_stat.save(stat)

            i = i+1

            if i%100==0:
                print(i,' records added')
