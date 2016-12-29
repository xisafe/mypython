# -*- coding: utf-8 -*-
__author__ = 'shawn'
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import time
import datetime
import json
import logging
import MySQLdb
from MySQLdb import cursors
import pymongo
import MysqlHelper
import urllib2
import threading
import multiprocessing

#采集CRM历史数据（2015年1月起）

def mongo_db():
    client = pymongo.MongoClient('localhost', 27017)
    db = client.mydb3 #连接库  20160414 之前mydb2
    return db

def mongo_db_tongdun(): #马宁的服务器
    client = pymongo.MongoClient('192.168.0.21', 27017)
    db = client.testfrom191 #连接库
    return db

def mongo_db_ent(): #0.21服务器
    client = pymongo.MongoClient('192.168.0.21', 27017)
    db = client.esb #连接库
    return db

def getJXLToken():
    url = 'https://www.juxinli.com/api/access_report_token?client_secret=6faf4041d77340c197dc8e49f625f0d1&hours=24&org_name=cardvalue'
    req = urllib2.Request(url)
    res = urllib2.urlopen(req)
    jstr= res.read()
    if len(jstr) > 0 :
        decoded = json.loads(jstr.decode('utf-8', 'ignore'))
        if decoded["success"] =='true':
            return decoded["access_token"]
        else:
            print '----get JXL token failed'
            exit()
    else:
        print '----call JXL token API failed'
        exit()


def getJXLReport():
#直接从JXL API 获取
    token =  getJXLToken()

    db = mongo_db()
    col_mca = db.t_cashadv_basic
    col_rpt = db.jxl_rpt

    threadingSum = threading.Semaphore(8)
    for mca in col_mca.find():
        if threadingSum.acquire():
            JXLReport(threadingSum,mca,token,col_rpt).start()
            threadingSum.release()


class JXLReport(threading.Thread):
    def __init__(self,threadingSum,mca,token,col):
        threading.Thread.__init__(self);
        self.threadingSum = threadingSum
        self.mca = mca
        self.token = token
        self.col =col

    def run(self):
        with self.threadingSum:
            mca = self.mca
            token = self.token
            col = self.col
            cnt = col.find({'_id':mca['cashadv_id']}).count()
            if not cnt >0:
                url = 'https://www.juxinli.com/api/access_report_data?access_token='+ token +'&name='+mca["OwnerName"]+ \
                      '&phone='+mca["OwnerCellphone"]+'&client_secret=6faf4041d77340c197dc8e49f625f0d1&idcard='+mca['OwnerSSN']
                req = urllib2.Request(url)
                res = urllib2.urlopen(req)
                jstr= res.read()
                rec = {'_id':mca['cashadv_id'],'acqDate':time.strftime('%Y-%m-%d %X', time.localtime())}
                if len(jstr) > 0 :
                    decoded = json.loads(jstr.decode('utf-8', 'ignore'))
                    if decoded["success"] =='true':
                        rec['HasJXL'] = 'Y'
                        rec['jxl'] = decoded
                        #col.update_one({ "_id" :rec["_id"] }, {'$set':decoded})
                        #col.update_one({ "_id" :rec["_id"] }, {'$set':{'HasJXL':'Y'}})
                        print str(mca['cashadv_id']) + ' found'
                    else:
                        rec['HasJXL'] = 'N'
                        #col.save(rec)
                        print str(mca['cashadv_id']) +' not found'
                    col.insert(rec)
                else:
                    print '----call JXL report API failed'
                    exit()
            else:
                print str(mca['cashadv_id']) +' exist'


def getTDReport(ip):
    # 从数据抓取平台获取
    if ip == '192.168.5.191':
        db1 = mongo_db_tongdun()
        col_risk_remote = db1.batchIdentifyRisk
    else:
        db1 = mongo_db_ent()
        col_risk_remote = db1.identifyRisk

    db = mongo_db()
    col_mca = db.t_cashadv_basic
    col_td = db.td_rpt


    for mca in col_mca.find():
        exist_td = col_td.find_one({'_id':mca['cashadv_id']})
        if not exist_td:
            td_data = col_risk_remote.find_one({'cashadvId':str(mca['cashadv_id'])})
            if td_data and td_data.has_key('data'):
                rec = {'_id':mca['cashadv_id'],'acqDate':time.strftime('%Y-%m-%d %X', time.localtime()),'data':td_data['data']}
                col_td.insert(rec)
                print 'find td ' + str(mca['cashadv_id'])
            else:
                print 'find no td ' +str(mca['cashadv_id'])

def getTDReport01():
    # 从数据抓取平台获取

    db1 = mongo_db_ent()
    col_risk_remote = db1.identifyRisk

    db = mongo_db()
    col_mca = db.t_cashadv_basic
    col_td = db.td_rpt


    for mca in col_mca.find():
        exist_td = col_td.find_one({'_id':mca['cashadv_id']})
        if not exist_td:
            td_data = col_risk_remote.find_one({'mobile':str(mca['OwnerCellphone'])})
            if td_data and td_data.has_key('data'):
                rec = {'_id':mca['cashadv_id'],'acqDate':time.strftime('%Y-%m-%d %X', time.localtime()),'data':td_data['data']}
                col_td.insert(rec)
                print 'find td ' + str(mca['cashadv_id'])
            else:
                print 'find no td ' +str(mca['cashadv_id'])

def getTDReport02():
    # 从数据抓取平台获取
    client = pymongo.MongoClient('127.0.0.1', 27017)
    db1 = client.mydb1 #连接库
    col_risk_remote = db1.reject_from_zxy

    db = mongo_db()
    col_mca = db.t_cashadv_basic
    col_td = db.td_rpt

    for mca in col_mca.find():
        exist_td = col_td.find_one({'_id':mca['cashadv_id']})
        if not exist_td:
            td_data = col_risk_remote.find_one({'cashadvId':str(mca['cashadv_id'])})
            if td_data and td_data.has_key('data'):
                rec = {'_id':mca['cashadv_id'],'acqDate':time.strftime('%Y-%m-%d %X', time.localtime()),'data':td_data['data']}
                col_td.insert(rec)
                print 'find td ' + str(mca['cashadv_id'])
            else:
                print 'find no td ' +str(mca['cashadv_id'])

def acqMCABasic():
    #CreateMCABasic().createTable()

    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    cls = AcqMCABasic(logger)
    cls.mysql2Mongo('t_cashadv_basic')
    cls.createIndxCashadvID('t_cashadv_basic')


class CreateMCABasic():

    mysql_host = '192.168.0.239'
    mysql_port = 3306
    mysql_user = "***"
    mysql_pass = "******" #请和CRM团队确认权限密码
    mysql_db = "test"
    #mysql_host = '192.168.0.89'
    #mysql_port = 3306
    #mysql_user = "***"
    #mysql_pass = "******"
    #mysql_db = "mca_crm"

    conn = None
    cursor = None


    def __init__(self):
        self.conn = self.getMysqlConn()
        self.cursor = self.conn.cursor()

    def getMysqlConn(self):
        return MySQLdb.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, \
                 passwd=self.mysql_pass, db=self.mysql_db, charset = 'utf8',cursorclass=MySQLdb.cursors.SSCursor)

    def createTable(self):
        self.cursor.execute('CALL CreateCashadvBasicTest()')
        self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()


class AcqMCABasic():
    mysql_host = '192.168.0.239'
    mysql_port = 3306
    mysql_user = "***"
    mysql_pass = "******"
    mysql_db = "test"
    #mysql_host = '192.168.0.89'
    #mysql_port = 3306
    #mysql_user = "***"
    #mysql_pass = "******"
    #mysql_db = "mca_crm"

    mongo_host = 'localhost'
    mongo_port = 27017

    conn = None
    cursor = None
    mongo = None
    mongodb = None

    def __init__(self, logger):
        self.logger = logger

        self.conn = self.getMysqlConn()
        self.cursor = self.conn.cursor()

        self.mongo = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port)
        self.mongodb = self.mongo['mydb3']

    def getMysqlConn(self):
        return MySQLdb.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, \
                 passwd=self.mysql_pass, db=self.mysql_db, charset = 'utf8',cursorclass=MySQLdb.cursors.SSCursor)

    def setMongoCollectionDocument(self, table, data):
        if(isinstance(data, dict) == False):
            return False
        else:
            self.mongodb[table].insert(data)

    def getMysqlTableDesc(self, table):
        sql = """desc %s""" % (table)
        n = self.cursor.execute(sql)
        data = self.cursor.fetchall()
        keys = []
        types = []
        for row in data:
            key = str(row[0])
            if(row[1].find('int') >= 0):
                type = 1

            elif (row[1].find('char') >= 0):
                type = 2
            elif (row[1].find('text') >= 0):
                type = 2
            elif(row[1].find('decimal') >= 0):
                type = 3
            else:
                type = 2
            keys.append(key)
            types.append(type)
        return keys, types

    def mysql2Mongo(self, table):
        self.mongodb[table].drop()
        keys, types = self.getMysqlTableDesc(table)

        sql = """select * from  %s """ % (table)

        n = self.cursor.execute(sql)
        data = self.cursor.fetchall()

        #print table, keys, types
        for row in data:
            ret = {}
            for k, key in enumerate(keys):
                #if key == 'cash_id':
                #    key = '_id'
                    #ret[key] = int(row[k])
                if(types[k] == 1):
                    if row[k]==None:
                        ret[key]= 0
                        continue
                    #print k, key, row
                    ret[key] = int(row[k])
                elif(types[k] == 2):
                    if row[k]==None:
                        ret[key]= ''
                        continue
                    ret[key] = str(row[k])
                elif(types[k] == 3):
                    if row[k]==None:
                        ret[key]= ''
                        continue
                    ret[key] = float(row[k])
                else:
                    if row[k]==None:
                        ret[key]= ''
                        continue
                    ret[key] = str(row[k])
            #if(table== 'hs_card') or (table== 'hs_hero'):
                #ret['rand'] = random.random()
            #print ret
            self.setMongoCollectionDocument(table, ret)

    def createIndxCashadvID(self,table):
        db = self.mongodb[table]
        db.ensure_index('cashadv_id', unique=True)


    def __del__(self):
        self.mongo.close()
        self.cursor.close()
        self.conn.close()


def AddFraudFlag():
    db = mongo_db()
    col_mca = db.t_cashadv_basic
    #欺诈商户定义

    #1）不考虑时间进度≤0.5的商户
    #2）0.5＜时间进度≤1：
    #  还款金额=当前所还全部金额
    #  还款金额/放款金额≤0.25*时间进度，认定为欺诈商户
	
	#  还款金额/放款金额≤0.05*时间进度，认定为欺诈商户  更改时间：2016-04-26


    now_date = datetime.datetime.now().date()
    for mca in col_mca.find():
        total_days = float(mca['PaybackDays'])
        try:
            creditDate = datetime.datetime.strptime(mca['ActualMerCreditDate'], "%Y-%m-%d").date()
        except:
            print str(mca['cashadv_id']) + 'has invalid credit date'
            continue
        elapsed_days = float((now_date - creditDate).days)
        if total_days != 0:
            period_progress = elapsed_days/total_days  #时间进度=（当前时间-放款时间）/融资天数
            if period_progress <= 0.5: #1）不考虑时间进度≤0.5的商户
                continue
            elif period_progress <= 1: #2）0.5＜时间进度≤1
                #还款金额=当前所还全部金额（totalReceive）
                payback_progress = float(mca['totalReceive'])/float(mca['TotalPayback'])
                 #  还款金额/放款金额≤0.05*时间进度，认定为欺诈商户
                if payback_progress <= (period_progress*0.05):
                    mca['IsFraud'] = 'Y'
                else:
                    mca['IsFraud'] = 'N'
            else: #时间进度＞1：
                #时间截点：理论到期日+14
                #还款金额=时间截点及之前所还全部金额  （receiveBeforeOverdue14）
                payback_progress = float(mca['receiveBeforeOverdue14'])/float(mca['TotalPayback'])
                #还款金额/放款金额≤0.05，认定为欺诈商户
                if payback_progress <= 0.05:
                    mca['IsFraud'] = 'Y'
                else:
                    mca['IsFraud'] = 'N'
            col_mca.save(mca)

def PrepareAntiFraudData():
    PrepareAntiFraudData_Basic()
    PrepareAntiFraudData_JXL()
    PrepareAntiFraudData_TD()

def PrepareAntiFraudData_Basic():
    db = mongo_db()
    db.fraud_analysis_data.drop()
    col_mca = db.t_cashadv_basic

    col_data = db.fraud_analysis_data
    #找出所有有欺诈标志的记录
    for mca in col_mca.find({"$or":[{'IsFraud':'Y'},{'IsFraud':'N'}]}) :
        data={}
        data['_id'] = mca['cashadv_id']
        if mca['IsFraud'] == 'Y':
            data['IsFraud'] = '01'
        else:
            data['IsFraud'] = '02'
        col_data.insert(data)

    for mca in col_mca.find({"IsFraud":{"$nin":['Y','N']}}) :
        data={}
        data['_id'] = mca['cashadv_id']
        data['IsFraud'] = '02'
        col_data.insert(data)

def PrepareAntiFraudData_JXL():
    db = mongo_db()

    col_jxl = db.jxl_rpt
    col_mca = db.t_cashadv_basic
    col_data = db.fraud_analysis_data
    #找出所有有欺诈标志的记录
    for data in col_data.find() :
        mca = col_mca.find_one({'cashadv_id':data['_id']})  #!!!!!!需要创建索引

        isValid = 'Y'
        #查询是否有聚信立数据
        jxl = col_jxl.find_one({'_id':data['_id']})

        if jxl and jxl.has_key('HasJXL') and jxl['HasJXL'] == 'Y':
            if len(jxl['jxl']['report_data']['data_source']) >0:

                age = abs(int(jxl['jxl']['report_data']['person']['age'])) #年龄

                #if age < 29:
                #    data['JXL_age'] = '01'
                #elif age < 34:
                #    data['JXL_age'] = '02'
                #elif age < 37:
                #    data['JXL_age'] = '03'
                #elif age < 43:
                #    data['JXL_age'] = '04'
                #else:
                #    data['JXL_age'] = '05'
                data['JXL_age'] = age

                if jxl['jxl']['report_data']['person']['gender'] == '男':
                    data['JXL_gender'] = '01'# 性别
                else:
                    data['JXL_gender'] = '02'
                #出生省份:去除“省”“市""自治区"后缀
                bp = jxl['jxl']['report_data']['person']['province']
                suff_position = 2 + bp.find('省') + bp.find('市') + bp.find('自治区')
                if suff_position == 0:
                    data['JXL_birthProvince'] = bp
                else:
                    data['JXL_birthProvince'] = bp[0:suff_position]

                try:
                    data['JXL_cellProvince'] = jxl['jxl']['report_data']['cell_behavior'][0]['behavior'][0]['cell_loc'] #手机省份
                except:
                    print str(data['_id'])

                for ds in jxl['jxl']['report_data']['data_source']:
                    if ds['category_value'] == '运营商':
                        if ds['name'].find('移动') >=0:
                            data['JXL_cellOperator'] = '01'
                        elif ds['name'].find('联通') >=0:
                            data['JXL_cellOperator'] = '02'
                        else:
                            data['JXL_cellOperator'] = '03'


                for ac in jxl['jxl']['report_data']['application_check']:
                    if ac['category'] == '号码绑定':
                        if ac['check_point'] == '是否收集登记号码的信息':
                            if not ac['result'] == '是':
                                isValid = 'N'

                    if ac['category'] == '身份验证':
                        #是否实名认证
                        if ac['check_point'].find('本人实名认证') >=0 :
                            if ac['result'] == '是':
                                data['JXL_cellRealName'] = '01'
                            else:
                                data['JXL_cellRealName'] = '02'

                    if ac['category'] == '网络黑名单':
                        #是否网贷黑名单(手机、身份证等）
                        if ac['check_point'].find('网贷黑名单') >=0 :
                            if ac['check_point'].find('身份证') >=0 :
                                if ac['result'] == '是':
                                    data['JXL_IDInLoanBlackList'] = '02'# 未出现
                                else:
                                    data['JXL_IDInLoanBlackList'] = '01'
                            if ac['check_point'].find('号码') >=0 :#申请人号码未出现在网贷黑名单上 申请人身份证号码未出现在法院黑名单上
                                if ac['result'] == '是':
                                    data['JXL_cellInLoanBlackList'] = '02'# 未出现
                                else:
                                    data['JXL_cellInLoanBlackList'] = '01'
                        #身份证出现在法院黑名单
                        if ac['check_point'].find('法院黑名单') >=0 :
                            if ac['result'] == '是':
                                data['JXL_IDIncourtBlackList'] = '02'# 未出现
                            else:
                                data['JXL_IDIncourtBlackList'] = '01'

                for ac in jxl['jxl']['report_data']['behavior_check']:

                    if ac['category'] == '呼叫行为':
                        #长时间关机
                        if ac['check_point'].find('长时间关机') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_cellCloseLong'] = '02'
                            else:
                                data['JXL_cellCloseLong'] = '01'
                        #绑定号码是新号码
                        if ac['check_point'].find('绑定号码是新号码') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_newCell'] = '02'# 未出现
                            else:
                                data['JXL_newCell'] = '01'

                        #呼叫澳门电话
                        if ac['check_point'].find('呼叫过澳门电话') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_callMacao'] = '02'# 未出现
                            else:
                                data['JXL_callMacao'] = '01'

                        #呼叫110
                        if ac['check_point'].find('呼叫过110') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_call110'] = '02'# 未出现
                            else:
                                data['JXL_call110'] = '01'

                        #呼叫120
                        if ac['check_point'].find('呼叫过120') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_call120'] = '02'# 未出现
                            else:
                                data['JXL_call120'] = '01'

                        #主动呼叫过的联系人出现在网贷黑名单上
                        if ac['check_point'].find('主动呼叫过的联系人出现在网贷黑名单上') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_callPersonInBlackList'] = '02'# 未出现
                            else:
                                data['JXL_callPersonInBlackList'] = '01'

                        #呼叫律师相关号码
                        if ac['check_point'].find('呼叫律师相关号码') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_callLawyer'] = '02'# 未出现
                            else:
                                data['JXL_callLawyer'] = '01'

                        #呼叫法院相关号码
                        if ac['check_point'].find('呼叫法院相关号码') >=0 :
                            if ac['result'] == '未出现':
                                data['JXL_callCourt'] = '02'# 未出现
                            else:
                                data['JXL_callCourt'] = '01'

                credit_card_call_in_cnt = 0
                credit_card_call_out_cnt = 0
                loan_call_in_cnt =0
                loan_call_out_cnt =0
                bank_call_in_cnt = 0
                bank_call_out_cnt = 0

                for ac in jxl['jxl']['report_data']['recent_need']:
                    if ac['req_type'] == '信用卡':
                        credit_card_call_out_cnt += ac['req_call_cnt']['call_out_cnt']
                        credit_card_call_in_cnt += ac['req_call_cnt']['call_in_cnt']
                    if ac['req_type'] == '贷款':
                        loan_call_out_cnt += ac['req_call_cnt']['call_out_cnt']
                        loan_call_in_cnt += ac['req_call_cnt']['call_in_cnt']
                    if ac['req_type'] == '银行':
                        bank_call_out_cnt += ac['req_call_cnt']['call_out_cnt']
                        bank_call_in_cnt += ac['req_call_cnt']['call_in_cnt']

                #if credit_card_call_in_cnt <1:
                #    data['JXL_recentCreditCardCallInCnt']  = '01'
                #elif credit_card_call_in_cnt <2:
                #    data['JXL_recentCreditCardCallInCnt']  = '02'
                #elif credit_card_call_in_cnt <4:
                #    data['JXL_recentCreditCardCallInCnt']  = '03'
                #else:
                #    data['JXL_recentCreditCardCallInCnt']  = '04'
                data['JXL_recentCreditCardCallInCnt']=credit_card_call_in_cnt


                #if credit_card_call_out_cnt <1:
                #    data['JXL_recentCreditCardCallOutCnt']  = '01'
                #elif credit_card_call_out_cnt <2:
                #    data['JXL_recentCreditCardCallOutCnt']  = '02'
                #elif credit_card_call_out_cnt <6:
                #    data['JXL_recentCreditCardCallOutCnt']  = '03'
                #elif credit_card_call_out_cnt <15:
                #    data['JXL_recentCreditCardCallOutCnt']  = '04'
                #else:
                #    data['JXL_recentCreditCardCallOutCnt']  = '05'
                data['JXL_recentCreditCardCallOutCnt']=credit_card_call_out_cnt

                cv = loan_call_in_cnt
                if cv <1:
                    dv  = '01'
                elif cv <2:
                    dv  = '02'
                elif cv <8:
                    dv  = '03'
                else:
                    dv  = '04'
                data['JXL_recentLoanCallInCnt'] = cv #dv

                cv = loan_call_out_cnt
                if cv <1:
                    dv  = '01'
                elif cv <3:
                    dv  = '02'
                elif cv <10:
                    dv  = '03'
                else:
                    dv  = '04'
                data['JXL_recentLoanCallOutCnt'] = cv #dv

                #if bank_call_in_cnt <1:
                #    data['JXL_recentBankCallInCnt']  = '01'
                #elif bank_call_in_cnt <2:
                #    data['JXL_recentBankCallInCnt']  = '02'
                #elif bank_call_in_cnt <4:
                #    data['JXL_recentBankCallInCnt']  = '03'
                #elif bank_call_in_cnt <10:
                #    data['JXL_recentBankCallInCnt']  = '04'
                #else:
                #    data['JXL_recentBankCallInCnt']  = '05'
                data['JXL_recentBankCallInCnt']=bank_call_in_cnt

                #if bank_call_out_cnt <2:
                #    data['JXL_recentBankCallOutCnt']  = '01'
                #elif bank_call_out_cnt <6:
                #    data['JXL_recentBankCallOutCnt']  = '02'
                #elif bank_call_out_cnt <13:
                #    data['JXL_recentBankCallOutCnt']  = '03'
                #elif bank_call_out_cnt <25:
                #    data['JXL_recentBankCallOutCnt']  = '04'
                #else:
                #    data['JXL_recentBankCallOutCnt']  = '05'
                data['JXL_recentBankCallOutCnt']=bank_call_out_cnt


                relative_call_in_cnt =0
                relative_call_out_cnt =0
                landlord_call_in_cnt = 0
                landlord_call_out_cnt = 0

                for ac in jxl['jxl']['report_data']['contact_list']:

                    if ac['phone_num'].find(mca['OwnerRelativePhone']) >= 0 and mca['OwnerRelativePhone']<>"":
                        relative_call_out_cnt += abs(ac['call_out_cnt'])
                        relative_call_in_cnt += abs(ac['call_in_cnt'])
                    elif ac['phone_num'].find(mca['OwnerRelativePhone']) >= 0 and mca['OwnerRelativePhone']=="":
                        relative_call_out_cnt = None
                        relative_call_in_cnt  = None

                    if ac['phone_num'].find(mca['landLordCellPhone']) >= 0 and mca['landLordCellPhone']<>"":
                        landlord_call_out_cnt += abs(ac['call_out_cnt'])
                        landlord_call_in_cnt += abs(ac['call_in_cnt'])
                    elif ac['phone_num'].find(mca['landLordCellPhone']) >= 0 and mca['landLordCellPhone']=="":
                        landlord_call_out_cnt = None
                        landlord_call_in_cnt = None

                cv = relative_call_in_cnt
                if cv <2:
                    dv  = '01'
                elif cv <11:
                    dv  = '02'
                elif cv <36:
                    dv  = '03'
                elif cv <94:
                    dv  = '04'
                else:
                    dv  = '05'
                data['JXL_relativeCallInCnt']  = cv #dv

                cv = relative_call_out_cnt
                if cv <2:
                    dv  = '01'
                elif cv <13:
                    dv  = '02'
                elif cv <39:
                    dv  = '03'
                elif cv <98:
                    dv  = '04'
                else:
                    dv  = '05'
                data['JXL_relativeCallOutCnt'] = cv #dv


                #if landlord_call_in_cnt <1:
                #    data['JXL_landLordCallInCnt']  = '01'
                #elif landlord_call_in_cnt <5:
                #    data['JXL_landLordCallInCnt']  = '02'
                #elif landlord_call_in_cnt <26:
                #    data['JXL_landLordCallInCnt']  = '03'
                #else:
                #    data['JXL_landLordCallInCnt']  = '04'
                data['JXL_landLordCallInCnt']=landlord_call_in_cnt

                #if landlord_call_out_cnt <1:
                #    data['JXL_landLordCallOutCnt']  = '01'
                #elif landlord_call_out_cnt <2:
                #    data['JXL_landLordCallOutCnt']  = '02'
                #elif landlord_call_out_cnt <6:
                #    data['JXL_landLordCallOutCnt']  = '03'
                #elif landlord_call_out_cnt <28:
                #    data['JXL_landLordCallOutCnt']  = '04'
                #else:
                #    data['JXL_landLordCallOutCnt']  = '05'
                data['JXL_landLordCallOutCnt']=landlord_call_out_cnt



                if isValid == 'Y':
                    data['HasJXL'] = '01'
                    col_data.save(data)
            else:
                print 'no ds ' + str(mca ['cashadv_id'])
                #删除没有data_source的聚信立报告数据
                #col_jxl.remove({'_id':jxl['_id']})


        else:
            print 'no jxl ' + str(mca ['cashadv_id'])

def PrepareAntiFraudData_TD():
    '''
    手机号命中同盾欺诈中级灰名单
    手机号命中同盾欺诈高级灰名单
    手机号命中失联名单
    手机号命中网贷黑名单
    身份证命中同盾欺诈中级灰名单
    身份证命中同盾欺诈高级灰名单
    身份证命中失联名单
    身份证命中法院执行名单
    身份证命中网贷黑名单

    '''
    db = mongo_db()
    col_td = db.td_rpt

    col_data = db.fraud_analysis_data
    #找出所有有欺诈标志的记录
    for data in col_data.find() :
        TD_cellInTDFraudMiddleBlackList = '02'   #手机号命中同盾欺诈中级灰名单
        TD_cellInTDFraudHighBlackList = '02'     #手机号命中同盾欺诈高级灰名单
        TD_cellInLostList = '02'                 #手机号命中失联名单
        TD_cellInLoanBlackList = '02'            #手机号命中网贷黑名单
        TD_IDInTDFraudMiddleBlackList = '02'     #身份证命中同盾欺诈中级灰名单
        TD_IDInTDFraudHighBlackList = '02'       #身份证命中同盾欺诈高级灰名单
        TD_IDInLostList = '02'                   #身份证命中失联名单
        TD_IDInCourtList = '02'                  #身份证命中法院执行名单
        TD_IDInLoanBlackList = '02'              #身份证命中网贷黑名单
        TD_multiLoanApplicationIn3Mon= '02'  		#3个月内申请人在多个平台申请借款
        TD_connectNameCardToMultiIDIn3Mon = '02'  #3个月内银行卡_姓名关联多个身份证
        TD_tooManyApplicationIn7Day= '02'         #7天内设备或身份证或手机号申请次数过多
        TD_IDFromHighRiskRegion= '02'				#身份证归属地位于高风险较为集中地区
        TD_IDFormatCheckError= '02'               #身份证格式校验错误

        td = col_td.find_one({'_id':data['_id']})
        if td and td.has_key('data'):
            for hit in td['data']['hit_rules']:

                if hit['name'].find('手机号命中同盾欺诈中级灰名单')>=0:
                    TD_cellInTDFraudMiddleBlackList = '01'
                if hit['name'].find('手机号命中同盾欺诈高级灰名单')>=0:
                    TD_cellInTDFraudHighBlackList = '01'
                if hit['name'].find('手机号命中失联名单')>=0:
                    TD_cellInLostList = '01'
                if hit['name'].find('手机号命中网贷黑名单')>=0:
                    TD_cellInLoanBlackList = '01'
                if hit['name'].find('身份证命中同盾欺诈中级灰名单')>=0:
                    TD_IDInTDFraudMiddleBlackList = '01'
                if hit['name'].find('身份证命中同盾欺诈高级灰名单')>=0:
                    TD_IDInTDFraudHighBlackList = '01'
                if hit['name'].find('身份证命中失联名单')>=0:
                    TD_IDInLostList = '01'
                if hit['name'].find('身份证命中法院执行名单')>=0:
                    TD_IDInCourtList = '01'
                if hit['name'].find('身份证命中网贷黑名单')>=0:
                    TD_IDInLoanBlackList = '01'
                if hit['name'].find('3个月内申请人在多个平台申请借款')>=0 :
                    TD_multiLoanApplicationIn3Mon  = '01'
                if hit['name'].find('3个月内银行卡_姓名关联多个身份证')	>=0 :
                    TD_connectNameCardToMultiIDIn3Mon  = '01'
                if hit['name'].find('7天内设备或身份证或手机号申请次数过多 ')>=0 :
                    TD_tooManyApplicationIn7Day   = '01'
                if hit['name'].find('身份证归属地位于高风险较为集中地区')>=0 :
                    TD_IDFromHighRiskRegion	= '01'
                if hit['name'].find('身份证格式校验错误')>=0 :
                    TD_IDFormatCheckError   = '01'

            data['TD_cellInTDFraudMiddleBlackList']  = TD_cellInTDFraudMiddleBlackList
            data['TD_cellInTDFraudHighBlackList'] = TD_cellInTDFraudHighBlackList
            data['TD_cellInLostList'] = TD_cellInLostList
            data['TD_cellInLoanBlackList'] = TD_cellInLoanBlackList
            data['TD_IDInTDFraudMiddleBlackList'] =  TD_IDInTDFraudMiddleBlackList
            data['TD_IDInTDFraudHighBlackList'] = TD_IDInTDFraudHighBlackList
            data['TD_IDInLostList'] = TD_IDInLostList
            data['TD_IDInCourtList'] = TD_IDInCourtList
            data['TD_IDInLoanBlackList'] = TD_IDInLoanBlackList
            data['TD_multiLoanApplicationIn3Mon']  = TD_multiLoanApplicationIn3Mon
            data['TD_connectNameCardToMultiIDIn3Mon']  = TD_connectNameCardToMultiIDIn3Mon
            data['TD_tooManyApplicationIn7Day']  = TD_tooManyApplicationIn7Day
            data['TD_IDFromHighRiskRegion'] = TD_IDFromHighRiskRegion
            data['TD_IDFormatCheckError']   = TD_IDFormatCheckError

            data['HasTD'] = '01'
            col_data.save(data)

        else:
            print 'no TongDun ' + str(data['_id'])


def calTDcount():
    db1 = mongo_db_tongdun()
    col_risk_remote = db1.batchIdentifyRisk
    a ={}
    for r in col_risk_remote.find():
        for d in r['data']['hit_rules']:
            if a.has_key(d['name']) :
                a[d['name']] +=1
            else:
                a[d['name']] =1
    print str(a)


def getEntInfo(ip):
    if ip=='192.168.0.21':
        db1 = mongo_db_ent()
    elif ip=='192.168.5.191':
        db1 = mongo_db_tongdun()
    entInfo = db1.queryEntInfo

    db = mongo_db()
    col_mca = db.t_cashadv_basic
    col_ent = db.ent_info

    for mca in col_mca.find():
        exist_td = col_ent.find_one({'leadsId':mca['lead_id']})
        if not exist_td:
            ent_data = entInfo.find_one({'leadsId':str(mca['lead_id'])})
            if ent_data and ent_data.has_key('leadsId') and len(ent_data['data']['basicItems'])>0:
                rec = {'_id':mca['cashadv_id'],'leadsId':mca['lead_id'],'acqDate':time.strftime('%Y-%m-%d %X', time.localtime())}
                # 中数智汇：商户主体性质 01：个体工商户 02：个人独资企业 03：企业 04：其他
                if ent_data['data']['basicItems'][0]['entType'].find("独资"):
                    rec['MerchantsNatureStr']='02'
                elif ent_data['data']['basicItems'][0]['entType'].find("个体"):
                    rec['MerchantsNatureStr']='01'
                elif ent_data['data']['basicItems'][0]['entType'].find("公司") or ent_data['data']['basicItems'][0]['entType'].find("企业"):
                    rec['MerchantsNatureStr']='03'
                else:
                    rec['MerchantsNatureStr']='04'

                # 中数智汇：企业经营年限 01：MA≦0.5 02：0.5<MA≦1 03：1<MA≦3 04：3<MA≦5 05：5<MA≦larger
                try:
                    esDate = datetime.datetime.strptime(ent_data['data']['basicItems'][0]['esDate'], "%Y-%m-%d").date()
                except:
                    print str(mca['lead_id']) + 'has invalid esDate date'
                    continue
                now_date = datetime.datetime.now().date()
                interval=float((now_date-esDate).days)/365
                #if interval<=0.5:
                #    rec['MAgeDec']='01'
                #elif interval>0.5 and interval<=1:
                #    rec['MAgeDec']='02'
                #elif interval>1 and interval<=3:
                #    rec['MAgeDec']='03'
                #elif interval>3 and interval<=5:
                #    rec['MAgeDec']='04'
                #elif interval>5:
                #    rec['MAgeDec']='05'
                #else:
                #    rec['MAgeDec']=''
                rec['MAgeDec']=interval

                # 中数智汇：注册资本 01：MC≦10万; 02：10万<MC≦50万 03：50万<MC≦100万 04：100万<MC≦larger 05：无
                MCapitalInt = float(ent_data['data']['basicItems'][0]['regCap'])
                #if MCapitalInt<=10:
                #    rec['MCapitalInt']='01'
                #elif MCapitalInt>10 and MCapitalInt<=50:
                #    rec['MCapitalInt']='02'
                #elif MCapitalInt>50 and MCapitalInt<=100:
                #    rec['MCapitalInt']='03'
                #elif MCapitalInt>100:
                #    rec['MCapitalInt']='04'
                #else:
                #    rec['MCapitalInt']='05'
                rec['MCapitalInt']=MCapitalInt

                #rec['MerchantsNatureStr'] = ent_data['data']['basicItems'][0]['entType']
                #rec['MAgeDec'] = ent_data['data']['basicItems'][0]['esDate']
                col_ent.insert(rec)
                print 'find enterprise ' + str(mca['lead_id'])
            else:
                print 'find no enterprise ' +str(mca['lead_id'])


if __name__ == '__main__':
    t2 = time.time()
    #取得聚信立报告
    getJXLReport()
    t3 = time.time()
    print("geting JXL report done. time:"+ str(t3 - t2))
'''
    t3 = time.time()
    #准备反欺诈分析输入数据
    PrepareAntiFraudData()
    t4 = time.time()
    print("PrepareAntiFraudData. time:"+ str(t4 - t3))

    t3 = time.time()
    getEntInfo('192.168.0.21')
    getEntInfo('192.168.5.191')
    t4 = time.time()
    print("getEntInfo. time:"+ str(t4 - t3))

    t1 = time.time()
    #创建 t_cashadv_basic 表
    acqMCABasic()
    t2 = time.time()
    print("acquring app_cashadv done. time:"+ str(t2 - t1))

    t3 = time.time()
    #添加欺诈标签
    AddFraudFlag()
    t4 = time.time()
    print("addding fraud flag done. time:"+ str(t4 - t3))



    t1 = time.time()
    #取得同盾报告
    #getTDReport('192.168.5.191')
    #getTDReport02()
    getTDReport01()
    t2 = time.time()
    print("acquring getTDReport done. time:"+ str(t2 - t1))
    #getTDReport()

    #准备反欺诈分析输入数据
    PrepareAntiFraudData()


    t1 = time.time()

    #创建 t_cashadv_basic 表
    acqMCABasic()
    t2 = time.time()
    print("acquring app_cashadv done. time:"+ str(t2 - t1))

    #取得聚信立报告
    getJXLReport()
    t3 = time.time()
    print("geting JXL report done. time:"+ str(t3 - t2))

    t3 = time.time()
    getEntInfo('192.168.0.21')
    getEntInfo('192.168.5.191')
    t4 = time.time()
    print("getEntInfo. time:"+ str(t4 - t3))




    t3 = time.time()
    getResultMongodb2Mysql()
    t4 = time.time()
    print("getResultMongodb2Mysql. time:"+ str(t4 - t3))

    t3 = time.time()
    #添加欺诈标签
    AddFraudFlag()
    t4 = time.time()
    print("addding fraud flag done. time:"+ str(t4 - t3))


    t2 = time.time()
    #print("acquring app_cashadv done. time:"+ str(t2 - t1))

    #取得聚信立报告
    getJXLReport()
    t3 = time.time()
    print("geting JXL report done. time:"+ str(t3 - t2))

    # 取得同盾报告
    getTDReport()
    #calTDcount()
'''





