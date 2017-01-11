#encoding:utf-8
import pandas as pd
import pymysql
import Relief as rf
import numpy as np
datapath=u'D:/个人征信'
def loanTrainDataToMysql(paths=datapath):
    #en=sql.create_engine('mysql://root:@127.0.0.1:3306/test')
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='test')
    loan_time_train = pd.read_csv(paths + '/train/loan_time_train.txt')
    loan_time_train.columns = ['userid', 'operdate']
    loan_time_train.to_sql(name='loan_time_train', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del loan_time_train
    overdue_train = pd.read_csv(paths + '/train/overdue_train.txt')
    overdue_train.columns = ['userid', 'delay']
    overdue_train.to_sql(name='overdue_train', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del overdue_train
    user_info_train = pd.read_csv(paths + '/train/user_info_train.txt')
    user_info_train.columns = ['userid', 'sex', 'vocation', 'edu', 'marry', 'household']
    user_info_train.to_sql(name='user_info_train', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del user_info_train
    bank_detail_train = pd.read_csv(paths + '/train/bank_detail_train.txt')
    bank_detail_train.columns = ['userid', 'operdate', 'trantype', 'tranamt', 'issalary']
    bank_detail_train.to_sql(name='bank_detail_train', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del bank_detail_train
    browse_history_train= pd.read_csv(paths + '/train/browse_history_train.txt')
    browse_history_train.columns = ['userid', 'operdate', 'saw', 'subsaw']
    browse_history_train['vars'] = browse_history_train['saw'].apply(lambda x: 'v' + str(x) + '_') + \
                                   browse_history_train['subsaw'].apply(lambda x: str(x))
    del browse_history_train['saw'], browse_history_train['subsaw']  # ,browse_history_train['operdate']
    browse_history_train = browse_history_train.groupby(by=['userid', 'vars']).count()  # .agg({u'nsize':numpy.size}) #,u'num':'count'
    browse_history_train['operdate'] = browse_history_train['operdate'].astype(int)
    browse_history_train = browse_history_train.unstack().fillna(0)  # .head(30)
    browse_history_train.columns = browse_history_train.columns.levels[1]
    browse_history_train.to_sql(name='browse_history_train', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del browse_history_train
    bill_detail_train = pd.read_csv(paths + '/train/bill_detail_train.txt')
    bill_detail_train.columns = ['userid', 'operdate', 'bankid', 'lastbill','lastpayback','cardamt','thisbillbalance','thisbillminpay','transnum','thisbill','adjustamt','rate','avabalance','creditamt','repaystatus']
    bill_detail_train.to_sql(name='bill_detail_train', flavor='mysql', con=conn, if_exists=u'replace',index=False)
    del bill_detail_train
def loanTestDataToMysql(paths=datapath):
    #en=sql.create_engine('mysql://root:@127.0.0.1:3306/test')
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='test')
    loan_time_test = pd.read_csv(paths + '/test/loan_time_test.txt')
    loan_time_test.columns = ['userid', 'operdate']
    loan_time_test.to_sql(name='loan_time_test', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del loan_time_test
    usersID_test = pd.read_csv(paths + '/test/usersID_test.txt')
    usersID_test.columns = ['userid']
    usersID_test.to_sql(name='overdue_test', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del usersID_test
    user_info_test = pd.read_csv(paths + '/test/user_info_test.txt')
    user_info_test.columns = ['userid', 'sex', 'vocation', 'edu', 'marry', 'household']
    user_info_test.to_sql(name='user_info_test', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del user_info_test
    bank_detail_test = pd.read_csv(paths + '/test/bank_detail_test.txt')
    bank_detail_test.columns = ['userid', 'operdate', 'trantype', 'tranamt', 'issalary']
    bank_detail_test.to_sql(name='bank_detail_test', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del bank_detail_test
    browse_history_test= pd.read_csv(paths + '/test/browse_history_test.txt')
    browse_history_test.columns = ['userid', 'operdate', 'saw', 'subsaw']
    browse_history_test.to_sql(name='browse_history_test', flavor='mysql', con=conn, if_exists=u'replace', index=False)
    del browse_history_test
    bill_detail_test = pd.read_csv(paths + '/test/bill_detail_test.txt')
    bill_detail_test.columns = ['userid', 'operdate', 'bankid', 'lastbill','lastpayback','cardamt','thisbillbalance','thisbillminpay','transnum','thisbill','adjustamt','rate','avabalance','creditamt','repaystatus']
    bill_detail_test.to_sql(name='bill_detail_test', flavor='mysql', con=conn, if_exists=u'replace',index=False)
    del bill_detail_test

if __name__ == "__main__":
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='test')
    sql="SELECT b.*,l.delay FROM browse_history_train b INNER JOIN overdue_train l on b.userid=l.userid"
    browse=pd.read_sql(sql, conn)
    rows, cols = browse.shape
    RankList=rf.ReliefF(np.array(browse.head(10).iloc[:,1:cols]),N=40,K=7,M=80)
    print RankList
    print browse.columns
