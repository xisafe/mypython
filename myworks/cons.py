from sqlalchemy import create_engine
import pymongo
import psycopg2
import pandas as pd
import confs
class conn:
    def __init__(self):
        self.db_map={   "swzn_102":"coo",
                        "swdc_102":"dc_data_maintain",
                        "sljr":"hrdb",
                        "xfqz":"java_xingfuqianzhuang",
                        "jrxj":"sljr_jrxj",
                        "slsw":"sljr_slsw",
                        "bdgl":"sljr_borrow",
                        "csxt":"collection",
                        "yhcg":"sljr_bank",
                        "zhxt":"sljr_payment",
                        "lyxt":"sljr_pay_rout",
                        "fkxt":"sljr_risk"
                        }
        
    def xfqz(db='java_xingfuqianzhuang'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def sljr_mysql(db='java_xingfuqianzhuang'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def jrxj(db='sljr_jrxj'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def yhcg(db='sljr_bank'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def bdgl(db='sljr_borrow'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def zfxt(db='sljr_payment'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def slsw(db='sljr_slsw'):
        engine= create_engine("mysql+pymysql://dc_select:Ksdj@s2^dh@rr-uf6j7j02i75dxe651o.mysql.rds.aliyuncs.com:3306/{0}?charset=utf8".format(db))
        return engine;
    def sljr_pg():
        engine= create_engine("postgresql+psycopg2://postgres:postgres@192.168.188.103:5432/sljr_xfqz")
        return engine;
    def coo(db='coo'):
        engine= create_engine("mysql+pymysql://ccr:ccr@1234@192.168.188.102:3306/{0}?charset=utf8".format(db))
        return engine;
    def meta(db='hive'):
        engine= create_engine("mysql+pymysql://xzh:Xzh@123456@192.168.190.10:3306/{0}?charset=utf8".format(db))
        return engine;
    def cons_map():
        cons_map=dict()
        for vars in dir(conn):
            if len(vars)>=3 and not(vars.startswith('_')) and vars!='cons_map':
                 try:
                     cons_map[vars]=eval('conn.{0}()'.format(vars))
                 except Exception as e:
                     print ('str(e):\t\t', str(e))
                     print('不是有效数据库链接：',vars)
        return cons_map
    def db_map():
        db_map={"swzn_102":"coo",
            "swdc_102":"dc_data_maintain",
            "sljr":"hrdb",
            "xfqz":"java_xingfuqianzhuang",
            "jrxj":"sljr_jrxj",
            "slsw":"sljr_slsw",
            "bdgl":"sljr_borrow",
            "csxt":"collection",
            "yhcg":"sljr_bank",
            "zhxt":"sljr_payment",
            "lyxt":"sljr_pay_rout",
            "fkxt":"sljr_risk"
            }
        return db_map
    def get_hive_tb(db='sdd'):
        sqls="""
        select  
            t.TBL_NAME tb,
            d.name db
            from  tbls t 
            inner join dbs d on d.db_id=t.db_id 
            where d.name in('sdd','cdi','app') """
        hive=conn.meta('hive')
        hive_tb=pd.read_sql(sqls,hive)
        tp=hive_tb[hive_tb['db']==db]
        return tp
    def hive_tb_exists(tb='sdd'):
        sqls="""
        select  
            t.TBL_NAME tb,
            d.name db
            from  tbls t 
            inner join dbs d on d.db_id=t.db_id 
            where d.name in('sdd','cdi','app') and t.tbl_name='{0}'"""
        hive=conn.meta('hive')
        hive_tb=pd.read_sql(sqls.format(tb),hive)
        if hive_tb.shape[0]>0:
            return 1
        else :
            return 0
    def sljr_tb_size(db='xfqz',tb='user'):#获取mysql数据表的大小
        sqls="""
        select count(1) num from {0}"""
        if db in confs.db_map.keys():
            hive=conn.sljr_mysql(confs.db_map[db])
            try:
                hive_tb=pd.read_sql(sqls.format(tb),hive)
                return hive_tb.iloc[0,0]
            except Exception as e:
                print(tb,'表不存在',str(e))
                return -1
        else:
            print(db,'不存在于confs.db_map中')
            return -1
    def etl_set_exists(tb='sdd'):
        sqls="""
        SELECT tb_name 
            FROM etl_job_set e
             where e.oper_date=CURRENT_DATE()
             and sh_files<>'无配置shell' and tb_name='{0}'"""
        hive=conn.meta('etl_data')
        hive_tb=pd.read_sql(sqls.format(tb),hive)
        if hive_tb.shape[0]>0:
            return 1
        else :
            return 0
        
    def get_hive_dml(db='xfqz',tb_list=['user','real_user']):
        dbmap=conn.db_map()
        tar_path=confs.main_path_py+'hive/ddl/'
        src_db=dbmap[db]
        db=db[0:4]
        is_success=1
        hive_tb=conn.get_hive_tb(db='sdd')
        hive_tb=list(hive_tb['tb'])
        #etl_con=conn.meta('etl_data')
        ddl_file = open(confs.main_path_py+'hive/ddl.sql', 'w+',encoding='utf-8')
        engine=conn.sljr_mysql('information_schema')
        for tb in tb_list:
            if db+'_'+tb in hive_tb:
                print(db+'_'+tb ,'表已经存在')
                is_success=0
            else:
                fsdd="\n\ncreate table if not exists sdd.{0}_{1} (".format(db,tb)+"\nload_data_time string comment '加载时间',\n "
                finc="\n\ncreate table if not exists inc.{0}_{1}_inc (\n".format(db,tb)
                tb_sql="SELECT TABLE_SCHEMA db,table_name tb,TABLE_COMMENT tb_comment FROM `TABLES` t where TABLE_SCHEMA='{0}' and table_name='{1}'"
                tb_com=pd.read_sql(tb_sql.format(src_db,tb),engine)
                if tb_com.shape[0]>0:
                    tb_sql=tb_com.iloc[0,2].replace(';','').replace('\'','')
                    lsdd="\n) comment '{0}' \n partitioned by (dt string) stored as orcfile;".format(tb_sql)
                    linc="\n) comment '{0}' \n row format delimited fields terminated by '\\001'  stored as textfile;".format(tb_sql)
                    col_sql="""select  CONCAT(LOWER(COLUMN_NAME),' string comment \\'',REPLACE(COLUMN_COMMENT,'\\'',''),'\\'') col 
                      from `COLUMNS` c where TABLE_SCHEMA='{0}' and table_name='{1}' ORDER BY c.ORDINAL_POSITION """
                    cols=pd.read_sql(col_sql.format(src_db,tb),engine)
                    col_sql=",\n".join(list(cols['col'])).replace(';',' ')
                    sdd_tb=fsdd+col_sql+lsdd
                    inc_tb=finc+col_sql+linc
                    file_object = open(tar_path+db+'_'+tb+'.sql', 'w+',encoding='utf-8')
                    file_object.write(sdd_tb)
                    file_object.write('\n\n')
                    file_object.write(inc_tb)
                    file_object.close()
                    ddl_file.write(sdd_tb)
                    ddl_file.write(inc_tb)
                    #is_success=1
                else:
                    print(tb,'表不存在')
                    is_success=0

        ddl_file.close()
        return is_success
if __name__ == '__main__':
    mp5=conn.db_map()
    #conns=conn()
    #hive_tb=conns.get_hive_tb()
    #hive_tb=list(hive_tb['tb'])
    pg=conn.sljr_pg()
    print(pd.read_sql('SELECT * FROM dc_stging.sljr_hive_batch_log;',pg))
    
    #s=conn.get_hive_dml(db='fkxt',tb_list=['approve_borrower_info','approve_credit_card','approve_credit_card_detail'])
    
