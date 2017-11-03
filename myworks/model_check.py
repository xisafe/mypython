from pdm import PDMHandler
from cons import conn
import pymysql 
import time
import pandas as pd
import confs
now = time.time()
today=time.strftime('%Y-%m-%d',time.localtime(now))
yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))
def check():
    ph = PDMHandler.parse('E:/sljr/project/3-设计文档/02-模型设计/Model_PDM_DMP 2.2.pdm')
    etl_data=conn.meta('etl_data')
    del_sql="""delete from pdm_meta where check_date=CURRENT_DATE();
                delete from hive_meta where check_date=CURRENT_DATE();""" 
    etl_data.execute(del_sql) #保证每天只有一份数据
    hive_meta_sql="""
            insert into hive_meta(tb_name,tb_name_cn,col_name,col_comment,col_data_type,check_date)
            select  
                 t.TBL_NAME tb_name,
                tp.tb_com tb_name_cn,
                v.COLUMN_NAME col_name, 
                v.`COMMENT` col_comment,
                v.TYPE_NAME  col_data_type,CURRENT_DATE() check_date
            from hive.columns_v2 v 
            inner join hive.sds s on v.CD_ID=s.CD_ID 
            inner join hive.tbls t on s.sd_id=t.sd_id 
            inner join hive.dbs d on d.db_id=t.db_id 
            LEFT JOIN(select s.tbl_id tb_id,
                    max(if(PARAM_KEY='comment',PARAM_VALUE,null)) tb_com,
                    FROM_UNIXTIME(max(if(PARAM_KEY='transient_lastDdlTime',PARAM_VALUE,null))) last_ddl_time,
                    FROM_UNIXTIME(max(if(PARAM_KEY='last_modified_time',PARAM_VALUE,null))) last_modified_time,
                    max(if(PARAM_KEY='last_modified_by',PARAM_VALUE,'')) last_modified_by
            from hive.TABLE_PARAMS s GROUP BY s.TBL_ID) tp on t.TBL_ID=tp.tb_id
            where d.`NAME` in( 'cdi','app') 
             union all
         SELECT    
                t.TBL_NAME tb_name,
                tp.tb_com tb_name_cn,
                p.PKEY_NAME col_name, 
                p.PKEY_COMMENT col_comment,
                p.PKEY_TYPE  col_data_type,CURRENT_DATE() check_date
            FROM hive.partition_keys p
            inner join hive.tbls t on p.tbl_id=t.tbl_id 
            inner join hive.dbs d on d.db_id=t.db_id 
            LEFT JOIN(select s.tbl_id tb_id,
                    max(if(PARAM_KEY='comment',PARAM_VALUE,null)) tb_com,
                    FROM_UNIXTIME(max(if(PARAM_KEY='transient_lastDdlTime',PARAM_VALUE,null))) last_ddl_time,
                    FROM_UNIXTIME(max(if(PARAM_KEY='last_modified_time',PARAM_VALUE,null))) last_modified_time,
                    max(if(PARAM_KEY='last_modified_by',PARAM_VALUE,'')) last_modified_by
            from hive.TABLE_PARAMS s GROUP BY s.TBL_ID) tp on t.TBL_ID=tp.tb_id
            where d.`NAME` in( 'cdi','app')
            """
    etl_data.execute(hive_meta_sql) #hive元数据
    insert_sql="""insert into pdm_meta(tb_name,tb_name_cn,col_name,
                col_name_cn,col_comment,col_data_type,create_time,
                create_by,update_time,update_by,check_date)
                VALUES('{0}','{1}','{2}','{3}','{4}','{5}',
                FROM_UNIXTIME({6}),'{7}',FROM_UNIXTIME({8}),'{9}',CURRENT_DATE())"""
    for pkg in PDMHandler.getPkgNodes(ph):
        #pkg_attrs = PDMHandler.getPkgAttrs(pkg)
        #print("P:", pkg_attrs["Name"],pkg_attrs["Code"],pkg_attrs["Creator"])
        for tbl in PDMHandler.getTblNodesInPkg(pkg) : 
            tbl_attrs = PDMHandler.getTblAttrs(tbl)
            #print(" T:", tbl_attrs["Name"],tbl_attrs["Code"])
            #print("  T-PATH:",PDMHandler.getNodePath(tbl))
            if tbl_attrs["Code"] not in ['etl_batch_log']:
                for col in PDMHandler.getColNodesInTbl(tbl) :
                    col_attrs = PDMHandler.getColAttrs(col)
                    sql=insert_sql.format(tbl_attrs["Code"],tbl_attrs["Name"],col_attrs["Code"], col_attrs["Name"],pymysql.escape_string(col_attrs["Comment"].replace(';','')),col_attrs["DataType"],col_attrs["CreationDate"],col_attrs["Creator"],col_attrs["ModificationDate"],col_attrs["Modifier"])
                    etl_data.execute(sql)
    check_sql_pdm_self="""
            select '新增' dtype,a.tb_name,a.tb_name_cn,a.col_name,a.col_comment,a.col_data_type FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            left join(
            SELECT * FROM pdm_meta where check_date=DATE_ADD(CURRENT_DATE(),INTERVAL -1 day)) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where b.col_name is null
            union all
            select '删除' dtype,b.tb_name,b.tb_name_cn,b.col_name,b.col_comment,b.col_data_type FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            right join(
            SELECT * FROM pdm_meta where check_date=DATE_ADD(CURRENT_DATE(),INTERVAL -1 day)) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where a.col_name is null
            union all
            select '修改表中文名称' dtype,a.tb_name,a.tb_name_cn,a.col_name,a.col_comment,a.col_data_type FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            inner join(
            SELECT * FROM pdm_meta where check_date=DATE_ADD(CURRENT_DATE(),INTERVAL -1 day)) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where a.tb_name<>b.tb_name
            union all
            select '修改表字段备注' dtype,a.tb_name,a.tb_name_cn,a.col_name,a.col_comment,a.col_data_type 
            FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            inner join(
            SELECT * FROM pdm_meta where check_date=DATE_ADD(CURRENT_DATE(),INTERVAL -1 day)) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where a.col_comment<>b.col_comment
            union all
            select '修改表字段类型' dtype,a.tb_name,a.tb_name_cn,a.col_name,a.col_comment,a.col_data_type
             FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            inner join(
            SELECT * FROM pdm_meta where check_date=DATE_ADD(CURRENT_DATE(),INTERVAL -1 day)) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where a.col_data_type<>b.col_data_type
            """
    pdm_hive_col_sql="""
            select '新增字段' dtype,a.tb_name,a.tb_name_cn,a.col_name,a.col_comment,a.col_data_type FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            left join(
            SELECT * FROM hive_meta where check_date= CURRENT_DATE()) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where b.col_name is null
            union all
            select '删除字段' dtype,b.tb_name,b.tb_name_cn,b.col_name,b.col_comment,b.col_data_type FROM
            (SELECT * FROM pdm_meta where check_date=CURRENT_DATE()) a
            right join(
            SELECT * FROM hive_meta where check_date= CURRENT_DATE()) b 
            on a.tb_name=b.tb_name and a.col_name=b.col_name
            where a.col_name is null
            """
    pdm_hive_tb_sql="""
                 SELECT DISTINCT '新增表' dtype, p.tb_name,p.tb_name_cn,'' col_name,'' col_comment,'' col_data_type 
                 FROM pdm_meta p left join hive_meta m on p.tb_name=m.tb_name and p.check_date=m.check_date 
                where m.tb_name is NULL and p.check_date=CURRENT_DATE()
                  union all
							   SELECT DISTINCT '删除表' dtype, m.tb_name,m.tb_name_cn,'' col_name,'' col_comment,'' col_data_type
                FROM pdm_meta p right join hive_meta m on p.tb_name=m.tb_name and p.check_date=m.check_date 
                where p.tb_name is NULL and m.check_date=CURRENT_DATE()"""
    col_diff=pd.read_sql(pdm_hive_col_sql,etl_data)
    tb_diff=pd.read_sql(pdm_hive_tb_sql,etl_data)
    tb_list=list(tb_diff['tb_name'])
    col_diff=col_diff[~col_diff['tb_name'].isin(tb_list)]
    return pd.concat([tb_diff,col_diff])
    #diffs.to_csv('模型差异.csv',index=False,encoding='utf-8')
    
def df_to_html(diffs):
    rows,cols=diffs.shape
    cat_str="<table border='1'><tr bgcolor='#FFBB77'>"
    for i in diffs.columns:
        cat_str=cat_str+'<td>'+i+'</td>'
    cat_str=cat_str+'</tr>'
    for i in range(rows):
        cat_str=cat_str+'<tr>'
        for j in range(cols):
            cat_str=cat_str+'<td>'+str(diffs.iloc[i,j])+'</td>'
        cat_str=cat_str+'</tr>'
    cat_str=cat_str+'</table>'
    return cat_str
if __name__ == '__main__' :
    diffs=check()
    #diffs=pd.read_sql(check_sql,etl_data)
    #diffs.to_csv('模型差异.csv',index=False,encoding='utf-8')
    cat_str=df_to_html(diffs)
    confs.send_mail('pdm和生产库元数据对比'+today,cat_str,content_type='html')