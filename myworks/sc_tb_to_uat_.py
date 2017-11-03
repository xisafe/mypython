from cons import conn as conn
import pandas as pd
import confs
from ssh import SSH_cmd as ssh_cmd
from ssh import SSH as ssh_con
#now = time.time()
#today=time.strftime('%Y-%m-%d',time.localtime(now))
#yesterday=time.strftime('%Y-%m-%d',time.localtime(now - 24*60*60))       
def get_sc_hive_dml():
        etl_data=conn.meta()
        tbs_sql="""
          select -- d.`NAME` db_name,
                concat( d.`NAME`,'.', t.TBL_NAME) tb_name,
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
        """
        part_sql="""        SELECT       
                concat(d.name,'.',t.TBL_NAME) tb_name,
                p.PKEY_NAME col_name, 
                p.PKEY_COMMENT col_comment,
                p.PKEY_TYPE  col_data_type
            FROM hive.partition_keys p
            inner join hive.tbls t on p.tbl_id=t.tbl_id 
            inner join hive.dbs d on d.db_id=t.db_id 
            where d.`NAME` in( 'cdi','app') """
        sc=pd.read_sql(tbs_sql,etl_data)
        parts=pd.read_sql(part_sql,etl_data)
        ddl_file = open(confs.main_path_py+'hive/sc_hive_tbs.sql', 'w+',encoding='utf-8')
        tb_list=sc[['tb_name','tb_name_cn']].drop_duplicates()
        tb_list=tb_list.set_index('tb_name').to_dict()['tb_name_cn']
        for tb in tb_list.keys():
            ddls="\ndrop table if exists {0};\ncreate table if not exists {0} (".format(tb)
            tb_com=sc[sc['tb_name']==tb]
            if tb_com.shape[0]>0:
                for i in tb_com.index:
                    tb_sql=tb_com.loc[i,'col_name'].ljust(30)+tb_com.loc[i,'col_data_type']+' COMMENT \''+tb_com.loc[i,'col_comment'].replace(';','').replace('\'','')+'\','#
                    ddls=ddls+'\n'+tb_sql
            ddls=ddls[:-1]+")\n comment '{0}'".format(tb_list[tb])
            tp_parts=parts[parts['tb_name']==tb]
            if tp_parts.shape[0]>0:
                #print('dsssss',tp_parts)
                p_str="\npartitioned by (" 
                for kp in tp_parts.index:
                    tb_sql=tp_parts.loc[kp,'col_name'].ljust(10)+tp_parts.loc[kp,'col_data_type']+' COMMENT \''+str(tp_parts.loc[kp,'col_comment'])+'\','#
                    p_str=p_str+'\n'+tb_sql
                p_str=(p_str[:-1])+')'
                ddls=ddls+p_str
            ddls=ddls+'\n STORED AS ORCfile;'
            ddl_file.write(ddls)
            ddl_file.write('\n\n')
            #print(ddls)
        ddl_file.close()
        sshcon=ssh_con()
        ssh=ssh_cmd(sshcon.ssh_uat)
        ssh.upload(confs.main_path_py+'hive/sc_hive_tbs.sql',confs.remote_path_py+'hive/sc_hive_tbs.sql')
        ssh.cmd_run(["hive -f '{0}'".format(confs.remote_path_py+'hive/sc_hive_tbs.sql')])
        ssh.close()
        return 1
if __name__ == '__main__':
    m=get_sc_hive_dml() #同步表结构