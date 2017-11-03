import auto_schedule as sc

if __name__=='__main__':
    auto=sc.auto_schedule()
    #auto.auto_deploy(tar_ssh='ssh_sc') # 自动部署
    auto.run_sql(tb='fact_ordr_repmt_plan_dtl_daily',tar_ssh='ssh_sc') #更新sql并执行
    #chmod 755 -R /home/bigdata/bin
    
    