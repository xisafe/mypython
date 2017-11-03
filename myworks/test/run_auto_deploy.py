import auto_schedule as sc

if __name__=='__main__':
    auto=sc.auto_schedule()
    auto.auto_deploy(tar_ssh='ssh_sc') # 自动部署
    #auto.del_job('shell_sms_user_log')
    #auto.run_sql(tb='dim_acct_flag_info_daily',tar_ssh='ssh_sc') #更新sql并执行
    #auto.write_sh()
    
    