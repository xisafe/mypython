from openpyxl import Workbook  
import pandas as pd
from etlpy.cons import conn as cons
def getHiveTb():
    engine=cons.meta('hive')
    sql_txt="""
            select  
                t.TBL_ID tb_id,
                d.name db, 
                t.TBL_NAME tb,
                v.COLUMN_NAME col, 
                v.TYPE_NAME ctype,
                v.`COMMENT` col_com
            from columns_v2 v 
            inner join sds s on v.CD_ID=s.CD_ID 
            inner join tbls t on s.sd_id=t.sd_id inner join dbs d on d.db_id=t.db_id 
            where d.`NAME` in('cdi','app') order by t.TBL_ID,v.INTEGER_IDX;
            """
    cols=pd.read_sql(sql_txt,engine)
    sql_txt="""
           select s.tbl_id tb_id,
                   max(if(PARAM_KEY='comment',PARAM_VALUE,null)) tb_com,
                   max(if(PARAM_KEY='numRows',PARAM_VALUE,'')) row_num,
                   max(if(PARAM_KEY='rawDataSize',PARAM_VALUE,'')) raw_data_size,
                   max(if(PARAM_KEY='totalSize',PARAM_VALUE,'')) total_size,
                   FROM_UNIXTIME(max(if(PARAM_KEY='transient_lastDdlTime',PARAM_VALUE,''))) last_ddl_time,
                   FROM_UNIXTIME(max(if(PARAM_KEY='last_modified_time',PARAM_VALUE,''))) last_modified_time,
                   max(if(PARAM_KEY='last_modified_by',PARAM_VALUE,null)) last_modified_by
            from TABLE_PARAMS s GROUP BY s.TBL_ID
            """
    tbs=pd.read_sql(sql_txt,engine)
    tp=cols[['tb_id','tb','db']].drop_duplicates()
    tbs=tbs.merge(tp,how='inner',left_on='tb_id',right_on='tb_id')
    return cols,tbs
def excels(tbs,cols):
    book = Workbook()
    sheet_index = book.create_sheet('目录')
    sheet_index['A1']='表名'
    sheet_index['B1']='表注释'
    sheet_index['C1']='备注'
    sheet_index['D1']='修改'
    for j in range(5):
            sheet_index.col(j).width=256*25
    for i in range(tbs.shape[0]):
        tb_id=tbs.loc[i,'tb_id']
        #db=tbs.loc[i,'db']
        tb=tbs.loc[i,'tb']
        tb_com=tbs.loc[i,'tb_com']
        sheet_name=tb
        print(tb_id,sheet_name)
        sheet = book.add_sheet(sheet_name)
        for j in range(6):
            sheet.col(j).width=256*30
        sheet.write(0,0,xlwt.Formula('HYPERLINK("#目录!A1","返回目录")'),sty_link)
        #sheet.write(1,1,'表名：',sty_t1)
        sheet.write(1,1,sheet_name,sty_t1)
        sheet.write(1,2,tb_com,sty_t1)
        sheet.write(2,0,'字段名',sty_t2)
        sheet.write(2,1,'注释',sty_t2)
        sheet.write(2,2,'字段类型',sty_t2)
        sheet.write(2,3,'源表',sty_t2)
        sheet.write(2,4,'源表字段',sty_t2)
        sheet.write(2,5,'备注',sty_t2)
        tp=cols[cols['tb_id']==tb_id][['col','col_com','ctype']].copy()
        for j in range(tp.shape[0]):
            sheet.write(j+3,0,tp.iloc[j,0],sty_t3)
            sheet.write(j+3,1,tp.iloc[j,1],sty_t3)
            sheet.write(j+3,2,tp.iloc[j,2],sty_t3)
            sheet.write(j+3,3,'',sty_t3)
            sheet.write(j+3,4,'',sty_t3)
            sheet.write(j+3,5,'',sty_t3)
        link = 'HYPERLINK("#%s!A1";"%s")' % (sheet_name, sheet_name)
        sheet_index.write(i+1, 0, xlwt.Formula(link),sty_t3)
        link = 'HYPERLINK("#%s!A1";"%s")' % (sheet_name, tb_com)
        sheet_index.write(i+1, 1, xlwt.Formula(link),sty_link)
        #line+=1
    book.save('d:/simple24.xls')
    return tp
if __name__=='__main__':
    cols,tbs=getHiveTb()
    tp=excels(tbs,cols)
    tbs[tbs['tb']=='cdi_fact_ordr_invt_dtl']
    