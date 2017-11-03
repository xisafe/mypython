# -*- coding=utf-8 -*-
import os
import sys
import datetime
import time
import MySQLdb
import xlwt
ezxf = xlwt.easyxf


# 检查输入日期格式是否正确
def check_date_str(date):
    try:
        d = date.split('-')
        datetime.date(int(d[0]), int(d[1]), int(d[2]))
    except Exception, e:
        print e.message
        print u'输入日期格式异常！'.encode('gbk')
        sys.exit()


# 参数设置
SERVER = '192.168.0.89'
USERNAME = 'developer'
PASSWD = 'YU7niP9C'
DB = 'mca_crm'

# 定义格式
heading_xf = ezxf('font: bold on; align: wrap on, vert centre, horiz center;'
                  'borders: left thin, right thin, top thin, bottom thin;'
                  'pattern: pattern solid,fore_colour sky_blue;')  # 表头格式
kinds = '''int text text text text text text text date int
 money money money money money money int money text percent
 money money text text money text date text text'''.split()  # 字段类型
col_width_px = (
    60,   70, 320, 130,  70, 110, 100, 100, 80, 90,
    100, 100, 100, 100, 100, 120,  70, 100, 70, 70,
    100, 100,  70, 100, 100,  70,  90,  70, 90)  # 每列列宽（像素）
kind_to_xf_map = {
    'date': ezxf('borders: left thin, right thin, top thin, bottom thin;', num_format_str='yyyy-mm-dd'),
    'int': ezxf('borders: left thin, right thin, top thin, bottom thin;', num_format_str='##0'),
    'money': ezxf('borders: left thin, right thin, top thin, bottom thin;', num_format_str='#,##0.00'),
    'percent': ezxf('borders: left thin, right thin, top thin, bottom thin;', num_format_str='0.00%'),
    'text': ezxf('borders: left thin, right thin, top thin, bottom thin;')
}
data_xfs = [kind_to_xf_map[k] for k in kinds]

# 连接数据库
conn = MySQLdb.connect(host=SERVER, user=USERNAME, passwd=PASSWD, db=DB, charset='utf8')

# 获取 cursor 对象进行操作
# cursor = conn.cursor(cursorclass = MySQLdb.cursors.DictCursor) # 使用字典cursor取得结果集
cursor = conn.cursor()

# 查询最大保理日期
sql_query_max = '''
SELECT
    IF(COUNT(1) =
    (
        SELECT
            COUNT(1)
        FROM
            settle_cooperation
        WHERE
            stauts = 1), settleDate, DATE_SUB(settleDate, INTERVAL 1 DAY)) AS 'maxSettleDate'
FROM
    (
        SELECT
            sr.settleDate
        FROM
            settle_receive sr, settle_cooperation sc
        WHERE
            sr.cooperation_id = sc.id
        AND sr.settleDate =
            (
                SELECT
                    MAX(settleDate)
                FROM
                    settle_receive
                WHERE
                    settle_status = 3)
        AND ( ((sr.status = 2 OR sr.status = 3) AND sc.cooperation_type = 1) OR
            (settle_status = 3 AND (sc.cooperation_type = 2 OR sc.cooperation_type = 4)) )) AS t
'''

sql = '''
SELECT
    cashadv_id AS '编号', productName AS '产品名称', registeredName AS '注册名称',
    CONCAT(paymentTerm, '-', paymentMethod) AS '还款方式',
    -- repayType AS '保理类型',
    acquirer AS '收单机构', -- agency AS '合作机构',
    leadsSource AS '线索来源',
    BusinessProvince AS '省份', CONCAT(region, '分公司') AS '银商分支',
    loanDate AS '放款日期', period AS '融资期限', loanAmt AS '实际融资金额',
    IF(stauts = '未完结', loanAmt - IFNULL(totalRepay, 0), 0) AS '余额',
    transferAmt AS '承购账款', totalPosCardAmt AS '累计POS还款',
    IF(stauts = '未完结', 0, IF(transferAmt - totalPosCardAmt < 0, 0, transferAmt - totalPosCardAmt)) AS '释放账款',
    IF(stauts = '未完结', IF (transferAmt - totalPosCardAmt < 0, 0, transferAmt - totalPosCardAmt), 0) AS '未回收承购账款',
    aimAmortization AS '目标还款期（月）', fee AS '手续费', stauts AS '状态',
    ROUND(fee / aimAmortization / loanAmt, 4) AS '手续费率', loanAmt * (0.015 / (12 / aimAmortization)) AS '银商收入',
    fee - loanAmt * (0.015 / (12 / aimAmortization)) AS '总利润', EXTRACT(YEAR_MONTH FROM loanDate) AS '放款月份',
    agentName AS '线下客户经理', offline_performance AS '员工激励',
    IF(prevNumber IS NULL OR prevNumber = '', '否', '是') AS '是否续贷', endDate AS '结束日期',
    IF(overdueDays <= 0, '否', '是') AS '是否逾期', shareOrg AS '公司分润所属机构'
FROM
    (
        SELECT
            sp.title AS 'acquirer', ll.title AS 'leadsSource',
            cv.extendFlag, repayType, period, cv.agency, sp2.title AS 'shareOrg',
            cv.cashadv_id, app.CorporateName AS 'registeredName', cv.loanDate, -- 注册名称，放款日期
            CASE
                WHEN ac.AgentBranch = '好易联' THEN '广州'
                WHEN ac.AgentBranch = '数字王府井' THEN '北京'
                ELSE ac.AgentBranch
            END AS 'region', -- 地区
            cv.prevNumber, -- 上次保理编号
            CAST(cv.loanAmt AS DECIMAL(18, 2)) AS 'loanAmt', CAST(cv.period / 30 AS UNSIGNED) AS 'aimAmortization', -- 预支额，还款月数
            IF(cv.endDate != '' AND cv.endDate <= %s, '已完结', '未完结') AS 'stauts', -- 保理状态
            IF(cv.extendFlag = '0' OR cv.extendFlag = '', CAST(cv.transferAmt AS DECIMAL(18, 2)),
                CAST(acs.originalTransferAmt AS DECIMAL(18, 2))) AS 'transferAmt', -- 承购账款
            IF(cv.repayType = '账户直接扣款', IFNULL(t_dtr.totalRepay, 0), IFNULL(t_ptr.totalRepay, 0)) AS 'totalRepay', -- 累计还款
            app.BusinessProvince, pn.title AS 'productName', ppm.payment_method AS 'paymentMethod', -- 经营省份，产品名称, 还款方式
            ppm.payment_term AS 'paymentTerm', -- 还款频率
            IF(cv.repayType = '账户直接扣款', IFNULL(t_sadp.totalDDPosCardAmt, 0), IFNULL(t_tpa.totalPosCardAmt, 0)) AS 'totalPosCardAmt',  -- 累计刷卡额
            DATEDIFF(IF(cv.status != '关闭', %s, cv.endDate), cv.loanDate) + 1 - ac.PaybackDays AS 'overdueDays', -- 逾期天数
            ui.name AS 'agentName', cv.fee AS 'fee', acm.offline_performance, cv.endDate -- 下线客户经理，手续费，线下激励，结束日期
        FROM
            (
                SELECT
                    cashadv_id,
                    CAST(MAX(IF(NAME = 'PaybackDays', VALUE, '')) AS UNSIGNED) AS 'period', -- 周期
                    MAX(IF(NAME = 'Status', VALUE, '')) AS 'status', -- 地区
                    MAX(IF(NAME = 'AgentBranch', VALUE, '')) AS 'region', -- 代理机构
                    MAX(IF(NAME = 'agency', VALUE, '')) AS 'agency', -- 合作机构
                    MAX(IF(NAME = 'AmountRequested', REPLACE(VALUE, ',', ''), '')) AS 'loanAmt', -- 总共需还款金额
                    MAX(IF(NAME = 'ActualMerCreditDate', VALUE, '')) AS 'loanDate', -- 放款日期
                    MAX(IF(NAME = 'PaybackCompDate', VALUE, '')) AS 'endDate', -- 结束日期
                    CAST(MAX(IF(NAME = 'TotalFactorFee', REPLACE(VALUE, ',', ''), '')) AS DECIMAL(18, 2)) AS 'fee', -- 保理手续费
                    MAX(IF(NAME = 'FundingProduct', VALUE, '')) AS 'repayType', -- 还款类型
                    MAX(IF(NAME = 'TransferReceive', REPLACE(VALUE, ',', ''), '')) AS 'transferAmt', -- 转让账款金额
                    MAX(IF(NAME = 'ExtendFlag', VALUE, '')) AS 'extendFlag', -- 展期标志
                    MAX(IF(NAME = 'main_cashadv_id', REPLACE(VALUE, ',', ''), '')) AS 'prevNumber', -- 上次保理编号
                    MAX(IF(NAME = 'app_agent_id', VALUE, '')) AS 'agentId' -- 线下客户经理
                FROM
                    (
                        SELECT
                            *
                        FROM
                            app_cashadv_values
                        WHERE
                            NAME IN ('PaybackDays', 'AgentBranch', 'AmountRequested', 'PaybackCompDate', 'TotalFactorFee',
                                'FundingProduct', 'agency', 'ActualMerCreditDate', 'TransferReceive', 'main_cashadv_id',
                                'app_agent_id', 'ExtendFlag', 'Status') AND
                            cashadv_id IN
                            ( -- 只统计还款清算中的融资保理
                                SELECT
                                    cashadv_id
                                FROM
                                    app_cashadv_values
                                GROUP BY
                                    cashadv_id
                                HAVING
                                    MAX(IF(NAME = 'Status', VALUE, '')) IN ('还款清算','关闭') AND
                                    MAX(IF(NAME = 'MCAServiceType', VALUE, '')) = '融资保理') ) AS t_qs
                GROUP BY
                    cashadv_id ) cv
        LEFT JOIN
            (
                SELECT
                    cashadv_id, SUM(pos_repaymentAmt) + SUM(other_repaymentAmt) AS 'totalRepay' -- 累计保理还款金额
                FROM
                    settle_daily_summary
                WHERE
                    settleDate <= %s
                GROUP BY
                    cashadv_id ) AS t_ptr
        ON
            t_ptr.cashadv_id = cv.cashadv_id
        LEFT JOIN
            (
                SELECT
                    cvid, SUM(receiveMoney) AS 'totalRepay' -- 累计还款（直扣）
                FROM
                    settle_deduct
                WHERE
                    result = '成功'
                GROUP BY
                    cvid) t_dtr
        ON
            cv.cashadv_id = t_dtr.cvid
        LEFT JOIN
            (
                SELECT
                    cashadv_id, SUM(transAmt) AS 'totalPosCardAmt' -- 累计刷卡金额
                FROM
                    settle_daily_summary, (
                        SELECT
                            cashadv_id AS 'cv_id',
                            MAX(IF(NAME = 'FundingProduct', RIGHT(VALUE, 4), '')) AS 'repayType', -- 还款方式
                            MAX(IF(NAME = 'ActualMerCreditDate', VALUE, '')) AS 'loanDate', -- 放款日期
                            MAX(IF(NAME = 'PaybackCompDate', VALUE, '')) AS 'endDate' -- 结束日期
                        FROM
                            (
                                SELECT
                                    *
                                FROM
                                    app_cashadv_values
                                WHERE
                                    NAME IN ('ActualMerCreditDate', 'FundingProduct', 'PaybackCompDate') ) t_acv
                        GROUP BY
                            cashadv_id
                        HAVING
                            repayType != '' AND
                            loanDate != '') t_ald
                WHERE
                    cashadv_id = cv_id AND
                    repayType != '账户直接扣款' AND
                    payableDate IS NOT NULL AND
                    payableDate >= loanDate AND -- 新增条件
                    payableDate <= IF(endDate IS NULL OR endDate = '', %s, endDate)
                GROUP BY
                    cashadv_id ) t_tpa
        ON
            cv.cashadv_id = t_tpa.cashadv_id
        LEFT JOIN
            (
                SELECT
                    cashadv_id, SUM(tran_amt) AS 'totalDDPosCardAmt'
                FROM
                    settle_account_deduct_pos
                WHERE
                    tran_date <= %s
                GROUP BY
                    cashadv_id ) t_sadp
        ON
           cv.cashadv_id = t_sadp.cashadv_id
        LEFT JOIN
            (
                SELECT
                    cashadv_id,
                    MAX(IF(NAME = 'TransferReceive', REPLACE(VALUE, ',', ''), '0')) AS 'originalTransferAmt' -- 第一次展期前承购账款
                FROM
                    app_cashadv_snapshot
                GROUP BY
                    cashadv_id, creationTime
                HAVING
                    MAX(IF(NAME = 'ExtendFlag', VALUE, '0')) = '0' AND
                    MAX(IF(NAME = 'snapshot_note', VALUE, '0')) = '展期') AS acs
        ON
            acs.cashadv_id = cv.cashadv_id
        LEFT JOIN
            (
                SELECT
                    cashadv_id, SUM(VALUE) AS 'offline_performance'
                FROM
                    app_cashadv_merit
                WHERE
                    NAME = 'agent_comm'
                GROUP BY
                    cashadv_id) AS acm
        ON
            cv.cashadv_id = acm.cashadv_id
        LEFT JOIN
            app_cashadv ac
        ON
            cv.cashadv_id = ac.cashadv_id
        LEFT JOIN
            app
        ON
            app.app_id = ac.app_id
        LEFT JOIN
            leads
        ON
            leads.lead_id = ac.lead_id_converted
        LEFT JOIN
            lead_lists ll
        ON
            ll.id = leads.list_id
        LEFT JOIN
            prod_name pn
        ON
            ac.prodName = pn.id
        LEFT JOIN
            prod_payment_method ppm
        ON
            ac.paymentMethod = ppm.id
        LEFT JOIN
            user_info ui
        ON
            cv.agentId = ui.uid
        LEFT JOIN
            sys_processors sp
        ON
            ac.processor_id = sp.processor_id
        LEFT JOIN
            sys_processors sp2
        ON
            ac.processorComm = sp2.processor_id
        WHERE
            loanDate <= %s
        ORDER BY
            cv.cashadv_id ) AS tmp
'''

# 查询最大保理日期
cursor.execute(sql_query_max)
max_bl_date = cursor.fetchone()[0]

# 日期输入并检查
print u'目前最大保理截止日期为：%s'.encode('gbk') % max_bl_date
end_date = raw_input(u'请输入截止日期(YYYY-MM-DD):'.encode('gbk'))
if end_date == "":
    end_date = max_bl_date.strftime('%Y-%m-%d')
else:
    check_date_str(end_date)

# 如果截止日期大于最大保理日期
if max_bl_date < datetime.datetime.strptime(end_date, '%Y-%m-%d').date():
    endDate = max_bl_date.strftime('%Y-%m-%d')

# 获取查询结果
cursor.execute(sql, (end_date, end_date, end_date, end_date, end_date, end_date))
data = cursor.fetchall()

# 当前路径
path = os.getcwd().decode('gbk').encode('utf-8')
des_file = u'余额_%s.xls' % end_date

# 创建工作簿及表单
book = xlwt.Workbook('utf-8')
sheet = book.add_sheet(u'余额')

# 表字段名信息写入
header = cursor.description
rowx = 0
for colx, value in enumerate(header):
    sheet.write(rowx, colx, value[0], heading_xf)
    sheet.col(colx).width = int(col_width_px[colx] * 36.568)  # 设置列宽 1px = 36.568

# 数据写入
if data:
    for rec in data:
        rowx += 1
        for colx, value in enumerate(rec):
            sheet.write(rowx, colx, value, data_xfs[colx])

# 写入最终文件
book.save(os.path.join(path, des_file))

# 关闭资源
cursor.close()
conn.close()
