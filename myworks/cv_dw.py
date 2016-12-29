# encoding: utf-8
import pymssql
class MSSQL:
    """
    对pymssql的简单封装
    pymssql库，该库到这里下载：http://www.lfd.uci.edu/~gohlke/pythonlibs/#pymssql
    使用该库时，需要在Sql Server Configuration Manager里面将TCP/IP协议开启
    用法：
    """
    def __init__(self):
        self.host ="192.168.0.98\SQLSERVERDB"
        self.user = "sa"
        self.pwd = "!@#qweasdZXC"
        self.db = "cv_dw"
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
    def GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise(NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise(NameError, "连接数据库失败")
        else:
            return cur

    def ExecQuery(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):

       # 执行非查询语句

        cur = self.GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def getworkdays(self, date1, date2):   # 两个日期之间的工作日
         sq = "SELECT sum(iswork)-1 days FROM t_dim_day where DDate BETWEEN '%s' and '%s'" % (date1, date2)
         days = self.ExecQuery(sq)
         return days[0][0]

def main():
# #  ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
# # #返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段
#  # ms.ExecNonQuery("insert into WeiBoUser values('2','3')")

    ms=MSSQL()
    resList = ms.ExecQuery("SELECT lead_id,BusinessName FROM Dim_leads")
    for (id,weibocontent) in resList:
        print weibocontent # str(weibocontent).decode("utf8")

if __name__ == '__main__':
    print "%s111111111"%__name__
    main()