# -*- encoding: utf8 -*-
#from __future__ import division
import sys
import pymysql

# 数据库配置参数
host = '192.168.0.20'
user = 'lepus_user'
password = 'lepus_user'
db = 'mca_crm'

#----------------------------------------------------------------------
def getConn(host, user, passwd, db='mysql', port=3306, charset=''):
  try:
    conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset)
    return conn
  except pymysql.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(1)
    
#----------------------------------------------------------------------
def closeConn(conn):
  """关闭 mysql connection"""
  conn.close()
  
#----------------------------------------------------------------------
def getValue(conn, query):
  """ 查询相关参数 返回一个值 """
  cursor = conn.cursor()
  getNum=cursor.execute(query)
  if getNum>0:
      result = cursor.fetchone()
  else:
      result=['0']
  return int(result[1])

def getQuery(conn, query):
  """ 查询相关参数 返回多个值 """
  cursor = conn.cursor()
  cursor.execute(query)
  result = cursor.fetchall()
  return result

#执行查询的总次数
Questions = "show global status like 'Questions'"
#服务器已经运行的时间（以秒为单位）
Uptime = "show global status like 'Uptime'"

Com_commit = "show global status like 'Com_commit'"
Com_rollback = "show global status like 'Com_rollback'"
#从硬盘读取键的数据块的次数。如果Key_reads较大，则Key_buffer_size值可能太小。
#可以用Key_reads/Key_read_requests计算缓存损失率
Key_reads = "show global status like 'Key_reads'"
#从缓存读键的数据块的请求数
Key_read_requests = "show global status like 'Key_read_requests'"
#向硬盘写入将键的数据块的物理写操作的次数
Key_writes = "show global status like 'Key_writes'"
#将键的数据块写入缓存的请求数 
Key_write_requests = "show global status like 'Key_write_requests'"
#是否有innodb引擎，5.5版本后没有了该参数。所以有特殊处理
Have_innodb = "show global variables like 'have_innodb'"
#不能满足InnoDB必须单页读取的缓冲池中的逻辑读数量。
Innodb_buffer_pool_reads = "show global status like 'Innodb_buffer_pool_reads'"
#InnoDB已经完成的逻辑读请求数
Innodb_buffer_pool_read_requests = "show global status like 'Innodb_buffer_pool_read_requests'"
#查询缓存被访问的次数
Qcache_hits = "show global status like 'Qcache_hits'"
#加入到缓存的查询数量，缓存没有用到
Qcache_inserts = "show global status like 'Qcache_inserts'"
#当前打开的表的数量
Open_tables = "show global status like 'Open_tables'"
#已经打开的表的数量。如果Opened_tables较大，table_cache 值可能太小
Opened_tables = "show global status like 'Opened_tables'"
#创建用来处理连接的线程数。如果Threads_created较大，你可能要
#增加thread_cache_size值。缓存访问率的计算方法Threads_created/Connections
Threads_created = "show global status like 'Threads_created'"
#试图连接到(不管是否成功)MySQL服务器的连接数。缓存访问率的计算方法Threads_created/Connections
Connections = "show global status like 'Connections'"
#Com_select/s：平均每秒select语句执行次数
#Com_insert/s：平均每秒insert语句执行次数
#Com_update/s：平均每秒update语句执行次数
#Com_delete/s：平均每秒delete语句执行次数
Com_select = "show global status like 'Com_select'"
Com_insert = "show global status like 'Com_insert'"
Com_update = "show global status like 'Com_update'"
Com_delete = "show global status like 'Com_delete'"
Com_replace = "show global status like 'Com_replace'"
#不能立即获得的表的锁的次数。如果该值较高，并且有性能问题，你应首先优化查询，然后拆分表或使用复制。
Table_locks_waited = "show global status like 'Table_locks_waited'"
#立即获得的表的锁的次数
Table_locks_immediate = "show global status like 'Table_locks_immediate'"
#服务器执行语句时自动创建的内存中的临时表的数量。如果Created_tmp_disk_tables较大，
#你可能要增加tmp_table_size值使临时 表基于内存而不基于硬盘
Created_tmp_tables = "show global status like 'Created_tmp_tables'"
#服务器执行语句时在硬盘上自动创建的临时表的数量
Created_tmp_disk_tables = "show global status like 'Created_tmp_disk_tables'"
#查询时间超过long_query_time秒的查询的个数 缓慢查询个数
Slow_queries = "show global status like 'Slow_queries'"
#没有主键（key）联合（Join）的执行。该值可能是零。这是捕获开发错误的好方法，因为一些这样的查询可能降低系统的性能。
Select_full_join = "show global status like 'Select_full_join'"

if __name__ == "__main__":
  conn = getConn(host, user, password, db)
  
  Questions = getValue(conn, Questions)
  Uptime = getValue(conn, Uptime)
  Com_commit = getValue(conn, Com_commit)
  Com_rollback = getValue(conn, Com_rollback)
  Key_reads = getValue(conn, Key_reads)
  Key_read_requests = getValue(conn, Key_read_requests)
  Key_writes = getValue(conn, Key_writes)
  Key_write_requests = getValue(conn, Key_write_requests)
  Qcache_hits = getValue(conn, Qcache_hits)
  Qcache_inserts = getValue(conn, Qcache_inserts)
  Open_tables = getValue(conn, Open_tables)
  Opened_tables = getValue(conn, Opened_tables)
  Threads_created = getValue(conn, Threads_created)
  Connections = getValue(conn, Connections)
  Com_select = getValue(conn, Com_select)
  Com_insert = getValue(conn, Com_insert)
  Com_update = getValue(conn, Com_update)
  Com_delete = getValue(conn, Com_delete)
  Com_replace = getValue(conn, Com_replace)
  Table_locks_immediate = getValue(conn, Table_locks_immediate)
  Table_locks_waited = getValue(conn, Table_locks_waited)
  Created_tmp_tables = getValue(conn, Created_tmp_tables)
  Created_tmp_disk_tables = getValue(conn, Created_tmp_disk_tables)
  Slow_queries = getValue(conn, Slow_queries)
  Select_full_join = getValue(conn, Select_full_join)
  
  print u"_____一般信息统计___________________"
  # QPS = Questions / Seconds
  QPS = str(round(Questions / Uptime, 5))
  print u"QPS (每秒查询次数): " + QPS
  TPS = str(round((Com_commit + Com_rollback)/Uptime, 5))
  print u"TPS(每秒执行的事务数量): " + TPS
  
  # Read/Writes Ratio
  rwr = str(round((Com_select + Qcache_hits)*1.0 / (Com_insert + Com_update + Com_delete + Com_replace) * 100, 5)) + "%"
  print u"Read/Writes占比: " + rwr + "\n"
  
  print u"_____缓存使用情况统计___________________"
  Key_buffer_read_hits = str(round((1 - Key_reads/Key_read_requests) * 100, 5)) + "%"
  Key_buffer_write_hits = str(round((1 - Key_writes/Key_write_requests) * 100, 5)) + "%"
  print u"MyISAM缓存读命中率(99.3% - 99.9%较好): " + str(Key_buffer_read_hits)
  print u"MyISAM缓存写命中率(99.3% - 99.9%较好): " + str(Key_buffer_write_hits) 
  if Qcache_hits>0:
      Query_cache_hits = str(round(((Qcache_hits/(Qcache_hits + Qcache_inserts)) * 100), 5)) + "%"
  else:
       Query_cache_hits='0.0%'
  print u"Query Cache 命中率: " + Query_cache_hits 
  cursor = conn.cursor()
  getFlag=cursor.execute(Have_innodb)
  if getFlag>0:
      result = cursor.fetchone()
      Have_innodb = result[1]
  else:
      Have_innodb ="YES"
  if (Have_innodb == "YES"):
    Innodb_buffer_pool_reads = getValue(conn, Innodb_buffer_pool_reads)
    Innodb_buffer_pool_read_requests = getValue(conn, Innodb_buffer_pool_read_requests)
    # Innodb_buffer_read_hits = (1 - Innodb_buffer_pool_reads / Innodb_buffer_pool_read_requests) * 100%
    Innodb_buffer_read_hits = str(round((1 - Innodb_buffer_pool_reads/Innodb_buffer_pool_read_requests) * 100, 5)) + "%"
    print u"Innodb缓存命中率(建议96% - 99%): " + Innodb_buffer_read_hits  
  
  Thread_cache_hits = str(round(((1 - Threads_created / Connections)) * 100, 5)) + "%"
  print u"线程缓存命中率=(应该90%以上): " + Thread_cache_hits + "\n"
  print u"_____慢查询sql监控________________"
  Slow_queries_per_second = str(round(Slow_queries*1.0 / (Uptime/60), 5))
  print "每分钟慢查询次数: " + Slow_queries_per_second
  Select_full_join_per_second = str(round(Select_full_join*1.0 / (Uptime/60), 5))
  print "每分钟无索引join操作: " + Select_full_join_per_second
  full_select_in_all_select = str(round((Select_full_join*1.0 / Com_select) * 100, 5)) + "%"
  print "无索引join操作占比: " + full_select_in_all_select 
  lock_contention = str(round((Table_locks_waited*1.00 / Table_locks_immediate) * 100, 5)) + "%"
  print "MyISAM加锁等待比率(<1% good, 1% warning, >3% bad): " + lock_contention
  print "当期已打开表数: " + str(Open_tables)
  print "累计打开表数: " + str(Opened_tables) 
  Temp_tables_to_disk = str(round((Created_tmp_disk_tables*1.0 / Created_tmp_tables) * 100, 5)) + "%"
  print u"临时表转化磁盘的比率:(最好不要超过10%) " + Temp_tables_to_disk
  closeConn(conn)
