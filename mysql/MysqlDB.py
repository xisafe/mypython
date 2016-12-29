#encoding:utf-8
import MySQLdb
import time
start = time.clock()
try:
    conn=MySQLdb.connect(host='192.168.0.16', port=3306, user='shujufenxi', passwd='H712EaJfLA',db='mca_crm',charset='utf8')
    cur=conn.cursor()
    lines=cur.execute('SELECT lead_id,creationtime,department FROM leads')
    print lines
    info = cur.fetchmany(lines)
    for ii in info:
        print ii
    cur.close()
    conn.close()
except MySQLdb.Error,e:
     print "Mysql Error %d: %s" % (e.args[0], e.args[1])
end = time.clock()
print "read: %f s" % (end - start)