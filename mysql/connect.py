import pymysql
import time
start = time.clock()
conn = pymysql.connect(host='192.168.0.16', port=3306, user='shujufenxi', passwd='H712EaJfLA',db='mca_crm',charset='utf8')
cur = conn.cursor()
cur.execute("SELECT lead_id,creationtime,department FROM leads")
for r in cur.fetchall():
           print(r)
           #cur.close()
conn.close()
end = time.clock()
print "read: %f s" % (end - start)