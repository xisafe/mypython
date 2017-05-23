import pymysql
import time
start = time.clock()
conn = pymysql.connect(host='192.168.199.112', port=3306, user='root', passwd='ezbuyisthebest',db='statis',charset='utf8')
cur = conn.cursor()
cur.execute("select c.customer_id,c.catalog_code from cc_customer c")
for r in cur.fetchall():
           print(r)
           #cur.close()
conn.close()
end = time.clock()
print("read: %f s" % (end - start))