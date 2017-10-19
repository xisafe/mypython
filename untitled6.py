from impala.dbapi import connect
#需要注意的是这里的auth_mechanism必须有，但database不必须
conn = connect(host='192.168.190.14', port=10000, database='default', auth_mechanism='PLAIN')
cur = conn.cursor()

cur.execute('SHOW DATABASES')
print(cur.fetchall())

cur.execute('SHOW Tables')
print(cur.fetchall())