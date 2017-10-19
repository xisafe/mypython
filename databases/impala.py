from impala.dbapi import connect
conn = connect(host='192.168.190.18', port=21050)
cursor = conn.cursor()
cursor.execute('show tables')
print (cursor.description) # prints the result set's schema
results = cursor.fetchall()