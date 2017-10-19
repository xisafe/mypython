from hive_service import ThriftHive
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
def ReadHiveTest(sql):
    try:
        tSocket = TSocket.TSocket('192.168.190.14',10000)
        tTransport = TTransport.TBufferedTransport(tSocket)
        protocol = TBinaryProtocol.TBinaryProtocol(tTransport)
        client = ThriftHive.Client(protocol)
        tTransport.open()
        client.execute(sql)
        return client.fetchAll()
    except Exception as e:
        print(str(e))
    finally:
        tTransport.close()
if __name__ == '__main__':
    showDatabasesSql = 'show databases'
    showTablesSql = 'show tables'
    selectSql = 'SELECT * FROM 07_jn_mysql_2'
    result = ReadHiveTest(selectSql)
    print(result[1])