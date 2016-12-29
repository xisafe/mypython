
try:
    import pyodbc
except:
    pyodbc = None
from base import ComponentBase
from dbapi import SQLConnectionImpl

class ODBCConnection(SQLConnectionImpl, ComponentBase):
    def __init__(self, dsn, uid=None, pwd=None):
        ComponentBase.__init__(self)
        self.dsn = dsn
        self.uid = uid
        self.pwd = pwd

    def openImpl(self):
        cs = 'DSN=%s'%self.dsn
        if self.uid != None:
          cs += ';UID=%s'%self.uid
        if self.pwd != None:
          cs += ';PWD=%s'%self.pwd
        self.setConnection(pyodbc.connect(cs))

    def closeImpl(self):
        self.getConnection().commit()
        self.getConnection().close()