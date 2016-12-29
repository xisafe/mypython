
import sqlite3
from base import ComponentBase
from dbapi import SQLConnectionImpl

class SQLiteConnection(SQLConnectionImpl, ComponentBase):
    def __init__(self, name):
        ComponentBase.__init__(self)
        self.name = name

    def openImpl(self):
        self.setConnection(sqlite3.connect(self.name, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES))
        self.getConnection().text_factory=lambda x: x

    def closeImpl(self):
        self.getConnection().commit()
        self.getConnection().close()