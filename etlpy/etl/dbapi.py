
from base import Header, Field, RowSource, RowTarget, FieldsCopier

class ISQLConnection:
    def getConnection(self):
        raise NotImplementedError()

    def makeInsert(self, table, fields):
        raise NotImplementedError()

    def makeSelect(self, table, keys, orderby=None):
        raise NotImplementedError()

class SQLConnectionImpl(ISQLConnection):
    def getConnection(self):
        return self.conn

    def setConnection(self, conn):
        self.conn = conn

    def makeHeader(self, curs):
        header = Header()
        for name, type_code, display_size, internal_size, precision, scale, null_ok in curs.description:
            header.append(self.cleateField(name, type_code, display_size, internal_size, precision, scale, null_ok))
        return header

    def cleateField(self, name, type_code, display_size, internal_size, precision, scale, null_ok):
        if type_code == None:
            type_code = str
        return Field(name, type_code, internal_size, precision)

    def makeSelect(self, table, cols=None, keys=None, orderby=None):
        sql_list = ["select "]
        if not cols:
            sql_list.append("*")
        else:
            sql_list.extend(", ".join(cols))
        sql_list.extend([" from ", table])
        sql_list.extend(self.makeWhere(keys))
        if orderby:
            sql_list.extend([" order by ", ", ".join(orderby)])
        return "".join(sql_list)

    def makeUpdate(self, table, keys, fields):
        sql_list = ["update ", table, " set "]
        is_first = True
        for field in fields:
            if not field in keys:
                if is_first:
                    is_first = False
                else:
                    sql_list.append(", ")
                sql_list.extend([field, "=?"])
        sql_list.extend(self.makeWhere(keys))
        return "".join(sql_list)

    def makeDelete(self, table, keys):
        sql_list = ["delete from ", table]
        sql_list.extend(self.makeWhere(keys))
        return "".join(sql_list)

    def makeInsert(self, table, fields):
        sql_list = ["insert into ", table, "("]
        field_list = []
        value_list = []
        is_first = True
        for field in fields:
            if is_first:
                is_first = False
            else:
                field_list.append(", ")
                value_list.append(", ")
            field_list.append(field)
            value_list.extend(["?"])
        sql_list.extend(field_list)
        sql_list.append(") values(")
        sql_list.extend(value_list)
        sql_list.append(")")
        return "".join(sql_list)

    def makeWhere(self, keys):
        sql_list = []
        if keys:
            sql_list.append(" where ")
            for i, key in enumerate(keys):
                if i > 0:
                    sql_list.append(" and ")
                sql_list.extend([key, "=?"])
        return sql_list

class DBAPISource(RowSource):
    def __init__(self, conn, query, params=None, header=None):
        RowSource.__init__(self, header)
        self.conn = conn
        self.query = query
        self.params = params or {}

    def openImpl(self):
        self.conn.open()
        self.curs = self.conn.getConnection().cursor()
        self.curs.execute(self.query, **self.params)
        self.description = self.curs.description
        self.current = None
        if self.getHeader() == None:
            header = self.conn.makeHeader(self.curs)
            self.current = self.curs.fetchone()
            self.buffered = True
            for i, field in enumerate(self.current or []):
                if self.current[i] != None:
                    header[i].type = type(self.current[i])
            self.setHeader(header)

    def closeImpl(self):
        self.curs.close()
        self.conn.close()

    def nextImpl(self):
        if self.buffered:
            row = self.current
            self.buffered = False
            self.current = None
        else:
            row = self.curs.fetchone()
        if row == None:
            raise StopIteration()
        return row

class TableSource(DBAPISource):
    def __init__(self, conn, name, header=None):
        cols = header and header.listNames() or None
        DBAPISource.__init__(self, conn, conn.makeSelect(name, cols), params=None, header=None)

class DBAPITarget(RowTarget):
    def __init__(self, conn, name, header=None, target_type="I", keys=None):
        RowTarget.__init__(self, header)
        self.conn = conn
        self.name = name
        self.target_type = target_type
        self.keys = keys

    def openImpl(self):
        self.conn.open()
        self.curs = self.conn.getConnection().cursor()
        names = self.getHeader().listNames()
        keys = self.keys or names
        if self.target_type == "I":
            self.query = self.conn.makeInsert(self.name, names)
            self.qHeader = Header(names=names)
        elif self.target_type == "U":
            self.query = self.conn.makeUpdate(self.name, keys, names)
            self.qHeader = Header(names=list(set(names) - set(keys)) + keys)
        elif self.target_type == "D":
            self.query = self.conn.makeDelete(self.name, keys)
            self.qHeader = Header(names=keys)
        self.copier = FieldsCopier()
        self.copier.makeHeaderPaths(self.getSource().getHeader(), self.qHeader)

    def closeImpl(self):
        self.curs.close()
        self.conn.close()

    def writerowImpl(self, row):
        params = self.qHeader.newBuffer()
        self.copier.copyFields(row, params)
        self.curs.execute(self.query, params)

    def flushImpl(self):
        self.conn.getConnection().commit()

class TableTarget(DBAPITarget):
    def __init__(self, conn, name, header=None, target_type="I", keys=None):
        DBAPITarget.__init__(self, conn, name, header=header, target_type=target_type, keys=keys)

