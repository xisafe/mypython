
from base import EventDispatcher, MemoryTable, ComponentBase, FieldsCopier, StructuredBase, \
Pump, MemoryTableTarget, MemoryTableSource, RowSourceProxy, Field

class ILookup:
    def lookupRows(self, keys):
        raise NotImplemented()

class LookupBase(ILookup):
    def __init__(self, keys):
        self.onBeforeLookup = EventDispatcher(self, "BeforeLookup")
        self.onAfterLookup = EventDispatcher(self, "AfterLookup")
        self.keys = keys
            
    def getKeys(self):
        return self.keys

    def lookupRows(self, keys):
        self.beforeLookupEvent()
        rows = self.lookupRowsImpl(keys)
        self.afterLookupEvent()
        return rows

    def lookupRowsImpl(self, keys):
        return []

    def beforeLookupEvent(self):
        self.onBeforeLookup.fire()

    def afterLookupEvent(self):
        self.onAfterLookup.fire()

class Lookup(LookupBase, StructuredBase, ComponentBase):
    def __init__(self, keys, header=None):
        LookupBase.__init__(self, keys)
        StructuredBase.__init__(self, header)
        ComponentBase.__init__(self)

    def getKeysHeader(self):
        return self.getHeader().cloneByNames(self.getKeys())

class LookupProxy(Lookup):
    def __init__(self, nestedLookup):
        Lookup.__init__(self, nestedLookup.getKeys())
        self.nestedLookup = nestedLookup

    def getNestedLookup(self):
        return self.nestedLookup

    def setNestedLookup(self, nestedLookup):
        self.nestedLookup = nestedLookup

    def lookupRowsImpl(self, keys):
        return self.getNestedLookup().lookupRows()

    def openImpl(self):
        return self.getNestedLookup().open()

    def getHeaderImpl(self):
        return self.getNestedLookup().getHeader()

    def setHeaderImpl(self, header):
        return self.getNestedLookup().setHeader(header)

    def closeImpl(self):
        return self.getNestedLookup().close()

class BufferedLookup(Lookup):
    def __init__(self, keys, source):
        Lookup.__init__(self, keys)
        self.source = source

    def makeTable(self):
        table = MemoryTable()
        Pump(self.source, MemoryTableTarget(table))()
        return table

    def makeCopier(self):        
        self.keysHeader = self.getHeader().cloneByNames(self.keys)
        self.copier = FieldsCopier()
        self.copier.makeHeaderPaths(self.getHeader(), self.keysHeader)

    def makeKey(self, row):
        buf = self.keysHeader.newBuffer()
        self.copier.copyFields(row, buf)
        return tuple(buf)

    def openImpl(self):
        table = self.makeTable()
        self.setHeader(table.getHeader())
        self.makeCopier()
        self.makeCache(MemoryTableSource(table))

    def makeCache(self, source):
        self.cache = {}
        for row in source:
            key = self.makeKey(row)
            rows = self.cache.get(key, None)
            if rows == None:
                rows = []
                self.cache[key] = rows
            rows.append(row)

    def lookupRowsImpl(self, keys):
        key = tuple(keys)
        return self.cache.get(key, [])[:]

class CachedLookup(LookupProxy):
    def __init__(self, lookup):
        LookupProxy.__init__(self, lookup)

    def openImpl(self):
        self.getNestedLookup().open()
        self.cache = {}

    def lookupRowsImpl(self, keys):
        key = tuple(keys)
        result = self.cache.get(key, None)
        if result == None:
            result = self.getNestedLookup().lookupRows(key)
            self.cache[key] = result
        return result[:]

class MemoryTableLookup(BufferedLookup):
    def __init__(self, keys, table):
        BufferedLookup.__init__(self, keys, MemoryTableSource(table))

class DBAPILookup(Lookup):
    def __init__(self, keys, conn, query):
        Lookup.__init__(self, keys)
        self.conn = conn
        self.query = query

    def openImpl(self):
        self.conn.open()
        Lookup.openImpl(self)
        curs = self.conn.getConnection().cursor()
        curs.execute(self.query, [None]*len(self.getKeys()))
        header = self.conn.makeHeader(curs)
        self.setHeader(header)
        curs.close()

    def closeImpl(self):
        Lookup.closeImpl(self)
        self.conn.close()

    def lookupRowsImpl(self, keys):
        curs = self.conn.getConnection().cursor()
        curs.execute(self.query, keys)
        result = curs.fetchall()
        curs.close()
        return result

class TableLookup(DBAPILookup):
    def __init__(self, table, keys, conn):
        DBAPILookup.__init__(self, keys, conn, conn.makeSelect(table, keys))

class LookupSource(RowSourceProxy):
    def __init__(self, nestedSource, lookup, keys=None, fields=None, hitCountCol=None, passAll=False):
        RowSourceProxy.__init__(self, nestedSource)
        self.lookup = lookup
        self.keys = keys
        self.fields = fields
        self.hitCountCol = hitCountCol
        self.passAll = passAll

    def openImpl(self):
        self.getNestedSource().open()
        self.lookup.open()
        self.keysHeader = self.getNestedSource().getHeader().cloneByNames(self.keys or self.lookup.getKeys())
        self.resultHeader = self.getNestedSource().getHeader().copy()
        addHeader = self.lookup.getHeader()
        if self.fields:
            addHeader = addHeader.cloneByNames(self.fields)
        self.resultHeader.addHeader(addHeader)
        if self.hitCountCol:
            self.resultHeader.append(Field(self.hitCountCol, int))
        self.currentRow = None
        self.currentLookup = None
        self.keyCopier = FieldsCopier()
        self.keyCopier.makeHeaderPaths(self.getNestedSource().getHeader(), self.keysHeader)
        self.lookupCopier = FieldsCopier()
        self.lookupCopier.makeHeaderPaths(self.lookup.getHeader(), self.resultHeader)
        self.sourceCopier = FieldsCopier()
        self.sourceCopier.makeHeaderPaths(self.getNestedSource().getHeader(), self.resultHeader)

    def getHeaderImpl(self):
        return self.resultHeader
        
    def setHeaderImpl(self, header):
        self.resultHeader = header
        
    def closeImpl(self):
        self.lookup.close()
        self.getNestedSource().close()
        
    def nextImpl(self):
        if self.currentRow == None:
            self.currentRow = self.getNestedSource().next()
            keys = self.keysHeader.newBuffer()
            self.keyCopier.copyFields(self.currentRow, keys)
            self.currentLookup = self.lookup.lookupRows(keys)
            self.hitCount = len(self.currentLookup)
        result = self.resultHeader.newBuffer()
        if self.currentLookup:
            self.lookupCopier.copyFields(self.currentLookup.pop(0), result)
        self.sourceCopier.copyFields(self.currentRow, result)
        if self.hitCountCol:
            result[-1] = self.hitCount
        if not self.passAll or not self.currentLookup:
            self.currentRow = None
        return result
            
