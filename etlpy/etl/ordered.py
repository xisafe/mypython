
import os, base
from base import Pump, RowSourceProxy, StructuredBase, RowSource, Field, FieldsCopier, IteratorSource
from fileetl import FileTarget, FileSource

class IOrder:
    def compare(self, r1, r2):
        raise NotImplemented()

class Order(StructuredBase, IOrder):
    def __init__(self, header, cols=None):
        StructuredBase.__init__(self, header)
        self.idxs = []
        self.ascs = []
        if cols:
            for col in cols:
                if type(col) == type(()):
                    name, asc = col
                else:
                    name = col
                    asc = True
                self.addCol(name, asc)

    def addCol(self, name, asc=True):
        self.idxs.append(self.getHeader().getIndex(name))
        self.ascs.append(asc)

    def elements(self, i, r1, r2):
        idx = self.idxs[i]
        return r1[idx], r2[idx]

    def compare(self, r1, r2):
        for i, asc in enumerate(self.ascs):
            e1, e2 = self.elements(i, r1, r2)
            if e1 < e2:
                return asc and -1 or 1
            if e1 > e2:
                return asc and 1 or -1
        return 0

    def listNames(self):
        return [self.getHeader()[i].getName() for i in self.idxs]

class Order2(Order):
    def __init__(self, header, header2, cols=None):
        self.header2 = header2
        self.idxs2 = []
        Order.__init__(self, header, cols)

    def addCol(self, name, asc=True):
        Order.addCol(self, name, asc)
        self.idxs2.append(self.header2.getIndex(name))

    def elements(self, i, r1, r2):
        return r1[self.idxs[i]], r2[self.idxs2[i]]

class GroupedSource(RowSourceProxy):
    ORDERED_INITIAL=0
    ORDERED_GROUP_BEGIN=1
    ORDERED_GROUP_NEXT=2

    def __init__(self, nestedSource, order, checkOrder=True, checkUnique=False):
        RowSourceProxy.__init__(self, nestedSource)
        self.curr = None
        self.order = order
        self.checkOrder = checkOrder
        self.checkUnique = checkUnique
        self.state = self.ORDERED_INITIAL

    def nextImpl(self):
        prev = self.curr
        self.curr = self.getNestedSource().next()
        if prev == None:
            self.groupChanged()
        else:
            cmp = self.order.compare(prev, self.curr)
            if cmp > 0:
                self.orderChanged()
            elif cmp == 0:
                self.nextInGroup()
            elif cmp < 0:
                self.groupChanged()
        return self.curr

    def orderChanged(self):
        if self.checkOrder:
            raise RuntimeError("row stream not ordered: %r"%self.curr)

    def nextInGroup(self):
        if self.checkUnique:
            raise RuntimeError("row stream keys not unique: %r"%self.curr)
        self.state = self.ORDERED_GROUP_NEXT

    def groupChanged(self):
        self.state = self.ORDERED_GROUP_BEGIN

    def isFirstInGroup(self):
        return self.state in [self.ORDERED_GROUP_BEGIN, self.ORDERED_INITIAL]

    def isGroupChanged(self):
        return self.state in [self.ORDERED_GROUP_BEGIN]

class Merge(RowSource):
    def __init__(self, src1, src2, cols=None, order=None):
        RowSource.__init__(self)
        self.src1 = src1
        self.src2 = src2
        self.cols = cols or []
        self.order = order

    def openImpl(self):
        self.src1.open()
        self.src2.open()
        self.makeHeader()
        self.makeOrder()
        self.makeProxy()

    def makeProxy(self):
        self.src1 = GroupedSource(self.src1, self.order)
        self.src2 = GroupedSource(self.src2, self.order)

    def makeHeader(self):
        self.src1.getHeader().checkHeader(self.src2.getHeader())
        self.setHeader(self.src1.getHeader().copy())

    def makeOrder(self):
        if not self.order:
            self.order = Order(self.src1.getHeader(), self.cols)

    def closeImpl(self):
        self.src1.close()
        self.src2.close()

    def beforeFirstInputImpl(self):
        self.src1.beforeFirstInput()
        self.src2.beforeFirstInput()
        self.next1()
        self.next2()

    def afterLastInputImpl(self):
        self.src1.afterLastInput()
        self.src2.afterLastInput()

    def next1(self):
        try:
            self.r1 = self.src1.next()
        except StopIteration, ex:
            self.r1 = None

    def next2(self):
        try:
            self.r2 = self.src2.next()
        except StopIteration, ex:
            self.r2 = None

    def nextImpl(self):
        result = None
        while result == None:
            if self.r1 == None:
                if self.r2 == None:
                    raise StopIteration()
                else:
                    result = self.processGreater()
            else:
                if self.r2 == None:
                    result = self.processLess()
                else:
                    cmp = self.order.compare(self.r1, self.r2)
                    if cmp < 0:
                        result = self.processLess()
                    elif cmp > 0:
                        result = self.processGreater()
                    else:
                        result = self.processEqual()
        return result

    def processEqual(self):
        return self.processLess()

    def processLess(self):
        result = self.r1
        self.next1()
        return result

    def processGreater(self):
        result = self.r2
        self.next2()
        return result


class Diff(Merge):
    def __init__(self, src1, src2, cols, cols2=None, diff="Diff"):
        Merge.__init__(self, src1, src2, cols)
        self.cols2 = cols2
        self.diff = diff

    def makeProxy(self):
        self.src1 = GroupedSource(self.src1, self.order, checkUnique=True)
        self.src2 = GroupedSource(self.src2, self.order, checkUnique=True)

    def openImpl(self):
        Merge.openImpl(self)
        if self.cols2 == None:
            self.cols2 = self.getHeader().listNames()
        self.getHeader().append(Field(self.diff, type=str, length=1))
        self.order2 = Order(self.getHeader(), self.cols2)

    def processLess(self):
        result = Merge.processLess(self)
        result.append("D")
        return result

    def processGreater(self):
        result = Merge.processGreater(self)
        result.append("I")
        return result

    def processEqual(self):
        result = None
        if self.order2.compare(self.r1, self.r2) != 0:
            result = self.r2
            result.append("U")
        self.next1()
        self.next2()
        return result

JOIN_REGULAR = 1
JOIN_LEFT    = 2
JOIN_RIGHT   = 3
JOIN_FULL    = 4

class MergeJoin(Merge):
    def __init__(self, src1, src2, cols, join_type=JOIN_REGULAR):
        Merge.__init__(self, src1, src2, cols)
        self.join_type = join_type

    def makeHeader(self):
        header = self.src1.getHeader().copy()
        header.addHeader(self.src2.getHeader())
        self.setHeader(header)
        self.copier1 = FieldsCopier()
        self.copier1.makeHeaderPaths(self.src1.getHeader(), header)
        self.copier2 = FieldsCopier()
        self.copier2.makeHeaderPaths(self.src2.getHeader(), header)

    def makeOrder(self):
        self.order = Order2(self.src1.getHeader(), self.src2.getHeader(), self.cols)

    def makeProxy(self):
        self.src1 = GroupedSource(self.src1, self.order, checkUnique=True)
        self.src2 = GroupedSource(self.src2, self.order, checkUnique=True)

    def processLess(self):
        if self.r2 == None and self.join_type in [JOIN_RIGHT, JOIN_REGULAR]:
            raise StopIteration()
        if self.join_type in [JOIN_LEFT, JOIN_FULL]:
            result = self.makeRecord(1)
        else:
            result = None
        self.next1()
        return result


    def processGreater(self):
        if self.r1 == None and self.join_type in [JOIN_LEFT, JOIN_REGULAR]:
            raise StopIteration()
        if self.join_type in [JOIN_RIGHT, JOIN_FULL]:
            result = self.makeRecord(2)
        else:
            result = None
        self.next2()
        return result

    def processEqual(self):
        result = self.makeRecord(3)
        self.next1()
        self.next2()
        return result

    def makeRecord(self, i):
        result = self.newBuffer()
        if i&2:
            self.copier2.copyFields(self.r2, result)
        if i&1:
            self.copier1.copyFields(self.r1, result)
        return result

class MergeLookup(MergeJoin):
    def __init__(self, src1, src2, cols):
        MergeJoin.__init__(self, src1, src2, cols, join_type=JOIN_LEFT)

    def makeProxy(self):
        self.src1 = GroupedSource(self.src1, self.order, checkUnique=False)
        self.src2 = GroupedSource(self.src2, self.order, checkUnique=True)

    def next2(self):
        if self.src1.isFirstInGroup():
            MergeJoin.next2(self)

class Sort(RowSourceProxy):
    def __init__(self, src, cols, maxrows=1000000):
        RowSourceProxy.__init__(self, src)
        self.cols = cols
        self.maxrows = maxrows

    def openImpl(self):
        self.getNestedSource().open()
        self.order = Order(self.getNestedSource().getHeader(), self.cols)
        self.setHeader(self.getNestedSource().getHeader())
        self.oldSrc = None

    def removeAll(self, src):
        if isinstance(src, FileSource):
            os.remove(src.name)
        elif isinstance(src, Merge):
            self.removeAll(src.src1)
            self.removeAll(src.src2)
        elif isinstance(src, RowSourceProxy):
            self.removeAll(src.getNestedSource())

    def closeImpl(self):
        realSrc = self.getNestedSource()
        realSrc.close()
        self.removeAll(realSrc)
        self.setNestedSource(self.oldSrc)

    def beforeFirstInputImpl(self):
        self.getNestedSource().beforeFirstInput()
        rows = []
        temps = []
        for i, row in enumerate(self.getNestedSource()):
            rows.append(row)
            if (i + 1)%self.maxrows == 0:
                temps.append(self.flushBuffer(rows))
                rows = []
        self.oldSrc = self.getNestedSource()
        self.oldSrc.afterLastInput()
        self.oldSrc.close()
        if not temps:
            realSrc = self.memorySort(rows)
        else:
            realSrc = self.mergeTemps(temps, rows)
        realSrc.open()
        realSrc.beforeFirstInput()
        self.setNestedSource(realSrc)

    def memorySort(self, rows):
        rows.sort(cmp=self.order.compare)
        return IteratorSource(self.getHeader(), rows)


    def mergeTemps(self, temps, rows):
        src = FileSource(temps[0].name)
        for trg in temps[1:]:
            src = Merge(src, FileSource(trg.name), order=self.order)
        if rows:
            src = Merge(src, self.memorySort(rows), order=self.order)
        return src

    def mergeTempsOld(self, temps, rows):
        hasMemRows = len(rows) > 0
        while len(temps) > 2 or hasMemRows:
            newtemps = []
            for i in xrange(0, len(temps), 2):
                src1 = FileSource(temps[i].name)
                isLast = i == len(temps) - 1
                src2 = None
                if not isLast: 
                    src2 = FileSource(temps[i + 1].name)
                else:
                    if hasMemRows:
                        src2 = self.memorySort(rows)
                        hasMemRows = False
                if not src2:
                    newtemps.append(temps[i])
                else:
                    filetarget = FileTarget(header=self.getHeader(), prefix='sort')
                    Pump(Merge(src1, src2, order=self.order), filetarget)()
                    newtemps.append(filetarget)
                    os.remove(src1.name)
                    if isinstance(src2, FileSource):
                        os.remove(src2.name)
            temps = newtemps
        result = FileSource(temps[0].name)
        if len(temps) > 1:
            result2 = FileSource(temps[1].name)
            result = Merge(result, result2, order=self.order)
        result.open()
        return result

    def flushBuffer(self, rows):
        memsource = self.memorySort(rows)
        filetarget = FileTarget(header=self.getHeader(), prefix='sort_')
        Pump(memsource, filetarget)()
        return filetarget

class LessOrder(IOrder):
    def compare(self, r1, r2):
        return -1

class Union(Merge):
    def __init__(self, src1, src2):
        Merge.__init__(self, src1, src2, cols=None, order=None)

    def makeProxy(self):
        pass

    def makeOrder(self):
        self.order = LessOrder()

class EqualOrder(IOrder):
    def compare(self, r1, r2):
        return 0

class Glue(MergeJoin):
    def __init__(self, src1, src2):
        MergeJoin.__init__(self, src1, src2, cols=None, join_type=JOIN_REGULAR)

    def makeProxy(self):
        pass

    def makeOrder(self):
        self.order = EqualOrder()

def glue_sources(*srcs):
    assert srcs
    src = srcs[0]
    for src2 in srcs[1:]:
        src = Glue(src, src2)
    return src

def union_sources(*srcs):
    assert srcs
    src = srcs[0]
    for src2 in srcs[1:]:
        src = Union(src, src2)
    return src
