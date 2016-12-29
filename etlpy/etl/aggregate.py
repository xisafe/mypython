
from base import Field, RowSourceProxy, callableFromString, FieldsCopier
from structure import Context, Row
from ordered import GroupedSource, Order

class IAggregator:
    def reset(self):
        raise NotImplemented()

    def pushNextValue(self, value):
        raise NotImplemented()

    def getAggregatedValue(self):
        raise NotImplemented()

class AggregatorBase(IAggregator):
    def __init__(self, init, t, f):
        self.init = init
        self.t = t
        self.f = f

    def reset(self):
        if self.init == None or not self.t:
            self.value = None
        else:
            self.value = self.t(self.init)

    def pushNextValue(self, value):
        next = self.f(self.value, value)
        if self.t:
            self.value = self.t(next)
        else:
            self.value = next

    def getAggregatedValue(self):
        return self.value

def asum(agg, val):
    return agg + val

class AggregatorSum(AggregatorBase):
    def __init__(self, t=float):
        AggregatorBase.__init__(self, 0, t, asum)

class AggregatorAvg(AggregatorSum):
    def __init__(self, t=float):
        AggregatorSum.__init__(self, t)
        self.count = AggregatorCount(t)

    def reset(self):
        AggregatorSum.reset(self)
        self.count.reset()

    def pushNextValue(self, value):
        AggregatorSum.pushNextValue(self, value)
        self.count.pushNextValue(value)

    def getAggregatedValue(self):
        return AggregatorSum.getAggregatedValue(self)/self.count.getAggregatedValue()

def acount(agg, val):
    return agg + 1

class AggregatorCount(AggregatorBase):
    def __init__(self, t):
        AggregatorBase.__init__(self, 0, int, acount)

def amin(agg, val):
    if agg == None or val < agg:
        return val
    else:
        return agg

class AggregatorMin(AggregatorBase):
    def __init__(self, t):
        AggregatorBase.__init__(self, None, t, amin)

def amax(agg, val):
    if agg == None or val > agg:
        return val
    else:
        return agg

class AggregatorMax(AggregatorBase):
    def __init__(self, t):
        AggregatorBase.__init__(self, None, t, amax)

def afirst(agg, val):
    if agg == None:
        return val
    else:
        return agg

class AggregatorFirst(AggregatorBase):
    def __init__(self, t):
        AggregatorBase.__init__(self, None, t, afirst)

def alast(agg, val):
    return val

class AggregatorLast(AggregatorBase):
    def __init__(self, t):
        AggregatorBase.__init__(self, None, t, alast)

class AggregatedColumn:
    def __init__(self, header, aggtype, oldname=None, flt=None, newname=None):
        self.header = header
        self.aggtype = aggtype.lower()
        self.oldname = oldname
        self.name = newname or oldname and self.aggtype + '_' + oldname or self.aggtype + '_item'
        self.flt = flt

    def open(self):
        t = self.oldname and self.header[self.oldname].getType() or int
        if   self.aggtype == "min":
            self.aggregator = AggregatorMin(t)
        elif self.aggtype == "max":
            self.aggregator = AggregatorMax(t)
        elif self.aggtype == "sum":
            self.aggregator = AggregatorSum(t)
        elif self.aggtype == "avg":
            self.aggregator = AggregatorAvg(t)
        elif self.aggtype == "first":
            self.aggregator = AggregatorFirst(t)
        elif self.aggtype == "last":
            self.aggregator = AggregatorLast(t)
        elif self.aggtype == "count":
            self.aggregator = AggregatorCount(t)
        else:
            raise ValueError("Unknown aggregation type " + self.aggtype)
        self.cflt = callable(self.flt) and self.flt \
            or isinstance(self.flt, str) and callableFromString(self.flt) \
            or None
        self.ctx = Context()
        self.index = self.header.safeIndex(self.oldname)

    def acceptValue(self, value):
        if self.cflt == None:
            return True
        self.ctx[self.oldname] = value
        return self.cflt(self.ctx)

    def makeField(self):
        return Field(self.name, self.aggregator.t)

class AggregatedSource(RowSourceProxy):
    def __init__(self, src, groupby=None, aggs=None):
        RowSourceProxy.__init__(self, src)
        self.groupby = groupby or []
        self.aggs = aggs or []
        self.aheader = None

    def openImpl(self):
        self.getNestedSource().open()
        header = self.getNestedSource().getHeader()
        self.order = Order(header, self.groupby)
        self.asrc = GroupedSource(self.getNestedSource(), self.order)
        self.aheader = self.getNestedSource().getHeader().cloneByNames(self.order.listNames())
        self.aggColumns = []
        for agg in self.aggs:
            args = (header,) + tuple(agg)
            ac = AggregatedColumn(*args)
            ac.open()
            self.aggColumns.append(ac)
            self.aheader.append(ac.makeField())
        self.copier = FieldsCopier()
        self.copier.makeHeaderPaths(self.getNestedSource().getHeader(), self.aheader)
        self.result = None
        self.stop = False
        self.ngroups = 0

    def getHeader(self):
        return self.aheader or self.getNestedSource().getHeader()

    def closeImpl(self):
        self.asrc.close()
        self.aheader = None

    def nextImpl(self):
        if self.stop:
            raise StopIteration()
        EOGFound = False
        result = None
        while not EOGFound:
            try:
                row = self.asrc.next()
            except StopIteration:
                self.stop = True
                if self.result == None:
                    raise

            if self.stop or self.asrc.isGroupChanged():
                EOGFound = self.stop or self.ngroups > 0
                if self.ngroups > 0:
                    self.copyAggsToResult()
                    result = self.result[:]
                if not self.stop:
                    self.makeResultFromCurrent(row)
                    self.resetAggs()
                    self.ngroups += 1
            if not self.stop:
                self.pushAggs(row)
        if result == None:
            raise StopIteration()
        return result

    def copyAggsToResult(self):
        groupbylen = len(self.groupby)
        for i, ac in enumerate(self.aggColumns):
            value = ac.aggregator.getAggregatedValue()
            self.result[groupbylen + i] = value

    def makeResultFromCurrent(self, row):
        self.result = self.aheader.newBuffer()
        self.copier.copyFields(row, self.result)

    def resetAggs(self):
        for ac in self.aggColumns:
            ac.aggregator.reset()

    def pushAggs(self, row):
        for ac in self.aggColumns:
            value = None
            if ac.index >= 0:
                value = row[ac.index]
            if ac.acceptValue(value):
                ac.aggregator.pushNextValue(value)
