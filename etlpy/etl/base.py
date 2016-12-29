import threading

class EventDispatcher:
    def __init__(self, source, name):
        self.source = source
        self.name = name
        self.handlers = []

    def fire(self):
        for handler in self.handlers:
            handler(source, self.name)

    def subscribe(self, handler):
        self.handlers.append(handler, self.name)

class Field:
    def __init__(self, name, type=type(''), length=None, prec=0):
        self.name = name
        self.type = type
        self.length = length
        self.prec = prec

    def copy(self):
        return Field(self.name, type=self.type, length=self.length, prec=self.prec)

    def getName(self):
        return self.name

    def getType(self):
        return self.type

    def getLength(self):
        return self.length

    def getPrec(self):
        return self.prec

    def getSpec(self):
        return (self.type.__name__, self.length, self.prec)

class Header:
    def __init__(self, fields = None, names = None):
        self.fields = []
        for field in ((fields or []) + (names or [])):
            if isinstance(field, (tuple, list)):
                self.fields.append(Field(*field))
            elif isinstance(field, Field):
                self.fields.append(field)
            else:
                self.fields.append(Field(field))
        self.index = None

    def copy(self):
        return Header(fields=[f.copy() for f in self.fields])

    def cloneByNames(self, names):
        return Header(fields=[self[name].copy() for name in names])

    def makeIndex(self):
        if self.index == None:
            self.index = {}
            for i, f in enumerate(self.fields):
                self.index[f.name] = i

    def safeIndex(self, name):
        self.makeIndex()
        return self.index.get(name, -1)
             
    def getIndex(self, name):
        self.makeIndex()
        return self.index[name]
             
    def append(self, f):
        self.fields.append(f)
        return self

    def __len__(self):
        return len(self.fields)

    def __getitem__(self, i):
        return self.fields[self.trueIndex(i)]

    def check(self, row):
        assert len(row) == len(self.fields)
        for i, r in enumerate(row):
            assert type(r) == self.fields[i].type

    def checkHeader(self, header):
        assert len(header) == len(self.fields)
        for i, f in enumerate(header.fields):
            assert f.type == self.fields[i].type
            assert f.name == self.fields[i].name

    def trueIndex(self, idx):
        if type(idx) == int:
            return idx
        else:
            return self.getIndex(idx)

    def listNames(self):
        return [f.getName() for f in self.fields]

    def listSpecs(self):
        return [f.getSpec() for f in self.fields]

    def hasField(self, name):
        self.makeIndex()
        return self.index.has_key(name)

    def addHeader(self, header):
        for field in header.fields:
            if not self.hasField(field.getName()):
                self.append(field)
        self.index = None

    def newBuffer(self):
        return [None]*len(self)

class IStructured:
    def setHeader(self, header):
        raise NotImplementedError()

    def getHeader(self):
        raise NotImplementedError()

class StructuredBase(IStructured):
    def __init__(self, header = None):
        self.onAfterSetHeader = EventDispatcher(self, "AfterSetHeader")
        self.header = header

    def setHeaderImpl(self, header):
        self.header = header

    def setHeader(self, header):
        self.setHeaderImpl(header)
        self.afterSetHeaderEvent()

    def getHeaderImpl(self):
        return self.header

    def getHeader(self):
        return self.getHeaderImpl()

    def afterSetHeaderEvent(self):
        self.onAfterSetHeader.fire()

    def newBuffer(self):
        return self.getHeader() and self.getHeader().newBuffer() or []

class IComponent:
    def open(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

class ComponentBase(IComponent):
    def __init__(self):
        self.onBeforeOpen = EventDispatcher(self, "BeforeOpen")
        self.onAfterOpen = EventDispatcher(self, "AfterOpen")
        self.onBeforeClose = EventDispatcher(self, "BeforeClose")
        self.onAfterClose = EventDispatcher(self, "AfterClose")

    def open(self):
        self.beforeOpenEvent()
        self.openImpl()
        self.afterOpenEvent()

    def openImpl(self):
        pass

    def beforeOpenEvent(self):
        self.onBeforeOpen.fire()

    def afterOpenEvent(self):
        self.onAfterOpen.fire()

    def close(self):
        self.beforeCloseEvent()
        self.closeImpl()
        self.afterCloseEvent()

    def closeImpl(self):
        pass

    def beforeCloseEvent(self):
        self.onBeforeClose.fire()

    def afterCloseEvent(self):
        self.onAfterClose.fire()

class IActivity:
    def run(self):
        raise NotImplementedError()

class ActivityBase(IActivity):
    def __init__(self):
        self.onBeforeRun = EventDispatcher(self, "BeforeRun")
        self.onAfterRun = EventDispatcher(self, "AfterRun")

    def run(self):
        self.beforeRunEvent()
        self.runImpl()
        self.afterRunEvent()

    def runImpl(self):
        pass

    def beforeRunEvent(self):
        self.onBeforeRun.fire()

    def afterRunEvent(self):
        self.onAfterRun.fire()

class IBuffered:
    def flush(self):
        raise NotImplementedError()

class BufferedBase(IBuffered):
    def __init__(self):
        self.onBeforeFlush = EventDispatcher(self, "BeforeFlush")
        self.onAfterFlush = EventDispatcher(self, "AfterFlush")

    def flush(self):
        self.beforeFlushEvent()
        self.flushImpl()
        self.afterFlushEvent()

    def flushImpl(self):
        pass

    def beforeFlushEvent(self):
        self.onBeforeFlush.fire()

    def afterFlushEvent(self):
        self.onAfterFlush.fire()

class IRowSource:
    def beforeFirstInput(self):
        raise NotImplementedError()

    def afterLastInput(self):
        raise NotImplementedError()

    def next(self):
        raise NotImplementedError()

class RowSourceBase(IRowSource):
    def __init__(self):
        self.onBeforeFirstInput = EventDispatcher(self, "BeforeFirstInput")
        self.onAfterLastInput = EventDispatcher(self, "AfterLastInput")
        self.onAfterNext = EventDispatcher(self, "AfterNext")

    def next(self):
        row = self.nextImpl()
        self.afterNext()
        return row

    def nextImpl(self):
        raise StopIteration()

    def __iter__(self):
        return self

    def beforeFirstInput(self):
        self.beforeFirstInputImpl()
        self.beforeFirstInputEvent()

    def beforeFirstInputEvent(self):
        self.onBeforeFirstInput.fire()
    
    def beforeFirstInputImpl(self):
        pass

    def afterLastInput(self):
        self.afterLastInputImpl()
        self.afterLastInputEvent()
    
    def afterLastInputEvent(self):
        self.onAfterLastInput.fire()
    
    def afterLastInputImpl(self):
        pass
    
    def afterNext(self):
        self.afterNextImpl()
        self.afterNextEvent()
    
    def afterNextEvent(self):
        self.onAfterNext.fire()
    
    def afterNextImpl(self):
        pass
    
class RowSource(StructuredBase, ComponentBase, RowSourceBase):
    def __init__(self, header = None):
        ComponentBase.__init__(self)
        StructuredBase.__init__(self, header)
        RowSourceBase.__init__(self)

class RowSourceProxy(RowSource):
    def __init__(self, nestedSource = None):
        RowSource.__init__(self)
        self.setNestedSource(nestedSource)

    def getNestedSource(self):
        return self.nestedSource

    def setNestedSource(self, nestedSource):
        self.nestedSource = nestedSource

    def openImpl(self):
        if self.nestedSource:
            return self.nestedSource.open()

    def closeImpl(self):
        if self.nestedSource:
            return self.nestedSource.close()

    def nextImpl(self):
        if self.nestedSource:
            return self.nestedSource.next()

    def beforeFirstInputImpl(self):
        if self.nestedSource:
            return self.nestedSource.beforeFirstInput()

    def afterLastInputImpl(self):
        if self.nestedSource:
            return self.nestedSource.afterLastInput()

    def setHeaderImpl(self, header):
        if self.nestedSource:
            return self.nestedSource.setHeader(header)

    def getHeaderImpl(self):
        if self.nestedSource:
            return self.nestedSource.getHeader()

class IteratorSource(RowSource):
    def __init__(self, header, it):
        RowSource.__init__(self, header)
        self.it = iter(it)

    def nextImpl(self):
        return self.it.next()

    def closeImpl(self):
        self.it = None

class IRowConsumer:
    def connectSource(self, source):
        raise NotImplementedError()

class RowConsumerBase(IRowConsumer):
    def __init__(self):
        self.onAfterConnectSource = EventDispatcher(self, "AfterConnectSource")

    def connectSource(self, source):
        self.connectSourceImpl(source)
        self.afterConnectSourceEvent()

    def connectSourceImpl(self, source):
        self.source = source

    def afterConnectSourceEvent(self):
        self.onAfterConnectSource.fire()

    def getSource(self):
        return self.source

class IRowProducer:
    def connectTarget(self, target):
        raise NotImplementedError()

class RowProducerBase(IRowProducer):
    def __init__(self):
        self.onAfterConnectTarget = EventDispatcher(self, "AfterConnectTarget")

    def connectTarget(self, target):
        self.connectTargetImpl(target)
        self.afterConnectTargetEvent()

    def connectTargetImpl(self, target):
        self.target = target

    def afterConnectTargetEvent(self):
        self.onAfterConnectTarget.fire()

class IRowTarget:
    def writerow(self, row):
        raise NotImplementedError()

    def beforeFirstOutput(self):
        raise NotImplementedError()

    def afterLastOutput(self):
        raise NotImplementedError()


class RowTargetBase(IRowTarget):
    def __init__(self):
        self.onBeforeFirstOutput = EventDispatcher(self, "BeforeFirstOutput")
        self.onAfterLastOutput = EventDispatcher(self, "AfterLastOutput")
        self.onAfterWriterow = EventDispatcher(self, "AfterWriterow")

    def writerow(self, row):
        self.writerowImpl(row)
        self.afterWriterow()

    def writerowImpl(self, row):
        pass

    def beforeFirstOutput(self):
        self.beforeFirstOutputImpl()
        self.beforeFirstOutputEvent()

    def beforeFirstOutputEvent(self):
        self.onBeforeFirstOutput.fire()
    
    def beforeFirstOutputImpl(self):
        pass

    def afterWriterow(self):
        self.afterWriterowImpl()
        self.afterWriterowEvent()
    
    def afterWriterowEvent(self):
        self.onAfterWriterow.fire()
    
    def afterWriterowImpl(self):
        pass
    
    def afterLastOutput(self):
        self.afterLastOutputImpl()
        self.afterLastOutputEvent()
    
    def afterLastOutputEvent(self):
        self.onAfterLastOutput.fire()
    
    def afterLastOutputImpl(self):
        pass
    
class StructuredRowConsumer(StructuredBase, RowConsumerBase):
    def __init__(self, header = None):
        StructuredBase.__init__(self, header)
        RowConsumerBase.__init__(self)

    def connectSourceImpl(self, source):
        RowConsumerBase.connectSourceImpl(self, source)
        if self.getHeader() == None:
            self.setHeader(source.getHeader())

class RowTarget(StructuredRowConsumer, ComponentBase, BufferedBase, RowTargetBase):
    def __init__(self, header = None):
        StructuredRowConsumer.__init__(self, header)
        ComponentBase.__init__(self)
        BufferedBase.__init__(self)
        RowTargetBase.__init__(self)

class MemoryTable(StructuredBase):
    def __init__(self, header=None, rows=None):
        StructuredBase.__init__(self, header)
        self.rows = rows != None and rows or []

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]

    def append(self, row):
        self.rows.append(row)

    def extend(self, it):
        for row in iter(it):
            self.append(row)

    def getRows(self):
        return self.rows

class MemoryTableTarget(RowTarget):
    def __init__(self, table):
        RowTarget.__init__(self, table.getHeader())
        self.table = table

    def writerowImpl(self, row):
        self.table.append(row)

    def setHeaderImpl(self, header):
        self.table.setHeader(header)

    def getHeaderImpl(self):
        return self.table.getHeader()

class MemoryTableIter:
    def __init__(self, table):
        self.table = table
        self.index = 0

    def __iter__(self):
        return self

    def next(self):
        if self.index >= len(self.table):
            raise StopIteration()
        row = self.table[self.index]
        self.index += 1
        return row

class MemoryTableSource(IteratorSource):
    def __init__(self, table):
        IteratorSource.__init__(self, table.getHeader(), MemoryTableIter(table))

class IActivityContainer:
    def registerActivity(self, name, comp):
        raise NotImplementedError()

    def registerDependency(self, after, before):
        raise NotImplementedError()

class ActivityContainerBase(IActivityContainer):
    def __init__(self, acts=None, deps=None):
        self.onAfterRegisterActivity = EventDispatcher(self, "AfterRegisterActivity")
        self.comps = {}
        self.depends = {}
        for k, v in (acts or {}).items():
            self.registerActivity(k, v)
        for k, v in (deps or {}).items():
            self.registerDependency(k, v)

    def registerActivity(self, name, comp):
        self.registerActivityImpl(name, comp)
        self.afterRegisterActivityEvent()
        return self

    def registerDependency(self, after, before):
        if not self.depends.has_key(after):
            self.depends[after] = []
        self.depends[after].append(before)

    def add(self, name=None, activity=None, dependsOn=None):
        self.registerActivity(name, activity)
        for dep in dependsOn or []:
            self.registerDependency(name, dep)
        return self

    def registerActivityImpl(self, name, comp):
        self.comps[name] = comp

    def __getitem__(self, name):
        return self.comps[name]

    def __setitem__(self, name, item):
        self.registerActivity(name, item)

    def items(self):
        for k, v in self.comps.items():
            yield (k, v)

    def __len__(self):
        return len(self.comps)

    def afterRegisterActivityEvent(self):
        self.onAfterRegisterActivity.fire()

class ActivityThread(threading.Thread):
    def __init__(self, name, activity):
        threading.Thread.__init__(self, name=name)
        self.activity = activity
        self.deps = []

    def addDependence(self, dep):
        self.deps.append(dep)

    def run(self):
        for thread in self.deps:
            thread.join()
        self.activity.run()

class ActivityContainer(ActivityContainerBase, ComponentBase, ActivityBase):
    def __init__(self):
        ActivityContainerBase.__init__(self)
        ComponentBase.__init__(self)
        ActivityBase.__init__(self)

    def openImpl(self):
        seen = {}
        order = []
        for name, comp in self.items(): 
            self.deps(name, seen, order)
        self.threads = {}
        for name in order:
            thread = ActivityThread(name, self.comps[name])
            if self.depends.has_key(name):
                for dep in self.depends[name]:
                    thread.addDependence(self.threads[dep])
            self.threads[name] = thread
            thread.start()

    def deps(self, name, seen, order):
        if not isinstance(self.comps[name], IActivity):
            return
        depSeen = seen.get(name, None)
        if depSeen == 2:
            return
        if depSeen == 1:
            raise RuntimeError("circular dependency: %s"%dep)
        seen[name] = 1
        if self.depends.has_key(name):
            for dep in self.depends[name]:
                self.deps(dep, seen, order)
        seen[name] = 2
        order.append(name)

    def closeImpl(self):
        pass

    def runImpl(self):
        self.open()
        for thread in self.threads.values():
            thread.join()
        self.close()

class IRowTransformer:
    def transformRow(self, row):
        raise NotImplemented()

class RowTransformerBase(IRowTransformer):
    def transformRow(self, row):
        return self.transformRowImpl(row)

    def transformRowImpl(self, row):
        return row

class Pump(ComponentBase, RowProducerBase, RowConsumerBase, ActivityBase, RowTransformerBase):
    def __init__(self, src, trg, bufsize=0):
        ComponentBase.__init__(self)
        RowProducerBase.__init__(self)
        RowConsumerBase.__init__(self)
        self.connectSource(src)
        self.connectTarget(trg)
        ActivityBase.__init__(self)
        self.bufsize = bufsize

    def __call__(self):
        return self.runImpl()

    def openImpl(self):
        self.source.open()
        self.target.connectSource(self.source)
        self.target.open()

    def closeImpl(self):
        self.target.close()
        self.source.close()

    def runImpl(self):
        self.open()
        self.source.beforeFirstInput()
        self.target.beforeFirstOutput()
        for i, row in enumerate(self.source):
            self.target.writerow(self.transformRow(row))
            if self.bufsize > 0 and (i + 1)%self.bufsize == 0:
                self.target.flush()
        self.target.flush()
        self.target.afterLastOutput()
        self.source.afterLastInput()
        self.close()

class FieldsCopier:
    def copyFields(self, src, trg):
        for src_i, trg_i in self.paths:
            trg[trg_i] = src[src_i]

    def makeHeaderPaths(self, srcHeader, trgHeader):
        self.paths = []
        for src_i in xrange(len(srcHeader)):
            name = srcHeader[src_i].name
            trg_i = trgHeader.safeIndex(name)
            if trg_i >= 0:
                self.paths.append((src_i, trg_i))
        return self

class FieldsPump(Pump, FieldsCopier):
    def __init__(self, src, trg):
        Pump.__init__(self, src, trg)

    def openImpl(self):
        Pump.openImpl(self)
        self.makeHeaderPaths(self.source.getHeader(), self.target.getHeader())

    def transformRowImpl(self, row):
        buffer = self.target.newBuffer()
        self.copyFields(row, buffer)
        return buffer

def callableFromString(expr):
    expr = compile(expr, "<string>", "eval")
    def evaluator(ctx, expr=expr):
        return eval(expr, globals(), ctx)
    return evaluator

