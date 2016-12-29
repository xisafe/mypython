
from base import Field, Header, RowSourceProxy
from types import registry

class Context(dict):
    def __getattr__(self, name):
        return self.get(name, None)

    def __setattr__(self, name, val):
        dict.__setitem__(self, name, val)

class Row:
    def __init__(self, header):
        self.header = header
        self.current = None

    def setCurrent(self, current):
        self.current = current

    def __getattr__(self, name):
        return self.__getitem__(name)

    def __setattr__(self, name, val):
        if not self.__dict__.has_key("header") or not self.__dict__.has_key("current"):
            self.__dict__[name] = val
        idx = self.header.safeIndex(name)
        if idx >= 0:
            self.current[idx] = val
        else:
            self.__dict__[name] = val

    def __getitem__(self, name):
        return self.current[self.header.trueIndex(name)]

    def __setitem__(self, name, v):
        self.current[self.header.trueIndex(name)] = v

class ChangeStructure(RowSourceProxy):
    def __init__(self, src, keep=None, drop=None, rename=None, dropall=False, newfields=None, changetype=None, changevalue=None, ctxinit=None, temps=None):
        RowSourceProxy.__init__(self, src)
        self.keep = keep or []
        self.drop = drop or []
        self.rename = rename or {}
        self.dropall = dropall or keep
        self.newfields = newfields or []
        self.changetype = changetype or {}
        self.changevalue = changevalue or {}
        self.temps = temps or []
        self.ctxinit = ctxinit

    def needToKeep(self, name):
        if name in self.keep or self.rename.has_key(name) or self.changetype.has_key(name) or self.changevalue.has_key(name):
            return True
        if name in self.drop:
            return False
        return not self.dropall 

    def makeFieldCopier(self, i, cnv=None):
        def copier(ctx, i=i, cnv=cnv):
            val = ctx["src"][i]
            if cnv: val = cnv(val)
            return val
        return copier

    def makeFieldEvaluator(self, expr):
        expr = compile(expr, "<string>", "eval")
        def evaluator(ctx, expr=expr):
            return eval(expr, globals(), ctx)
        return evaluator

    def makeConstFactory(self, const):
        def makeConst(ctx, const=const):
            return const
        return makeConst

    def makeNewHeader(self):
        oldheader = self.getNestedSource().getHeader()
        header2 = Header()
        self.valueFactory = []
        for i, field in enumerate(oldheader):
            if self.needToKeep(field.name):
                newfield = field.copy()
                newfield.name = self.rename.get(field.name, field.name)
                vf = self.changevalue.get(field.name, None)
                if isinstance(vf, str):
                    vf = self.makeFieldEvaluator(vf)
                cnv = None
                ct = self.changetype.get(field.name, None)
                if ct:
                    newtype = type(ct)
                    if isinstance(ct, (tuple, list)):
                        newtype, cnv = ct
                        if isinstance(newtype, str):
                            newtype = registry.getTypeByName(newtype)
                    elif isinstance(ct, (type, str)):
                        newtype = ct
                        if isinstance(newtype, str):
                            newtype = registry.getTypeByName(newtype)
                        cnv = registry.getConverterByTypes(newfield.type, newtype)
                    newfield.type = newtype
                vf = vf or self.makeFieldCopier(i, cnv)
                self.valueFactory.append(vf)
                header2.append(newfield)
        for newfield in self.newfields:
            vf = None
            if isinstance(newfield, (tuple, list)):
                newfield, vf = newfield
                if isinstance(vf, str):
                    vf = self.makeFieldEvaluator(vf)
            if not callable(vf):
                vf = self.makeConstFactory(vf)
            if isinstance(newfield, (tuple, list)):
                newfield = Field(*newfield)
            elif isinstance(newfield, int):
                newfield = oldheader[newfield].copy()
            elif not isinstance(newfield, Field):
                newfield = Field(newfield)
            self.valueFactory.append(vf)
            header2.append(newfield)
        self.setHeader(header2)
        self.tempVars = []
        for name, vf in self.temps:
            if isinstance(vf, str):
                vf = self.makeFieldEvaluator(vf)
            if not callable(vf):
                vf = self.makeConstFactory(vf)
            self.tempVars.append((name, vf))
            
    def openImpl(self):
        self.getNestedSource().open()
        self.srcRow = Row(self.getNestedSource().getHeader())
        self.ctx = Context()
        self.ctx["src"] = self.srcRow
        self.makeNewHeader()
        self.contextInit = self.ctxinit
        if self.contextInit and not callable(self.contextInit):
            self.contextInit = self.makeFieldEvaluator(self.contextInit)

    def nextImpl(self):
        row = self.getNestedSource().next()
        self.srcRow.setCurrent(row)
        if self.contextInit:
            self.contextInit(self.ctx)
        for name, vf in self.tempVars:
            self.ctx[name] = vf(self.ctx)
        newrow = self.newBuffer()
        for i in xrange(len(newrow)):
            newrow[i] = self.valueFactory[i](self.ctx)
        return newrow

    def getHeaderImpl(self):
        return self.header2

    def setHeaderImpl(self, header):
        self.header2 = header
