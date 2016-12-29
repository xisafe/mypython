
from base import RowSourceProxy, callableFromString
from structure import Context, Row

class FilteredSource(RowSourceProxy):
    def __init__(self, src, *exprs):
        RowSourceProxy.__init__(self, src)
        self.exprs = exprs

    def openImpl(self):
        self.getNestedSource().open()
        self.filters = []
        for expr in self.exprs:
            if callable(expr):
                self.filters.append(expr)
            else:
                self.filters.append(callableFromString(str(expr)))
        self.srcRow = Row(self.getNestedSource().getHeader())
        self.ctx = Context()
        self.ctx["src"] = self.srcRow

    def nextImpl(self):
        while True:
            row = self.getNestedSource().next()
            self.srcRow.setCurrent(row)
            passing = True
            for flt in self.filters:
                passing = passing and bool(flt(self.ctx))
                if not passing:
                    break
            if passing:
                return row

