
from base import RowTarget, RowSource
import os
import tempfile
import cPickle as pickle
import codecs

class OutputStreamTarget(RowTarget):
    def __init__(self, stream, enc = None):
        RowTarget.__init__(self)
        self.stream = stream
        if enc:
            fe, te = enc
            self.stream = codecs.EncodedFile(self.stream, fe, te, 'ignore')

    def beforeFirstOutputImpl(self):
        self.stream.write("|".join([repr(elt) for elt in self.getHeader().listNames()]) + "\n")
        #self.stream.write("|".join([repr(s) for s in self.getHeader().listSpecs()]) + "\n")

    def writerowImpl(self, row):
        self.stream.write("|".join([str(elt) for elt in row]) + "\n")

class StreamTarget(RowTarget):
    def __init__(self, output, header=None):
        RowTarget.__init__(self, header)
        self.output = output

    def beforeFirstOutputImpl(self):
        pickle.dump(self.header, self.output, -1)

    def writerowImpl(self, row):
        pickle.dump(row, self.output, -1)

    def afterLastOutputImpl(self):
        pickle.dump(None, self.output, -1)

class FileTarget(StreamTarget):
    def __init__(self, name=None, header=None, prefix=None):
        StreamTarget.__init__(self, None, header)
        self.name = name
        self.prefix = prefix

    def openImpl(self):
        if self.name:
            self.output = open(self.name, 'wb')
        else:
            fd, self.name = tempfile.mkstemp(prefix=self.prefix)
            self.output = os.fdopen(fd, "wb")

    def closeImpl(self):
        self.output.close()
        
class StreamSource(RowSource):
    def __init__(self, input):
        RowSource.__init__(self)
        self.input = input

    def openImpl(self):
        header = pickle.load(self.input)
        self.setHeader(header)

    def nextImpl(self):
        row = pickle.load(self.input)
        if row == None:
            raise StopIteration()
        return row

class FileSource(StreamSource):
    def __init__(self, name):
        StreamSource.__init__(self, None)
        self.name = name

    def openImpl(self):
        self.input = open(self.name, 'rb')
        StreamSource.openImpl(self)

    def closeImpl(self):
        self.input.close()

