import time, datetime, decimal

class ITypeHome:
    def getName(self):
        raise NotImplementedError()

    def getType(self):        
        raise NotImplementedError()

    def getConverter(self, fromType):
        raise NotImplementedError()

class TypeHomeRegistry:
    def __init__(self):
        self.types = {}

    def registerType(self, home):
        self.types[home.getName()] = home

    def getTypeByName(self, name):
        return self.getHomeByName(name).getType()

    def getHomeByName(self, name):
        return self.types[name]

    def getHomeByType(self, type):
        return self.getHomeByName(type.__name__)

    def getHomeByValue(self, value):
        return self.getHomeByType(value.__class__)

    def getConverterByTypes(self, fromType, toType):
        return self.getHomeByType(toType).getConverter(fromType)

    def getConverterByNames(self, fromType, toType):
        return self.getHomeByName(toType).getConverter(self.getTypeByName(fromType))

def identity(x):
    return x

class TypeHomeBase(ITypeHome):
    def __init__(self, type):
        self.type = type
        self.convs = {}

    def getName(self):
        return self.type.__name__

    def getType(self):        
        return self.type

    def getConverter(self, fromType):
        if self.type == fromType:
            return identity
        return self.convs[fromType]

    def registerConverter(self, fromType, f):
        self.convs[fromType] = f

class IntTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, int)
        self.registerConverter(str, int)
        self.registerConverter(float, int)
        self.registerConverter(bool, int)
        self.registerConverter(decimal.Decimal, int)

class BoolTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, bool)
        self.registerConverter(str, bool)
        self.registerConverter(float, bool)
        self.registerConverter(decimal.Decimal, bool)

class FloatTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, float)
        self.registerConverter(str, float)
        self.registerConverter(int, float)
        self.registerConverter(decimal.Decimal, float)

DEVAULT_ENCODING="cp1251"

class StrTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, str)
        self.registerConverter(unicode, lambda x: x.encode(DEVAULT_ENCODING))
        self.registerConverter(float, str)
        self.registerConverter(int, str)
        self.registerConverter(bool, lambda x: str(x)[0])
        self.registerConverter(decimal.Decimal, str)
        self.registerConverter(datetime.date, formatDate)
        self.registerConverter(datetime.time, formatTime)
        self.registerConverter(datetime.datetime, formatDateTime)

class UnicodeTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, unicode)
        self.registerConverter(str, lambda x: unicode(x, DEVAULT_ENCODING))
        self.registerConverter(float, unicode)
        self.registerConverter(int, unicode)
        self.registerConverter(bool, lambda x: unicode(x)[0])
        self.registerConverter(decimal.Decimal, unicode)
        self.registerConverter(datetime.date, lambda x: unicode(formatDate(x)))
        self.registerConverter(datetime.time, lambda x: unicode(formatTime(x)))
        self.registerConverter(datetime.datetime, lambda x: unicode(formatDateTime(x)))

def formatDate(date):
    return date.isoformat()

def parseDate(sdate):
    st = time.strptime(sdate, '%Y-%m-%d')
    return datetime.date(st.tm_year, st.tm_mon, st.tm_mday)

class DateTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, datetime.date)
        self.registerConverter(str, parseDate)

def formatTime(time):
    return time.isoformat()

def parseTime(stime):
    p = stime.split(".", 1)
    if len(p) < 2:
        p1, p2 = p[0], None
    else:
        p1, p2 = p
    st = time.strptime(p1, '%H:%M:%S')
    t = datetime.time(st.tm_hour, st.tm_min, st.tm_sec)
    if p2:
        t.replace(microsecond=int(p2[:6]))
    return t

class TimeTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, datetime.time)
        self.registerConverter(str, parseTime)

def formatDateTime(dt):
    return dt.isoformat(' ')

def parseDateTime(sdt):
    return datetime.datetime.combine(parseDate(sdt[:10]), parseTime(sdt[11:]))

class DateTimeTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, datetime.datetime)
        self.registerConverter(str, parseDateTime)

def decimalFromFloat(f):
    return decimal.Decimal(str(f))

class DecimalTypeHome(TypeHomeBase):    
    def __init__(self):
        TypeHomeBase.__init__(self, decimal.Decimal)
        self.registerConverter(str, decimal.Decimal)
        self.registerConverter(int, decimal.Decimal)
        self.registerConverter(float, decimalFromFloat)

registry = TypeHomeRegistry()
registry.registerType(IntTypeHome())
registry.registerType(BoolTypeHome())
registry.registerType(FloatTypeHome())
registry.registerType(StrTypeHome())
registry.registerType(UnicodeTypeHome())
registry.registerType(DateTypeHome())
registry.registerType(TimeTypeHome())
registry.registerType(DateTimeTypeHome())
registry.registerType(DecimalTypeHome())

