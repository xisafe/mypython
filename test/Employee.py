__author__ = 'cardvalue'
from com.newbee import EmployeeType
class Employee(EmployeeType):
    def __init__(self):
        self.first = "Sean"
        self.last  = "Guo"
        self.id = "731"
    def getEmployeeFirst(self):
        return self.first
    def getEmployeeLast(self):
        return self.last
    def getEmployeeId(self):
        return self.id

    def setEmployeeId(self, newId):
        self.id = newId

def getNunmberValue(seed):
    v0 = 0
    for i in range(1,seed+1):
        v0 +=i
    return v0