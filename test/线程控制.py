#!/usr/bin/python
#coding:utf-8
import threading
import datetime
import logging
import time
#接着用Semaphore去控制线程数：
logging.basicConfig(level = logging.DEBUG,format='(%(threadName)-10s) %(message)s',)
list = ['192.168.1.1','192.168.1.2']
class Test(threading.Thread):
    def __init__(self,threadingSum, ip):
        threading.Thread.__init__(self)
        self.ip = ip
        self.threadingSum = threadingSum

    def run(self):
        with self.threadingSum:
            logging.debug("%s start!" % self.ip)
            time.sleep(5)
            logging.debug('%s Done!' % self.ip)


#if __name__ == "__main__":
    #设置线程数
threadingSum = threading.Semaphore(1)

    #启动线程
for ip in list:
    t = Test(threadingSum,ip)
    print 'start:'+ ip
    t.start()
    #等待所有线程结束
for t in threading.enumerate():
    if t is threading.currentThread():
        continue
    t.join()
logging.debug('Done!')