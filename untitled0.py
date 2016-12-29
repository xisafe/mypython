#!/usr/bin/env python2
#-*-encoding:utf-8-*-
import os
import sys

import os.path
def getAllFiles(rootPath):
    fileList=[]
    for root,dirs,files in os.walk(rootPath):
        for file in files:
            fileList.append((file,os.path.join(root,file)))
    return fileList
if __name__ == '__main__':
    t= getAllFiles(r'e:\myfirst')
    os.remove('e:\myfirst\popup.html')