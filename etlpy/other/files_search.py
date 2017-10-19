import os
path='e:'
allFileNum = 0  
def printPath(level, path):  
    global allFileNum  
    ''''' 
    打印一个目录下的所有文件夹和文件 
    '''  
    # 所有文件夹，第一个字段是次目录的级别  
    dirList = []  
    # 所有文件  
    fileList = []  
    # 返回一个列表，其中包含在目录条目的名称(google翻译)  
    files = os.listdir(path)  
    # 先添加目录级别  
    dirList.append(str(level))  
    for f in files:  
        if(os.path.isdir(path + '/' + f)):  
            # 排除隐藏文件夹。因为隐藏文件夹过多  
            if(f[0] == '.'):  
                pass  
            else:  
                # 添加非隐藏文件夹  
                dirList.append(f)  
        if(os.path.isfile(path + '/' + f)):  
            # 添加文件  
            fileList.append(f)  
    # 当一个标志使用，文件夹列表第一个级别不打印  
    i_dl = 0  
    for dl in dirList:  
        if(i_dl == 0):  
            i_dl = i_dl + 1  
        else:  
            # 打印至控制台，不是第一个的目录  
            print('-' * (int(dirList[0])), dl)
            # 打印目录下的所有文件夹和文件，目录级别+1  
            printPath((int(dirList[0]) + 1), path + '/' + dl)  
    for fl in fileList:  
        # 打印文件  
        print('-' * (int(dirList[0])), fl) 
        # 随便计算一下有多少个文件  
        allFileNum = allFileNum + 1  
#-*- coding: UTF-8 -*- 

'''
1、读取指定目录下的所有文件
2、读取指定文件，输出文件内容
3、创建一个文件并保存到指定目录
'''
import os

# 遍历指定目录，显示目录下的所有文件名
def eachFile(filepath):
    pathDir =  os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('%s%s' % (filepath, allDir))
        print child.decode('gbk') # .decode('gbk')是解决中文显示乱码问题

# 读取文件内容并打印
def readFile(filename):
    fopen = open(filename, 'r') # r 代表read
    for eachLine in fopen:
        print "读取到得内容如下：",eachLine
    fopen.close()
    
# 输入多行文字，写入指定文件并保存到指定文件夹
def writeFile(filename):
    fopen = open(filename, 'w')
    print "\r请任意输入多行文字"," ( 输入 .号回车保存)"
    while True:
        aLine = raw_input()
        if aLine != ".":
            fopen.write('%s%s' % (aLine, os.linesep))
        else:
            print "文件已保存!"
            break
    fopen.close()

if __name__ == '__main__':
    filePath = "D:\\FileDemo\\Java\\myJava.txt"
    filePathI = "D:\\FileDemo\\Python\\pt.py"
    filePathC = "C:\\"
    eachFile(filePathC)
    readFile(filePath)
    writeFile(filePathI)  
if __name__ == '__main__':  
    printPath(1, path)  
    print('总文件数 =', allFileNum)