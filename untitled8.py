import random
import time
import requests
from multiprocessing import Pool
def down(url):
    print('ter')
    s=requests.Session()
    res=s.get(url)
    a=random.randint(0,40)
    f=open('%d.jpg'%a,'wb')
    f.write(res.content)
    
def main():    
    f=open('E:\\test.txt','r')
    a=f.readlines()
    po=Pool(3)
    for i in a:
        po.apply_async(down,(i,))
    po.close()#进程池一定要先关在join
    po.join()
if __name__=='__main__':
    s=time.time()
    main()
    e=time.time()
    print(e-s)