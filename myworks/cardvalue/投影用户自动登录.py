# encoding=GBK
import os
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium import webdriver
print 'ͶӰ���ڿ����У��������-----------------------'
chrome_driver = os.path.abspath("C:/chromedriver.exe")
#os.system('"C:/Program Files (x86)/Google/Chrome/Application/chrome.exe" http://192.168.0.98:9000/cv')
os.environ["webdriver.chrome.driver"] = chrome_driver
option=webdriver.ChromeOptions()
option.add_argument('-test-type')  # ȥ��Chrome������ϵĻ�ɫ����
driver = webdriver.Chrome(chrome_driver,chrome_options=option)
driver.get("http://192.168.0.98:9000/cv/login")
driver.maximize_window()
user=driver.find_element_by_id("user")
user.send_keys("touying")
paw=driver.find_element_by_id("password")
paw.send_keys("touying")
driver.find_element_by_id("ctrls-container").click() #�����¼
time.sleep(10)
driver.find_element_by_id("btn-rpt-run").send_keys(Keys.F11) #ȫ���鿴

