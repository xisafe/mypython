# encoding: utf-8
# -*- coding:utf-8 -*-

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

wd = webdriver.Firefox() #Chrome()  #
wd.get("http://192.168.0.98:9000/cv/login")
wd.maximize_window()

try:
    """这段可以查看selenium的源码,属于smart wait"""
    email = WebDriverWait(wd,timeout=10).until(EC.presence_of_element_located((By.ID,'user')),message=u'元素加载超时!')
    email.send_keys("test")
    passwd = WebDriverWait(wd,timeout=10).until(EC.presence_of_element_located((By.ID,'password')),message=u'元素加载超时!')
    passwd.send_keys("test")
    wd.find_element_by_id("ctrls-container").click() #点击登录
except NoSuchElementException as e:
    print e.message