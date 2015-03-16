#coding=utf8
__author__ = 'wanglu'

from selenium import webdriver
import ConfigUtil as confUtil
import json
import whaledataspider.settings as conf
import pickle

import sys

#解决些HTML文件的编码问题。
reload(sys)
sys.setdefaultencoding( "utf-8" )


'''
通用的获取Cookie的方法。
从配置文件中读取要设置的变量,user.json 该配置文件
中保存了获取cookie的用户名和密码，页面输入信息和登录配置。
将Cookie保存到文件中。
'''
def getCookie(siteName,cookieFileName):

    #从配置文件中获取登录微博的URL和用户名、密码。
    userJson = confUtil.getJsonStr("user.json")
    weibo_com = userJson.get(siteName)
    login_url = weibo_com.get("login_url")
    username = weibo_com.get("username")
    password = weibo_com.get("password")
    username_xpath=weibo_com.get("username_xpath")
    password_xpath=weibo_com.get("password_xpath")
    submit_xpath=weibo_com.get("submit_xpath")

    driver = webdriver.PhantomJS(service_args=conf.service_args,desired_capabilities=conf.dcap)
    driver.get(login_url)

    #添加等待，等待元素都完成加载。
    driver.implicitly_wait(10)

    #输入用户名和密码
    driver.find_element_by_xpath(username_xpath).send_keys(username)
    driver.find_element_by_xpath(password_xpath).send_keys(password)
    #提交
    driver.find_element_by_xpath(submit_xpath).click()

    #保存Cookie对象。
    cookiePkl=getCookieFile(cookieFileName)
    pickle.dump(driver.get_cookies(),open(cookiePkl,"wb"))

    driver.start_client()
    driver.close()
    driver.quit()
    #返回cookie的存放文件路径。
    return cookiePkl


'''
获取Cookie文件路径。
'''
def getCookieFile(siteName):
    return  "%s/cookie/%s_cookie.pkl" % (conf.CONFIG_PATH,siteName)
'''
测试生成的Cookie文件是否有效。
'''
def testCookieFile(siteName):
    login_url='http://s.weibo.com/weibo/%25E6%25B5%258B%25E8%25AF%2595?topnav=1&wvr=6&b=1'
    cookieFile=getCookieFile(siteName)

    driver = webdriver.PhantomJS(service_args=conf.service_args,desired_capabilities=conf.dcap)

    cookies = pickle.load(open(cookieFile,"rb"))
    for cookie in cookies:
        driver.add_cookie(cookie)
    driver.get(login_url)

    #添加等待，等待元素都完成加载。
    driver.implicitly_wait(10)

    driver.find_element_by_xpath(conf.Wait_Element)


    with open("%s/cookie/%s_page.html" % (conf.CONFIG_PATH,siteName),'wb') as p:
        p.write(driver.page_source.encode("utf-8"))
    driver.close()


if __name__ == "__main__":
    #生成Cookie
    filePath=getCookie("weibo_com","test_filter_spider_234374000")
    print(filePath)
    #获取页面内容。
    #testCookieFile('weibo_com')

