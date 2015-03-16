#coding=utf8
__author__ = 'wanglu'

from scrapy_redis.spiders import RedisSpider
from scrapy import signals, log
from scrapy.spider import Spider

import whaledataspider.util.CookieGen as cookieGen
import whaledataspider.settings as conf

from selenium import webdriver

import os
import pickle

'''
实际从Redis读取StartURL的Spider
'''
class BaseSpider(RedisSpider):

    #下载中间件使用属性
    isNeedCookie = False
    isNeedProcessJs = False
    cookieFile = None
    siteName = None

    #selenium driver
    dr = None
    Wait_Element = None

    #设置开始url变量数组。否则继承此类的爬虫会报错。
    start_urls = []

    #转内容的基本字段名称
    itemKeys = ['publish_time','site_source','site_type','site_url','task_id','content','author','catch_date']


    def __init__(self, **kargs):
        self.isNeedCookie = kargs.get("isNeedCookie",False)
        self.isNeedProcessJs = kargs.get("isNeedProcessJs",False)
        self.cookieFile = kargs.get("cookieFile",None)
        self.siteName = kargs.get("siteName", None)
        self.cookieFileName = kargs.get("name", "No").replace(":","_")

        self.spider_type = kargs.get("spider_type",None)
        self.spider_group_name = kargs.get("spider_group_name",None)
        self.task_id = kargs.get("task_id","-1")


    def parse(self, response):
        pass

    #重载下面方法，设置信号处理方法。
    def setup_redis(self):
        #调用父类方法
        super(BaseSpider, self).setup_redis()
        #注册事件
        self.crawler.signals.connect(self.engine_start, signal=signals.engine_started)
        self.crawler.signals.connect(self.engine_stop, signal=signals.engine_stopped)

    #引擎启动时，执行的方法。主要判断爬虫是否启动模拟器。
    def engine_start(self):
        log.msg("爬取引擎启动",log.INFO)
        if self.isNeedProcessJs:
            log.msg("启动模拟器")
            self.startSelenium()

    #当爬虫引擎关闭时，如果有模拟器，则停止模拟器。
    def engine_stop(self):
        log.msg("爬取引擎关闭",log.INFO)
        self.__stop_dr()

    #关闭模拟器。
    def __stop_dr(self):
        if self.dr:
            self.dr.start_client()
            self.dr.close()
            self.dr.quit()


    #启动模拟器
    def startSelenium(self):
        if conf.IS_NEED_AGENT:
            self.dr = webdriver.PhantomJS(service_args=conf.service_args,desired_capabilities=conf.dcap)
        else:
            self.dr = webdriver.PhantomJS()


        if self.isNeedCookie:
            cookieFile = cookieGen.getCookieFile(self.cookieFileName)
            #判断Cookie文件是否存在。
            if not os.path.exists(cookieFile):
                cookieFile = cookieGen.getCookie(self.siteName,self.cookieFileName)
                cookies = pickle.load(open(cookieFile,"rb"))
                for cookie in cookies:
                    self.dr.add_cookie(cookie)
            else:
                cookies = pickle.load(open(cookieFile,"rb"))
                for cookie in cookies:
                    self.dr.add_cookie(cookie)

        self.dr.implicitly_wait(conf.PhantomJS_Wait)



'''
用来进行测试的基础Spider
'''
class BaseTestSpider(Spider):

    #下载中间件使用属性
    isNeedCookie = False
    isNeedProcessJs = False
    cookieFile = None
    siteName = None

    #selenium driver
    dr = None

    def __init__(self, **kargs):
        self.isNeedCookie = kargs.get("isNeedCookie",False)
        self.isNeedProcessJs = kargs.get("isNeedProcessJs",False)
        self.cookieFile = kargs.get("cookieFile",None)
        self.siteName = kargs.get("siteName", None)

    def set_crawler(self, crawler):
        log.msg("加载爬虫",log.INFO)
        super(BaseTestSpider,self).set_crawler(crawler)
        self.register_driver()



    def parse(self, response):
        pass

    #重载下面方法，设置信号处理方法。
    def register_driver(self):
        #注册事件
        self.crawler.signals.connect(self.engine_start, signal=signals.engine_started)
        self.crawler.signals.connect(self.engine_stop, signal=signals.engine_stopped)

     #引擎启动时，执行的方法。主要判断爬虫是否启动模拟器。
    def engine_start(self):
        log.msg("爬取引擎启动",log.INFO)
        if self.isNeedProcessJs:
            log.msg("启动模拟器")
            self.startSelenium()

    #当爬虫引擎关闭时，如果有模拟器，则停止模拟器。
    def engine_stop(self):
        log.msg("爬取引擎关闭",log.INFO)
        self.__stop_dr()

    #关闭模拟器。
    def __stop_dr(self):
        if self.dr:
            self.dr.start_client()
            self.dr.close()
            self.dr.quit()


    #启动模拟器
    def startSelenium(self):
        if conf.IS_NEED_AGENT:
            self.dr = webdriver.PhantomJS(service_args=conf.service_args,desired_capabilities=conf.dcap)
        else:
            self.dr = webdriver.PhantomJS(desired_capabilities=conf.dcap)


        if self.isNeedCookie:
            cookieFile = cookieGen.getCookieFile(self.co)
            #判断Cookie文件是否存在。
            if not os.path.exists(cookieFile):
                cookieFile = cookieGen.getCookie(self.siteName)
                cookies = pickle.load(open(cookieFile,"rb"))
                for cookie in cookies:
                    self.dr.add_cookie(cookie)
            else:
                cookies = pickle.load(open(cookieFile,"rb"))
                for cookie in cookies:
                    self.dr.add_cookie(cookie)

        self.dr.implicitly_wait(conf.PhantomJS_Wait)

