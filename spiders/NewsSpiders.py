#coding=utf8
__author__ = 'wanglu'

from whaledataspider.spiders.BaseSpiders import BaseSpider
from whaledataspider.items.NewsItems import SinaNewsItemLoader
import whaledataspider.util.ConfigUtil as confUtil

import whaledataspider.util.FilterOper as fo
from whaledataspider.util.RedisUtil import RedisConfUtil
import whaledataspider.util.URLUtil as urlUtil

import time,datetime
import urllib

from scrapy.selector import Selector
from scrapy import log
import urllib2
import re

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class SinaNewsContentSpider(BaseSpider):

    def __init__(self,**kwargs):
        super(SinaNewsContentSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("news.json").get(self.siteName).get(self.spider_type)
        self.Wait_Element = self.xpathConf.get("wait_element")

        self.itemKeys.append("title")

    def parse(self, response):

        hxs = Selector(response)

        for con in hxs.xpath(self.xpathConf.get("parse_xpath")):
            ss = Selector(text=con.extract())
            el = SinaNewsItemLoader(selector=ss)

            el.add_value("spider_type",self.spider_type)
            el.add_value("site_source","news.sina.com.cn")
            el.add_value("site_type","news")
            el.add_value("task_id",self.task_id)
            nowTime=time.localtime()
            nowDate=datetime.datetime(nowTime[0],nowTime[1],nowTime[2])
            el.add_value("catch_date",nowDate.strftime('%Y-%m-%d'))

            url = response.url
            el.add_xpath('author', self.xpathConf.get("author"))
            el.add_value('site_url',url)
            el.add_xpath('title', self.xpathConf.get("title"))
            el.add_value('content',ss.xpath(self.xpathConf.get("content")).extract())
            el.add_xpath('publish_time',self.xpathConf.get("publish_time"))

            yield el.load_item()





