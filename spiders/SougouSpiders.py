#coding=utf8
__author__ = 'wanglu'

from whaledataspider.spiders.BaseSpiders import BaseSpider
from whaledataspider.items.WeiboItems import WeiboComItemLoader
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

'''
搜狗搜索微信呢文章
'''
#http://weixin.sogou.com/weixin?query=%E6%B5%8B%E8%AF%95&type=2&page=2&ie=utf8
class SougouSearchWeixinByKeywordSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(SougouSearchWeixinByKeywordSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("sougou.json").get("sougou").get("search_conf")
        self.Wait_Element = self.xpathConf.get("wait_element")


    def next_request(self):
        """Returns a request to be scheduled or none."""
        keyword = self.server.lpop(self.redis_key)

        if keyword:
            self.reRun = keyword[0:1]
            keyword = keyword[1:len(keyword)]
            self.keyword = keyword
            url = self.getSearchUrl(keyword)
            self.baseUrl = url
            return self.make_requests_from_url(url)


    def getSearchUrl(self,keyword):
        #http://weixin.sogou.com/weixin?query=%E6%B5%8B%E8%AF%95&type=2&page=2&ie=utf8
        url = "http://weixin.sogou.com/weixin?query=%s&type=2&page=1&ie=utf8"
        url = url % tuple([urllib.quote(keyword)])

        return url


    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)

        list = hxs.xpath(self.xpathConf.get("search_num"))
        print list
        if len(list)>0:
            search_num = list[0].extract()
            num = int(fo.getBaiduSearchNum(search_num))
            if num/10>80:
                self.saveUrlToRedis(80)
            else:
                self.saveUrlToRedis(num/10)


    def saveUrlToRedis(self,num):
        save_url_key = self.spider_group_name+"-"+"filter"
        baseUrl = "http://weixin.sogou.com/weixin?query=%s&type=2&ie=utf8" % urllib.quote(self.keyword)
        if num>0:
            for p in range(num):
                    url = self.reRun+baseUrl+"&page="+str(p+1)
                    RedisConfUtil.con.lpush(save_url_key,url)
        else:
            url = self.reRun+baseUrl+"&page=1"
            RedisConfUtil.con.lpush(save_url_key,url)



'''
搜狗的微信文章的URL过滤
'''
class SougouWeixinFilterSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(SougouWeixinFilterSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("sougou.json").get("sougou").get("sougou_weixin")
        self.Wait_Element = self.xpathConf.get("wait_element")


    def next_request(self):
        """Returns a request to be scheduled or none."""
        url = self.server.lpop(self.redis_key)

        if url:
             #重跑
            self.reRun = url[0:1]
            url = url[1:len(url)]
            return self.make_requests_from_url(url)


    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)

        for con in hxs.xpath(self.xpathConf.get("parse_xpath")):
            ss = Selector(text=con.extract())
            el = WeiboComItemLoader(selector=ss)
            el.add_value("site_source","mp.weixin.qq.com")
            el.add_value("site_type","weixin")
            el.add_value("task_id",self.task_id)
            nowTime=time.localtime()
            nowDate=datetime.datetime(nowTime[0],nowTime[1],nowTime[2])
            el.add_value("catch_date",nowDate.strftime('%Y-%m-%d'))

            el.add_xpath('site_url',self.xpathConf.get("site_url"))


            yield el.load_item()




