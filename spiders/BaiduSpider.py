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
百度的高级搜索功能，过去搜去的URL。
'''

class BaiduSearchBySiteKeywordSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(BaiduSearchBySiteKeywordSpider,self).__init__(**kwargs)
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
            url = self.getSearchUrl(keyword)
            self.baseUrl = url
            return self.make_requests_from_url(url)

    def getSearchUrl(self,keyword):
        #&pn=0http://www.baidu.com/s?q1=%E4%B8%A4%E4%BC%9A+2015+%E9%80%80%E4%BC%91%E5%B9%B4%E9%BE%84&q2=&q3=&q4=&rn=10&lm=0&ct=0&ft=&q5=&q6=news.sina.com.cn&tn=baiduadv
        url = "http://www.baidu.com/s?q1="
        count = 0
        site = "=&q3=&q4=&rn=50&lm=0&ct=0&ft=&q5=&q6=%s&tn=baiduadv"
        for key in keyword.split("+"):
            if count==0:
                site = site % key
            else:
                if count ==1:
                    url = "%s%s" % tuple([url,urllib.quote(key)])
                else:
                    url ="%s+%s"  % tuple([url,urllib.quote(key)])
            count = count+1;

        return url+site


    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)

        list = hxs.xpath(self.xpathConf.get("search_num"))

        if len(list)>0:
            search_num = list[0].extract()
            num = int(fo.getBaiduSearchNum(search_num))
            if num/50>50:
                self.saveUrlToRedis(50)
            else:
                self.saveUrlToRedis(num/50)


    def saveUrlToRedis(self,num):
        save_url_key = self.spider_group_name+"-"+"filter"
        if num>0:
            for p in range(num):
                    url = self.reRun+self.baseUrl+"&pn="+str(p*50)
                    RedisConfUtil.con.lpush(save_url_key,url)
        else:
            url = self.reRun+self.baseUrl+"&pn=0"
            RedisConfUtil.con.lpush(save_url_key,url)




class BaiduSinaNewsFilterSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(BaiduSinaNewsFilterSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("baidu.json").get("baidu").get("sina_news")
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
            el.add_value("site_source","news.sina.com.cn")
            el.add_value("site_type","news")
            el.add_value("task_id",self.task_id)
            nowTime=time.localtime()
            nowDate=datetime.datetime(nowTime[0],nowTime[1],nowTime[2])
            el.add_value("catch_date",nowDate.strftime('%Y-%m-%d'))

            baidu_url = ss.xpath(self.xpathConf.get("site_url")).extract()
            if baidu_url and len(baidu_url)>0:
                source_url = urlUtil.getRedirectUrl(baidu_url[0],timeout=10)
                sinaUrl = fo.findSinaNewsUrl(source_url)
                if sinaUrl:
                    el.add_value('site_url',sinaUrl)
                else:
                    continue
            else:
                continue


            yield el.load_item()