#coding=utf8
__author__ = 'wanglu'

from whaledataspider.spiders.BaseSpiders import BaseTestSpider
from whaledataspider.spiders.BaseSpiders import BaseSpider
from whaledataspider.items.WeiboItems import WeiboComItemLoader
import whaledataspider.util.ConfigUtil as confUtil

import whaledataspider.util.FilterOper as fo
from whaledataspider.util.RedisUtil import RedisConfUtil

import time,datetime
import urllib

from scrapy.selector import Selector
from scrapy import log

import sys

#解决些HTML文件的编码问题。
reload(sys)
sys.setdefaultencoding( "utf-8" )


'''
微博爬虫根据搜索URL获取内容。
'''
class WeiboComSearchSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboComSearchSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("weibo.json").get("weibo_com").get("search_conf")
        self.Wait_Element = self.xpathConf.get("wait_element")

        self.itemKeys.append('attitude')
        self.itemKeys.append('comments')
        self.itemKeys.append('repost')
        self.itemKeys.append('user_url')



    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)
        for con in hxs.xpath(self.xpathConf.get("parse_xpath")):

            ss = Selector(text=con.extract())
            el = WeiboComItemLoader(selector=ss)
            #'site_source','site_type','site_url'
            el.add_value("site_source","weibo.com")
            el.add_value("site_type","weibo")
            el.add_value("task_id",self.task_id)
            nowTime=time.localtime()
            nowDate=datetime.datetime(nowTime[0],nowTime[1],nowTime[2])
            el.add_value("catch_date",nowDate.strftime('%Y-%m-%d'))

            el.add_xpath('author', self.xpathConf.get("author"))
            el.add_xpath('user_url',self.xpathConf.get("user_url"))
            el.add_xpath('site_url',self.xpathConf.get("site_url"))
            el.add_value('content',ss.xpath(self.xpathConf.get("content")).extract())
            el.add_xpath('publish_time',self.xpathConf.get("publish_time"))
            attitude = ss.xpath(self.xpathConf.get("attitude")).extract()

            if attitude:
                el.add_value("attitude",attitude)
            else:
                el.add_value("attitude","0")

            comments = ss.xpath(self.xpathConf.get("comments")).extract()
            if comments:
                el.add_value("comments",comments)
            else:
                el.add_value("comments","0")
            repost = ss.xpath(self.xpathConf.get("repost")).extract()
            if repost:
                el.add_value("repost",repost)
            else:
                el.add_value("repost","0")

            log.msg(repost)
            yield el.load_item()


class WeiboSearchStartSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboSearchStartSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("weibo.json").get("weibo_com").get(self.spider_type)
        self.Wait_Element = self.xpathConf.get("wait_element")

    def next_request(self):
        """Returns a request to be scheduled or none."""
        keyword = self.server.lpop(self.redis_key)

        if keyword:
            self.reRun = keyword[0:1]
            keyword = keyword[1:len(keyword)]
            url = self.getSearchUrl(keyword)
            print(url)
            return self.make_requests_from_url(url)

    def getSearchUrl(self,keyword):
        self.keyword = keyword
        url = "http://s.weibo.com/weibo/%s&b=1&page=1" % urllib.quote(keyword)
        return url

    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)

        list = hxs.xpath(self.xpathConf.get("search_num"))

        if len(list)>0:
            search_num = list[0].extract()
            num = int(fo.getNum(search_num))
            if num/25>50:
                self.saveUrlToRedis(50)
            else:
                self.saveUrlToRedis(num/25)

    def saveUrlToRedis(self,num):
        save_url_key = self.spider_group_name+"-"+"filter"
        if num>0:
            for p in range(num):
                    url = self.reRun+"http://s.weibo.com/weibo/%s&b=1&page=%s" % tuple([urllib.quote(self.keyword),p+1])
                    RedisConfUtil.con.lpush(save_url_key,url)
        else:
            url = self.reRun+"http://s.weibo.com/weibo/%s&b=1&page=%s" % tuple([urllib.quote(self.keyword),1])
            RedisConfUtil.con.lpush(save_url_key,url)



class WeiboUrlFilterSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboUrlFilterSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("weibo.json").get("weibo_com").get(self.spider_type)
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
            el.add_value("site_source","weibo.com")
            el.add_value("site_type","weibo")
            el.add_value("task_id",self.task_id)
            nowTime=time.localtime()
            nowDate=datetime.datetime(nowTime[0],nowTime[1],nowTime[2])
            el.add_value("catch_date",nowDate.strftime('%Y-%m-%d'))


            el.add_xpath('site_url',self.xpathConf.get("site_url"))

            yield el.load_item()


'''
根据微博的URL，爬取微博内容。
'''
class WeiboComContentSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboComContentSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("weibo.json").get("weibo_com").get(self.spider_type)
        self.Wait_Element = self.xpathConf.get("wait_element")

        self.itemKeys.append('attitude')
        self.itemKeys.append('comments')
        self.itemKeys.append('repost')
        self.itemKeys.append('user_url')


    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)

        for con in hxs.xpath(self.xpathConf.get("parse_xpath")):

            ss = Selector(text=con.extract())
            el = WeiboComItemLoader(selector=ss)
            #'site_source','site_type','site_url'
            el.add_value("spider_type",self.spider_type)
            el.add_value("site_source","weibo.com")
            el.add_value("site_type","weibo")
            el.add_value("task_id",self.task_id)
            nowTime=time.localtime()
            nowDate=datetime.datetime(nowTime[0],nowTime[1],nowTime[2])
            el.add_value("catch_date",nowDate.strftime('%Y-%m-%d'))

            url = response.url
            el.add_xpath('author', self.xpathConf.get("author"))
            el.add_value('user_url',url[0:url.rfind('/')])
            el.add_value('site_url',url)
            el.add_value('content',ss.xpath(self.xpathConf.get("content")).extract())
            el.add_xpath('publish_time',self.xpathConf.get("publish_time"))

            attitude = ss.xpath(self.xpathConf.get("attitude")).extract()

            if attitude:
                el.add_value("attitude",attitude)
            else:
                el.add_value("attitude","0")

            comments = ss.xpath(self.xpathConf.get("comments")).extract()
            if comments:
                el.add_value("comments",comments)
            else:
                el.add_value("comments","0")
            repost = ss.xpath(self.xpathConf.get("repost")).extract()
            if repost:
                el.add_value("repost",repost)
            else:
                el.add_value("repost","0")

            log.msg(repost)
            yield el.load_item()



'''
爬取微博账户的用户信息的开始爬虫。
'''
class WeiboComUserInfoStartSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboComUserInfoStartSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

    def next_request(self):
        """Returns a request to be scheduled or none."""
        keyword = self.server.lpop(self.redis_key)

        if keyword:
            self.server.lpush(self.spider_group_name+"-filter",keyword)
            return None

    def parse(self, response):
        pass


'''
爬取微博账户的用户信息的过滤爬虫。
'''
class WeiboComUserInfoFilterSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboComUserInfoFilterSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")


    def next_request(self):
        """Returns a request to be scheduled or none."""
        keyword = self.server.lpop(self.redis_key)

        if keyword:
            self.server.lpush(self.spider_group_name+"-content",keyword)
            return None

    def parse(self, response):
        pass


'''
爬取微博账户的用户信息的内容爬虫。
'''
class WeiboComUserInfoContentSpider(BaseSpider):

    def __init__(self, **kwargs):
        super(WeiboComUserInfoContentSpider,self).__init__(**kwargs)
        self.name = kwargs.get("name")
        self.redis_key = kwargs.get("redis_key")

        #获取微博的XPath配置
        self.xpathConf = confUtil.getJsonStr("weibo.json").get("weibo_cn").get("user_info")
        self.Wait_Element = self.xpathConf.get("wait_element")

    def next_request(self):
        """Returns a request to be scheduled or none."""
        keyword = self.server.lpop(self.redis_key)

    def next_request(self):
        """Returns a request to be scheduled or none."""
        url = self.server.lpop(self.redis_key)

        if url:
             #重跑
             try:
                self.accountNum, url = url.split("|")
                return self.make_requests_from_url(url)
             except:
                 log.msg("WeiboComUserInfoContentSpider url exception :"+url,log.ERROR)

    def parse(self, response):
        #获取一个选择器
        hxs = Selector(response)
        print hxs.extract()
        for con in hxs.xpath(self.xpathConf.get("parse_xpath")):
            s = Selector(text=con.extract())
            r = [self.accountNum]
            imgs = s.xpath(self.xpathConf.get("face_image")).extract()
            if len(imgs)>0:
                r.append(imgs[0])
            else:
                r.append("")

            num_info = s.xpath(self.xpathConf.get("num_info")).extract()


            if len(num_info)>0:
                for num in fo.getWeiboCnUserInfo(num_info[0]):
                    r.append(num)
            else:
                r.append(0)
                r.append(0)
                r.append(0)

            #生成保存Redis的格式
            save_info = "%s|%s|%s|%s|%s" % tuple(r)

            self.server.lpush("weibo_user_watch_values",save_info)






