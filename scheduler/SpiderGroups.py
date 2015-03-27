#coding=utf8
__author__ = 'wanglu'

import whaledataspider.scheduler.SpiderSignals as spiderSignal

from scrapy.utils.project import  get_project_settings
from scrapy.crawler import CrawlerProcess
from scrapy import signals

import multiprocessing as mul

from whaledataspider.spiders import WeiboSpiders
from whaledataspider.spiders import BaiduSpider
from whaledataspider.spiders import NewsSpiders
from whaledataspider.spiders import SougouSpiders
from whaledataspider.spiders import WeixinSpiders


#配置爬虫类，和爬虫需要的参数。
Spider_Dict={
    "weibo_search_group":{
        "start_spider":(WeiboSpiders.WeiboSearchStartSpider,{"isNeedProcessJs":True,"siteName":"weibo_com","isNeedCookie":True,}),
        "filter_spider":(WeiboSpiders.WeiboUrlFilterSpider,{"isNeedProcessJs":True,"siteName":"weibo_com","isNeedCookie":True,}),
        "content_spider":(WeiboSpiders.WeiboComContentSpider,{"isNeedProcessJs":True,"siteName":"weibo_com","isNeedCookie":True,}),
    },
    "baidu_search_sina_news_group":{
        "start_spider":(BaiduSpider.BaiduSearchBySiteKeywordSpider,{"isNeedProcessJs":True,"siteName":"baidu","isNeedCookie":False,}),
        "filter_spider":(BaiduSpider.BaiduSinaNewsFilterSpider,{"isNeedProcessJs":True,"siteName":"baidu","isNeedCookie":False,}),
        "content_spider":(NewsSpiders.SinaNewsContentSpider,{"isNeedProcessJs":False,"siteName":"sina_news","isNeedCookie":False,}),
    },
    "sougou_search_weixin_group":{
        "start_spider":(SougouSpiders.SougouSearchWeixinByKeywordSpider,{"isNeedProcessJs":False,"siteName":"sougou","isNeedCookie":False,}),
        "filter_spider":(SougouSpiders.SougouWeixinFilterSpider,{"isNeedProcessJs":False,"siteName":"sougou","isNeedCookie":False,}),
        "content_spider":(WeixinSpiders.WeixinContentSpider,{"isNeedProcessJs":False,"siteName":"weixin","isNeedCookie":False,}),
    },
    "weibo_userinfo_group":{
        "start_spider":(WeiboSpiders.WeiboComUserInfoStartSpider,{"isNeedProcessJs":False,"siteName":"weibo_com","isNeedCookie":False,}),
        "filter_spider":(WeiboSpiders.WeiboComUserInfoFilterSpider,{"isNeedProcessJs":False,"siteName":"weibo_com","isNeedCookie":False,}),
        "content_spider":(WeiboSpiders.WeiboComUserInfoContentSpider,{"isNeedProcessJs":True,"siteName":"weibo_cn","isNeedCookie":True,}),
    },
}



'''
一个进程调用的爬虫方法。
'''
def startSpider(group_type,spider_type,spider_group_name,spider_name):
    #调用Scrapy内部方法
    settings = get_project_settings()
    #实例化一个爬虫进程
    crawlerProcess = CrawlerProcess(settings)

    #创建一个爬虫，一个爬取处理器可以，运行多个爬取。
    crawler = crawlerProcess.create_crawler(spider_name)

    #设置爬虫的状态。 当爬虫发出该信号后，调用响应的方法。
    crawler.signals.connect(spiderSignal.startSingnal, signals.spider_opened)
    crawler.signals.connect(spiderSignal.idleSingnal, signals.spider_idle)
    crawler.signals.connect(spiderSignal.errorSingnal, signals.spider_error)
    crawler.signals.connect(spiderSignal.stopSingnal, signals.spider_closed)

    #获取爬取类
    spiderConf = Spider_Dict[group_type][spider_type]
    spiderArgs = spiderConf[1].copy()
    spiderArgs["name"] = spider_name
    spiderArgs["redis_key"] = spider_name
    spiderArgs["spider_type"] = spider_type
    spiderArgs["spider_group_name"] = spider_group_name
    spiderArgs["task_id"] = "-1"

    spider = spiderConf[0](**spiderArgs)

    #给爬虫设置爬取类
    crawler.configure()
    crawler.crawl(spider)

    #爬虫启动。
    crawlerProcess.start()
    crawlerProcess.stop()

#启动一个爬虫进程。
def run(group_type,spider_type,spider_group_name,spider_name):
    p = mul.Process(target=startSpider, args=(group_type,spider_type,spider_group_name,spider_name,))
    p.start()



'''
一个进程调用的爬虫方法。
'''
def startSpiderTest(group_type,spider_type,spider_group_name,spider_name):
    #调用Scrapy内部方法
    settings = get_project_settings()
    #实例化一个爬虫进程
    crawlerProcess = CrawlerProcess(settings)

    #创建一个爬虫，一个爬取处理器可以，运行多个爬取。
    crawler = crawlerProcess.create_crawler(spider_name)

    #设置爬虫的状态。 当爬虫发出该信号后，调用响应的方法。
    crawler.signals.connect(spiderSignal.startSingnal, signals.spider_opened)
    crawler.signals.connect(spiderSignal.errorSingnal, signals.spider_error)
    crawler.signals.connect(spiderSignal.stopSingnal, signals.spider_closed)

    #获取爬取类
    spiderConf = Spider_Dict[group_type][spider_type]
    spiderArgs = spiderConf[1].copy()
    spiderArgs["name"] = spider_name
    spiderArgs["redis_key"] = spider_name
    spiderArgs["spider_type"] = spider_type
    spiderArgs["spider_group_name"] = spider_group_name
    spiderArgs["task_id"] = "-1"

    spider = spiderConf[0](**spiderArgs)

    #给爬虫设置爬取类
    crawler.configure()
    crawler.crawl(spider)

    #爬虫启动。
    crawlerProcess.start()
    crawlerProcess.stop()

#启动一个爬虫进程。
def runTest(group_type,spider_type,spider_group_name,spider_name):
    p = mul.Process(target=startSpiderTest, args=(group_type,spider_type,spider_group_name,spider_name,))
    p.start()



if __name__ == "__main__":
    #runTest("baidu_search_sina_news_group","start_spider","baidu_sina","baidu_search")
    #runTest("baidu_search_sina_news_group","filter_spider","baidu_sina","baidu_sina_filter")
    runTest("baidu_search_sina_news_group","content_spider","baidu_sina","sina_content")

    #runTest("sougou_search_weixin_group","start_spider","sougou_weixin","weixin_start")
    #runTest("sougou_search_weixin_group","filter_spider","sougou_weixin","weixin_filter")
    #runTest("sougou_search_weixin_group","content_spider","sougou_weixin","weixin_content")

    #runTest("weibo_userinfo_group","start_spider","weibo_userinfo","weibo_userinfo_start")
    #runTest("weibo_userinfo_group","content_spider","weibo_userinfo","weibo_userinfo_values:userinfo_content")


