#coding=utf8
__author__ = 'wanglu'

import os
from whaledataspider.util.RedisUtil import RedisConfUtil

import whaledataspider.util.RedisUtil as redisKey
import json


'''
当爬虫启动时，汇报状态
'''
def startSingnal(spider):
    print '爬虫:',spider.name,"启动"

'''
当爬虫空闲时，汇报状态
'''
def idleSingnal(spider):
    print '爬虫:',spider.name,"空闲"
    print(os.getpid())
    if(RedisConfUtil.spiderStop(spider.name)):
        spider.crawler.stop()
    else:
        spider_status = json.loads(RedisConfUtil.con.hget(redisKey.spider_status,spider.name))
        spider_status["status"] = "2"
        spider_status["update_date"] = RedisConfUtil.getUpdateTime()
        RedisConfUtil.con.hset(redisKey.spider_status,spider.name,json.dumps(spider_status))


'''
当爬虫停止时，汇报状态
'''
def stopSingnal(spider):
    print '爬虫:',spider.name,"停止"
    #删除配置。
    RedisConfUtil.con.lpush(redisKey.remove_spider_conf,spider.name)

'''
当爬虫异常时，汇报状态
'''
def errorSingnal(spider):
    print '爬虫:',spider.name,"异常"





