#coding=utf8
__author__ = 'wanglu'

from whaledataspider.spiders import WeiboSpiders

import datetime
import multiprocessing as mul
import whaledataspider.util.ConfigUtil as confUtil

from whaledataspider.util.RedisUtil import RedisConfUtil
import whaledataspider.scheduler.SpiderGroups as spiderGroup
import json
import time



import sys

reload(sys)
sys.setdefaultencoding("utf-8")



#定义机器上爬虫的管理。
class HostSpiderManager():

    def __init__(self):
        self.local_ip = confUtil.getLocalIp()
        self.redisConfUtil = RedisConfUtil()

    #启动爬虫
    def startSpider(self):
        hostSpiders = self.redisConfUtil.getHostSpiderConf(self.local_ip)
        if hostSpiders and len(hostSpiders)>0:
            for spider in hostSpiders:
                spiderConf = json.loads(spider)
                if spiderConf.get("status") == "0":
                    spiderGroup.run(spiderConf["spider_group_type"],spiderConf["spider_type"],spiderConf["spider_group_name"],spiderConf["spider_name"])
                    spiderConf["status"] = "1"
                    self.redisConfUtil.setSpiderStatusConf(spiderConf["spider_name"],json.dumps(spiderConf))

    #停止爬虫
    def stopSpider(self):
        pass

    #重启爬虫
    def reStartSpider(self):
        pass


    #向Redis汇报自己的状态
    def reportStatus(self,status):
        oldStatus = self.redisConfUtil.getSpiderHostConf(self.local_ip)
        if oldStatus:
            conf = json.loads(oldStatus)
            conf["status"] = status
            conf["update_date"] = self.redisConfUtil.getUpdateTime()
            strConf = json.dumps(conf)
            self.redisConfUtil.setSpiderHostConf(self.local_ip,strConf)
        else:
            strConf = '{"status":"%s","update_date":"%s"}' % tuple([status,self.redisConfUtil.getUpdateTime()])
            self.redisConfUtil.setSpiderHostConf(self.local_ip,strConf)


    #停止该机器爬虫调度，会关闭该机器上的所有爬虫。
    def stop(self):
        pass


    def changeHostSpiderStatus(self):
        hostSpiders = self.redisConfUtil.getHostSpiderConf(self.local_ip)
        if hostSpiders and len(hostSpiders)>0:
            for spider in hostSpiders:
                spiderConf = json.loads(spider)
                spiderConf["status"] = "0"
                self.redisConfUtil.setSpiderStatusConf(spiderConf["spider_name"],json.dumps(spiderConf))



if __name__ == '__main__':
    hostSpiderMag = HostSpiderManager()
    hostSpiderMag.changeHostSpiderStatus()
    while True:
        hostSpiderMag.reportStatus('1')
        hostSpiderMag.startSpider()
        time.sleep(3)


