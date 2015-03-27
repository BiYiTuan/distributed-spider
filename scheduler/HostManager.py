#coding=utf8
__author__ = 'wanglu'

from whaledataspider.scheduler import SpiderGroups as spiderGroup
from whaledataspider.util.RedisUtil import RedisConfUtil
import whaledataspider.util.RedisUtil as redisKey

import json
import time

import threading

'''
根据前台配置，管理机器上的爬虫。
'''
class HostSpiderManager():

    def __init__(self):
        self.redisConfUtil = RedisConfUtil()

    #向Redis同步爬虫类型配置。
    def reportSpiderType(self):
        self.redisConfUtil.setSpiderGroupTypes(spiderGroup.Spider_Dict.keys())

    #设置启动爬虫配置。
    def setStartSpiderConf(self,host_ip,spider_group_name,spider_group_type,spider_type,spider_name):
        spiderConf = '{"spider_group_name":"%s","spider_name":"%s","spider_group_type":"%s","spider_type":"%s","update_date":"%s","status":"0","host_ip":"%s"}' \
        % tuple([spider_group_name,spider_name,spider_group_type,spider_type,self.redisConfUtil.getUpdateTime(),host_ip])
        hostConfStr = self.redisConfUtil.getSpiderHostConf(host_ip)

        hostConf = json.loads(hostConfStr)
        spiders_name = hostConf.get("spiders_name")
        if spiders_name:
            spiders_name = "%s%s," % tuple([spiders_name,spider_name])
        else :
            spiders_name = '%s,' % spider_name

        hostConf["spiders_name"] = spiders_name

        self.redisConfUtil.setSpiderStatusConf(spider_name,spiderConf)
        self.redisConfUtil.setSpiderHostConf(host_ip,json.dumps(hostConf))

        self.addGroupStatus(spider_group_name,spider_type,spider_name,host_ip)


    #更新爬虫族的爬虫状态
    def addGroupStatus(self,spider_group_name,spider_type,spider_name,host_ip):
        statusS = self.redisConfUtil.getSpiderGroupStatus(spider_group_name)
        if statusS:
            conf = json.loads(statusS)
            if conf.get(spider_type):
                conf[spider_type] = '%s%s,' % tuple([conf[spider_type],spider_name+":"+host_ip])
            else:
                conf[spider_type] = '%s,' % tuple([spider_name+":"+host_ip])

            conf["update_date"] = self.redisConfUtil.getUpdateTime()
            self.redisConfUtil.setSpiderGroupStatus(spider_group_name,json.dumps(conf))
        else:
            conf = '{"%s":"%s","update_date":"%s"}' % tuple([spider_type,spider_name+":"+host_ip+",",self.redisConfUtil.getUpdateTime()])
            self.redisConfUtil.setSpiderGroupStatus(spider_group_name,conf)


    #启动一个爬虫组。 每个类型爬虫先启动一个。
    def addGroupStart(self,spider_group_name):
        spider_types = ["start_spider","filter_spider","content_spider"]
        #获取该爬虫族的的爬虫族类型
        if self.redisConfUtil.getSpiderGroupConf(spider_group_name):
            spider_group_type = json.loads(self.redisConfUtil.getSpiderGroupConf(spider_group_name)).get("spider_group_type")
            for spider_type in spider_types:
                host_ip = self.getSpiderStartHostIp(spider_group_name)
                if host_ip:
                    spider_name = self.getSpiderName(spider_group_name,spider_type)
                    self.setStartSpiderConf(host_ip,spider_group_name,spider_group_type,spider_type,spider_name)

    #开启一个爬虫配置。
    def addSpiderStart(self,spider_group_name,spider_group_type,spider_type):
        #获取该爬虫族的的爬虫族类型
        host_ip = self.getSpiderStartHostIp(spider_group_name)
        if host_ip:
            spider_name = self.getSpiderName(spider_group_name,spider_type)
            self.setStartSpiderConf(host_ip,spider_group_name,spider_group_type,spider_type,spider_name)

    #获取爬虫启动机器的IP地址。
    def getSpiderStartHostIp(self,spider_group_name):
        #返回结果
        result = None
        groupConfS = self.redisConfUtil.getSpiderGroupConf(spider_group_name)
        groupConf = json.loads(groupConfS)
        hosts = []
        if groupConf.get("spider_host"):
            for h in groupConf["spider_host"].split(','):
                hosts.append(h)

        if len(hosts)==0:
            hosts = self.redisConfUtil.getSpiderHost()

        if len(hosts)==1 :
            hostConfS =self.redisConfUtil.getSpiderHostConf(hosts[0])
            if hostConfS:
                hostConf = json.loads(hostConfS)
                if hostConf["status"] == "1" :
                    result = hosts[0]
        else:
            spiderNum =10000
            for h in hosts:
                n = 0
                hostConfS =self.redisConfUtil.getSpiderHostConf(h)
                hostConf = json.loads(hostConfS)
                if hostConf.get("spiders_name") and hostConf["status"] == "1":
                    n = hostConf["spiders_name"].split(',')

                if n<spiderNum:
                    spiderNum = n
                    result = h

        return  result


    #获取一个随机的爬虫名称
    def getSpiderName(self,spider_group_name,spider_type):
        return spider_group_name+":"+spider_type+":"+self.redisConfUtil.getUpdateTime()


    #判断是否有新的爬虫族加入
    def moniterNewGroupAdd(self):
        confKeys = self.redisConfUtil.getSpiderGroupConfKey()
        statusKeys = self.redisConfUtil.getSpiderGroupStatusKey()

        for key in confKeys:
            flag = True
            for skey in statusKeys:
                if key == skey:
                    flag = False
            if flag:
                self.addGroupStart(key)


    #监控爬虫的状态。当30秒未更新机器状态，将机器状态设置为停机。 当30分钟没有更改机器状态，将机器设置为死机，关闭所有爬虫配置。
    def moniterHostStatus(self):

        #获取机器所有爬虫机器IP
        host_ips = self.redisConfUtil.con.hkeys(redisKey.spider_host)
        if host_ips and len(host_ips)>0:
            for host_ip in host_ips:
                hostConf = json.loads(self.redisConfUtil.getSpiderHostConf(host_ip))
                oldUpdateDate = int(hostConf["update_date"])
                curr = int(self.redisConfUtil.getUpdateTime())
                if (curr-oldUpdateDate)/1000>30*60:
                    for spider_name in hostConf["spiders_name"].split(","):
                        self._removeSpiderConf(spider_name)

                    self.redisConfUtil.con.hdel(redisKey.spider_host,host_ip)

                if (curr-oldUpdateDate)/1000>30:
                    hostConf["status"] = "-1"
                    self.redisConfUtil.setSpiderHostConf(host_ip,json.dumps(hostConf))



    #删除爬虫的配置。由爬虫汇报要删除爬虫名。 redisKey = removeSpiderConf
    def moniterStopSpider(self):

        length = self.redisConfUtil.con.llen(redisKey.remove_spider_conf)
        if length>0:
            for i in range(length):
                spider_name = self.redisConfUtil.con.rpop(redisKey.remove_spider_conf)
                self._removeSpiderConf(spider_name)


    #监控爬虫族中的爬虫状态数量，动态的启动和停止爬虫。
    def moniterSpiderGroup(self):

        limit =100

        #获取每个爬虫族的配置和filter爬虫和content爬虫的数量 ，当前URL池中URL的数量。超过100则新增一个爬虫。一直到增加到最大值。
        spider_group_names = self.redisConfUtil.con.hkeys(redisKey.spider_group_status)

        if spider_group_names and len(spider_group_names)>0:
            for spider_group_name in spider_group_names:
                #获取filter
                #获取该爬虫族的数量配置
                spiderGroupConf = json.loads(self.redisConfUtil.getSpiderGroupConf(spider_group_name))
                max_filter = int(spiderGroupConf["filter_spider_num"])
                max_content = int(spiderGroupConf["content_spider_num"])
                spider_group_type = spiderGroupConf["spider_group_type"]
                #获取爬虫族状态配置
                #校验爬虫现在运行的状态与Host和Group的状态是否一致。
                self.checkSpiderStatus()
                run_filter=0
                run_content=0
                run_start=0
                if self.redisConfUtil.getSpiderGroupStatus(spider_group_name):
                    spiderGroupStatus= json.loads(self.redisConfUtil.getSpiderGroupStatus(spider_group_name))
                    run_filter = self._getCount(spiderGroupStatus["filter_spider"])
                    run_content = self._getCount(spiderGroupStatus["content_spider"])
                    run_start = self._getCount(spiderGroupStatus["start_spider"])

                #当没有filter或content时，启动一个。
                hasAdd = False
                if run_start==0:
                    self.addSpiderStart(spider_group_name,spider_group_type,"start_spider")
                    hasAdd=True
                if run_filter==0:
                    self.addSpiderStart(spider_group_name,spider_group_type,"filter_spider")
                    hasAdd=True
                if run_content==0:
                    self.addSpiderStart(spider_group_name,spider_group_type,"content_spider")
                    hasAdd=True

                if hasAdd:
                    continue

                #获取URL池的连接数
                pool_filter = self.redisConfUtil.con.llen(spider_group_name+"-filter")
                pool_content = self.redisConfUtil.con.llen(spider_group_name+"-content")

                #判断是否需要停止或关闭一个爬虫。
                if run_filter*limit<pool_filter:
                    if run_filter<max_filter:
                        self.addSpiderStart(spider_group_name,spider_group_type,"filter_spider")
                else:
                    if run_filter>1 and pool_filter<50:
                        spider_name= spiderGroupStatus["filter_spider"].split(",")[0]
                        spider_name = spider_name[0:spider_name.rindex(":")]
                        self.stopSpider(spider_group_name,spider_name,"filter_spider")

                if run_content*limit<pool_content:
                    if run_content<max_content:
                        self.addSpiderStart(spider_group_name,spider_group_type,"content_spider")
                else:
                    if run_content>1 and pool_content<50 :
                        spider_name= spiderGroupStatus["content_spider"].split(",")[0]
                        spider_name = spider_name[0:spider_name.rindex(":")]
                        self.stopSpider(spider_group_name,spider_name,"content_spider")




    #检查爬虫的状态和爬虫族中是否一致。 以spider_status的状态为准
    def checkSpiderStatus(self):
        group_result = {}
        host_result = {}
        allSpider = self.redisConfUtil.con.hgetall(redisKey.spider_status)
        for spider in allSpider:
            spiderConf = json.loads(allSpider[spider])
            host_ip = spiderConf["host_ip"]
            spider_name = spiderConf["spider_name"]
            spider_type = spiderConf["spider_type"]
            spider_group_name = spiderConf["spider_group_name"]
            if host_result.get(host_ip):
                host_result[host_ip]="%s%s," % tuple([host_result[host_ip],spider_name])
            else:
                host_result[host_ip]="%s," % tuple([spider_name])

            if group_result.get(spider_group_name):
                if group_result.get(spider_group_name).get(spider_type):
                    group_result[spider_group_name][spider_type] = "%s%s," % tuple([group_result[spider_group_name][spider_type],spider_name+":"+host_ip])
                else:
                    group_result[spider_group_name][spider_type] = "%s," % tuple([spider_name+":"+host_ip])

            else:
                sub = {}
                sub[spider_type] = "%s," % tuple([spider_name+":"+host_ip])
                group_result[spider_group_name] = sub


        allGroup = self.redisConfUtil.con.hgetall(redisKey.spider_group_status)
        for group in allGroup:
            groupStatus = json.loads(allGroup[group])
            if group_result.get(group):

                for type in ["start_spider","filter_spider","content_spider"]:
                    if group_result[group].get(type):
                        groupStatus[type] = group_result[group][type]
                    else:
                        groupStatus[type]=""


                self.redisConfUtil.con.hset(redisKey.spider_group_status,group,json.dumps(groupStatus))

            else:
                self.redisConfUtil.con.hdel(redisKey.spider_group_status,group)


        allHost = self.redisConfUtil.con.hgetall(redisKey.spider_host)
        for host in allHost:
            hostStatus = json.loads(allHost[host])

            if host_result.get(host):
                hostStatus["spiders_name"] = host_result[host]
                self.redisConfUtil.con.hset(redisKey.spider_host,host,json.dumps(hostStatus))

            else:
                self.redisConfUtil.con.hdel(redisKey.spider_host,host)


    #停止一个爬虫的操作。
    def stopSpider(self,spider_group_name,spider_name,spider_type):
        #将爬虫未爬取的URL，返回到URL池中。
        if spider_type == "filter_spider":
            for i in range(self.redisConfUtil.con.llen(spider_name)):
                val = self.redisConfUtil.con.rpop(spider_name)
                if val:
                    self.redisConfUtil.con.lpush(spider_group_name+"-filter",val)

        if spider_type == "content_spider":
            for i in range(self.redisConfUtil.con.llen(spider_name)):
                val = self.redisConfUtil.con.rpop(spider_name)
                if val:
                    self.redisConfUtil.con.lpush(spider_group_name+"-content",val)
        #情况爬虫的配置
        self._removeSpiderConf(spider_name)




    #判断是否要求启动或停止爬虫
    def _getCount(self,val):
        count =0
        for s in val.split(","):
            if s:
                count= count+1

        return count


    #删除一个爬虫配置
    def _removeSpiderConf(self,spider_name):
        #获取爬虫的状态信息。
        spider_conf = self.redisConfUtil.getSpiderConf(spider_name)
        if spider_conf:
            spiderConf = json.loads(self.redisConfUtil.getSpiderConf(spider_name))
            spider_group_name = spiderConf["spider_group_name"]
            spider_type = spiderConf["spider_type"]
            host_ip = spiderConf["host_ip"]
            #更新spider_group_status
            spider_group_status_conf = self.redisConfUtil.getSpiderGroupStatus(spider_group_name)
            if spider_group_status_conf:
                spider_group_status = json.loads(spider_group_status_conf)
                spiders = spider_group_status[spider_type]
                news = ""
                for s in spiders.split(","):
                    if s:
                        if spider_name != s[0:s.rindex(":")]:
                            news = "%s%s," % tuple([news,s])

                spider_group_status[spider_type] = news

                self.redisConfUtil.setSpiderGroupStatus(spider_group_name,json.dumps(spider_group_status))

            #更新机器上的爬虫
            spider_hosts = json.loads(self.redisConfUtil.getSpiderHostConf(host_ip))
            spiders_name = spider_hosts["spiders_name"]
            new_spiders_name = ""
            for s in spiders_name.split(","):
                if s:
                    if spider_name != s:
                        new_spiders_name = "%s%s," % tuple([new_spiders_name,s])

            spider_hosts["spiders_name"] = new_spiders_name
            self.redisConfUtil.setSpiderHostConf(host_ip,json.dumps(spider_hosts))


            #删除爬虫设置URL的状态
            self.redisConfUtil.con.hdel(redisKey.spider_set_url_status,spider_name)

            #删除爬虫的配置
            self.redisConfUtil.con.hdel(redisKey.spider_status,spider_name)




#所有爬虫的爬虫URL管理。
'''
通过爬虫族状态信息，获取爬虫族的所有爬虫。
根据爬虫族配置，获取每个爬虫爬取URL的间隔。如果没有配置默认值为3秒
1、和上次给爬虫设置URL的时间做比较，看是否到达放置URL的时间。
2、判断该爬虫是否出了完上次放置的URL。 如果出了完成。写入处理的URL。
    并查看爬虫状态是否存活。

3、keyword  : spider_group_name-run: 0:正常爬取  1：重跑
    filter: spider_group_name-filter:   过滤爬虫的URL池
    content: spider_group_name-content: 爬取内容的URL池。

    每个爬虫监控自己的spider_name的 redis_key

4、spider_set_url_status:{
        spider_name:{
            spider_name:"",
            update_date:""
        },
        spider_group_name:{
            spider_group_name:"",
            update_date:""
        }
    }
    记录爬虫设置URL的在状态。

'''
class SpiderUrlManager(threading.Thread):

    def __init__(self):
        self.redisConfUtil = RedisConfUtil()
        threading.Thread.__init__(self)

    def run(self):
        while True:
            self.intervalRun()
            time.sleep(1)


    #Run方法间隔调用。
    def intervalRun(self):
        #获取所有具有运行状态的爬虫族名
        spider_group_names = self.redisConfUtil.getSpiderGroupStatusKey()
        if spider_group_names:
            for spider_group_name in spider_group_names:
                self._setSpiderGroupStart(spider_group_name)
                self._setSpiderStart(spider_group_name)

    #通过配置的族的关键词获取，设置爬虫族的起始值。 其中会设置到关键词的重跑问题。 （重跑通过前台设置，该方法只负责重复爬取）
    def _setSpiderGroupStart(self,spider_group_name):
        #获取爬虫族的配置信息，通过该信息查每类爬虫的设置URL的间隔。
        spider_group_conf_j = json.loads(self.redisConfUtil.getSpiderGroupConf(spider_group_name))
        group_interval = spider_group_conf_j.get("group_interval",60*60)
        keyword_redis_key = spider_group_conf_j.get("keyword_redis_key")

        #判断爬虫族设置时间间隔到了没有
        if self._hasTimeReach(spider_group_name,group_interval):
            #判断过去爬取是否完成。
            if self._hasOldUrl(spider_group_name+"-start"):
                if keyword_redis_key:
                    for i in range(self.redisConfUtil.con.llen(keyword_redis_key)):
                        word = self.redisConfUtil.con.lindex(keyword_redis_key,i)
                        self.redisConfUtil.con.lpush(spider_group_name+"-start","0"+word)

                    self._updateSpiderSetUrlStatus(spider_group_name)

    def _setSpiderStart(self,spider_group_name):
        spiders = self._getActiveSpiderBySpiderGroupName(spider_group_name)
        if spiders:
            for spider in spiders:
                if self._hasTimeReach(spider[0],spider[2]):
                    if self._hasOldUrl(spider[0]):
                        self._setSpiderUrl(spider_group_name,spider[1],spider[0])

    #根据爬虫族名称获取爬虫族中存活爬虫的信息 (spider_name,spider_type interval)
    def _getActiveSpiderBySpiderGroupName(self,spider_group_name):

        result = []

        #获取爬虫族的配置信息，通过该信息查每类爬虫的设置URL的间隔。
        spider_group_conf_j = json.loads(self.redisConfUtil.getSpiderGroupConf(spider_group_name))
        start_interval = spider_group_conf_j.get("start_interval",60*30)
        filter_interval = spider_group_conf_j.get("filter_interval",60)
        content_interval = spider_group_conf_j.get("content_interval",3)

        interval ={"start_spider":start_interval,"filter_spider":filter_interval,"content_spider":content_interval}

        #根据爬虫族名称获取爬虫族下的所有爬虫信息。
        spider_group_status = self.redisConfUtil.getSpiderGroupStatus(spider_group_name)
        if spider_group_status:
            spider_group_status_j = json.loads(spider_group_status)
            for spider_type in ["start_spider","filter_spider","content_spider"]:
                if spider_group_status_j.get(spider_type):
                    spiders = spider_group_status_j[spider_type]
                    for spiderHost in spiders.split(","):
                        if spiderHost.find(":")>0:
                            spider_name = spiderHost[0:spiderHost.rindex(":")]
                            if not self._isSpiderStop(spider_name):
                                result.append((spider_name,spider_type,interval.get(spider_type)))


        if len(result)>0:
            return result
        else:
            return None


    #判断爬虫是否存活
    def _isSpiderStop(self,spider_name):
        return self.redisConfUtil.spiderStop(spider_name)

    #判断爬虫是否还有以前设置的URL
    def _hasOldUrl(self,spider_name):
        return self.redisConfUtil.con.llen(spider_name)==0

    #判断时间是否到达
    def _hasTimeReach(self,spider_name,interval):
        spider_set_url = self.redisConfUtil.getSpiderSetUrlStatus(spider_name)
        if spider_set_url:
            spider_set_url_j = json.loads(spider_set_url)
            oldUpdateDate = int(spider_set_url_j["update_date"])
            curr = int(self.redisConfUtil.getUpdateTime())
            if (curr-oldUpdateDate)/1000>int(interval):
                return True
            else:
                return False
        else:
            return True

    #设置爬虫URL,并更新该爬虫的设置URL的时间。
    def _setSpiderUrl(self,spider_group_name,spider_type,spider_name):

        if spider_type=="start_spider":
            self._setUrl(spider_group_name+"-start",spider_name)
        if spider_type=="filter_spider":
            self._setUrl(spider_group_name+"-filter",spider_name)
        if spider_type=="content_spider":
            for i in range(1):
                self._setUrl(spider_group_name+"-content",spider_name)

        self._updateSpiderSetUrlStatus(spider_name)

    #设置爬虫URL方法
    def _setUrl(self,source,to):
        url = self.redisConfUtil.con.rpop(source)
        if url:
            self.redisConfUtil.con.lpush(to,url)

    #更新设置爬虫URL的状态。
    def _updateSpiderSetUrlStatus(self,spider_name):
        spider_set_url = self.redisConfUtil.getSpiderSetUrlStatus(spider_name)
        if spider_set_url:
            spider_set_url_j = json.loads(spider_set_url)
            spider_set_url_j["update_date"] = self.redisConfUtil.getUpdateTime()
            self.redisConfUtil.setSpiderSetUrlStatus(spider_name,json.dumps(spider_set_url_j))
        else :
            spider_set_url = '{"spider_name":"%s","update_date":"%s"}' % tuple([spider_name,self.redisConfUtil.getUpdateTime()])
            self.redisConfUtil.setSpiderSetUrlStatus(spider_name,spider_set_url)

if __name__ == "__main__":

    hostSpiderM = HostSpiderManager()
    hostSpiderM.reportSpiderType()
    setUrlThread = SpiderUrlManager()
    setUrlThread.start()
    while True:
        hostSpiderM.moniterNewGroupAdd()
        hostSpiderM.moniterHostStatus()
        hostSpiderM.moniterStopSpider()
        hostSpiderM.moniterSpiderGroup()
        time.sleep(3)





