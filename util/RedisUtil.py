#encoding=utf8
__author__ = 'wanglu'

import redis
import json
import datetime
import random

import whaledataspider.settings as conf


#在Redis中存放配置的Key
'''
存放爬虫的状态信息：存放格式为：
spider_status:   Map
spider_name{
				spider_group_name:"爬虫所属组的名称",
				host_name:"爬虫所在机器名称",
				spider_name:"爬虫名称"
				status:"爬虫当前状态",  0：准备启动  1：正在运行  2：运行空闲  -1：停止  11:正则启动
				spider_group_type:"爬虫族的类型"
				spider_type:"爬虫类型",
				update_date:"爬虫状态更新时间秒",
			}
'''
spider_status = "spider_status"

'''
爬虫机器信息
spider_host:  Map
host_name:{
				spiders_name:"机器上的爬虫名，逗号分隔"
				status:"机器状态"  1：启动  -1：停止
				update_date:""
			}
'''

spider_host = "spider_host"


#爬虫组类型配置
spider_group_type = "spider_group_type"

'''
spider_group_status{
				spider_group_name{
					start_spider:"name:host,"
					filter_spider:"name:host,"
					content_spider:"name:host,"
					update_date:""
				}
			}
'''
#爬虫族运行状态配置
spider_group_status = "spider_group_status"

'''
			spider_group_conf{
				spider_group_name:{
					keyword_redis_key:"",
					spider_group_type:"",
					spider_host:"",
					filter_spider_num:"",
					content_spider_num:"",
					"group_interval":"60",
					"start_interval":"1",
					"filter_interval":"1",
					"content_interval":"3"
				}
			}
'''
#爬虫族配置
spider_group_conf = "spider_group_conf"


'''
spider_set_url_status:{
        spider_name:{
            spider_name:"",
            update_date:""
        },
        spider_group_name:{
            spider_group_name:"",
            update_date:""
        }
    }
'''
#爬虫设置爬取URL状态
spider_set_url_status = "spider_set_url_status"

#要删除爬虫配置的列表。
remove_spider_conf = "remove_spider_conf"

'''
获取配置的工具类
'''
class RedisConfUtil():

    #计算爬虫状态更新比较基准
    compare_datetime = datetime.datetime(2015, 3, 4)
    con = redis.Redis(conf.REDIS_HOST,conf.REDIS_PORT)



    @classmethod
    def getUpdateTime(cls):
        days = (datetime.datetime.now()- cls.compare_datetime).days*24*60*60*1000
        times =(datetime.datetime.now()- cls.compare_datetime).seconds*1000
        m = random.randint(0,1000)

        return str(days+times+m)

    #判断Redis在爬虫状态是否要停止。
    @classmethod
    def spiderStop(self,spiderName):
        s = self.con.hget(spider_status,spiderName)
        if s:
            j = json.loads(s)
            return j.get("status") == "-1"
        else:
            return True

    #获取主节点上设置的机器上的所有爬虫配置信息。
    def getHostSpiderConf(self,host_ip):
        hostConf = self.con.hget(spider_host,host_ip)
        if hostConf:
            spiderConfs = []
            spiderNames = json.loads(hostConf).get("spiders_name")
            if spiderNames:
                spiders = spiderNames.split(',')
                for sp in spiders:
                    spiderConf = self.con.hget(spider_status,sp)
                    if spiderConf:
                        spiderConfs.append(spiderConf)
            if spiderConfs and len(spiderConfs)>0:
                return  spiderConfs
            else :
                return None
        else:
            return None

    #爬虫机器启动时，清空爬虫信息。
    def clearHostSpider(self):
        pass

    #获取爬虫机器配置
    def getSpiderHostConf(self,host_ip):
        return  self.con.hget(spider_host,host_ip)

    #设置爬虫机器的配置信息。
    def setSpiderHostConf(self,host_ip,conf):
        self.con.hset(spider_host,host_ip,conf)

    #向爬虫族类型中添加爬虫组类型。
    def setSpiderGroupTypes(self,spiderGroupTypes):
        for spiderGroupType in spiderGroupTypes:
            self.con.sadd(spider_group_type,spiderGroupType)

    #设置爬虫的启动配置信息。
    def setSpiderStatusConf(self,spider_name,conf):
        self.con.hset(spider_status,spider_name,conf)

    #获取爬虫族的状态
    def getSpiderGroupStatus(self,spider_group_name):
        return self.con.hget(spider_group_status,spider_group_name)

    #设置爬虫族的状态
    def setSpiderGroupStatus(self,spider_group_name,conf):
        self.con.hset(spider_group_status,spider_group_name,conf)

    #获取爬虫族的配置信息。
    def getSpiderGroupConf(self,spider_group_name):
        return self.con.hget(spider_group_conf,spider_group_name)

    def setSpiderGroupConf(self,spider_group_name,conf):
        self.con.hset(spider_group_conf,spider_group_name,conf)

    #获取所有爬虫机器
    def getSpiderHost(self):
        return self.con.hkeys(spider_host)



    #获取配置的爬虫族的key
    def getSpiderGroupConfKey(self):
        return self.con.hkeys(spider_group_conf)

    #获取已经启动的爬虫族的key
    def getSpiderGroupStatusKey(self):
        return self.con.hkeys(spider_group_status)

    #获取爬虫设置URL的状态
    def getSpiderSetUrlStatus(self,spider_name):
        return self.con.hget(spider_set_url_status,spider_name)

    #获取爬虫设置URL的状态
    def setSpiderSetUrlStatus(self,spider_name,conf):
        return self.con.hset(spider_set_url_status,spider_name,conf)

    #获取爬虫的配置。
    def getSpiderConf(self,spider_name):
        return self.con.hget(spider_status,spider_name)

    @classmethod
    def clearReids(cls):
        for key in cls.con.keys("*"):
            cls.con.delete(key)




if __name__ == '__main__':


    redisConfUtil = RedisConfUtil()
    print redisConfUtil.getUpdateTime()
    redisConfUtil.clearReids()

    '''
    spikerConf = '{"keyword_redis_key":"weibo_com_keyword","spider_group_type":"weibo_search_group","spider_host":"10.1.48.170","filter_spider_num":"3","content_spider_num":"5"}'
    redisConfUtil.setSpiderGroupConf("test",spikerConf)
    redisConfUtil.con.lpush("weibo_com_keyword","测试")
    '''

    '''
    spikerConf = '{"keyword_redis_key":"sina_news_keyword","spider_group_type":"baidu_search_sina_news_group","spider_host":"10.1.48.170","filter_spider_num":"3","content_spider_num":"5"}'
    redisConfUtil.setSpiderGroupConf("test",spikerConf)

    redisConfUtil.con.lpush("sina_news_keyword","news.sina.com.cn+2015+两会+退休")
    '''

    '''
    spikerConf = '{"keyword_redis_key":"weixin_keyword","spider_group_type":"sougou_search_weixin_group","spider_host":"10.1.48.170","filter_spider_num":"3","content_spider_num":"5"}'
    redisConfUtil.setSpiderGroupConf("test",spikerConf)

    redisConfUtil.con.lpush("weixin_keyword","测试")
    '''

    spikerConf = '{"keyword_redis_key":"weibo_user_watchurls","spider_group_type":"weibo_userinfo_group","spider_host":"10.1.48.170","filter_spider_num":"1","content_spider_num":"5","group_interval":"60","start_interval":"1","filter_interval":"1","content_interval":"3"}'
    redisConfUtil.setSpiderGroupConf("test",spikerConf)

    redisConfUtil.con.lpush("weibo_user_watchurls","1647263235|http://weibo.cn/1647263235")






