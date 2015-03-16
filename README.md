# distributed-spider
基于Scrapy+Redis的分布式爬虫



1、编写不同类型的爬虫族类型：

#爬虫组类型配置
spider_group_type = "spider_group_type"

每个爬虫族类别下，有三类爬虫：

start_spider

filter_spider

content_spider

分别完成，爬虫的开始入口解析。爬虫重复url过滤。爬虫内容爬取。

2、根据不同的爬虫族类型配置爬虫。

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

定义该爬虫族是哪类爬虫，爬虫族下每类爬虫的并行最大数量。

有HostManager程序根据每一类爬虫的URL池大小，来动态启动爬虫和关闭闲置爬虫。
但保持没个爬虫族下每类爬虫都有一个实例进程处于工作状态。



3、运行爬虫机器状态：

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

监控运行爬虫机器的状态和爬虫数量。

4、爬虫族运行状态。
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



记录爬虫族内爬虫的状态信息。

5、记录爬虫运行状态信息。

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



记录爬虫运行状态信息。

6、给爬虫设置爬取URL的状态信息。


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



7、爬虫停止，移除配置信息。

#要删除爬虫配置的列表。
remove_spider_conf = "remove_spider_conf"


