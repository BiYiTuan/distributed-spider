# -*- coding: utf-8 -*-

# Scrapy settings for myspider project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'whaledataspider'

SPIDER_MODULES = ['whaledataspider.spiders']
NEWSPIDER_MODULE = 'whaledataspider.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'myspider (+http://www.yourdomain.com)'


# Enables scheduling storing requests queue in redis.
#SCHEDULER = "scrapy_redis.scheduler.Scheduler"
SCHEDULER = "scrapy.core.scheduler.Scheduler"

# Don't cleanup redis queues, allows to pause/resume crawls.
SCHEDULER_PERSIST = True

# Schedule requests using a priority queue. (default)
#SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderPriorityQueue'

# Schedule requests using a queue (FIFO).
#SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderQueue'

# Schedule requests using a stack (LIFO).
#SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderStack'

# Max idle time to prevent the spider from being closed when distributed crawling.
# This only works if queue class is SpiderQueue or SpiderStack,
# and may also block the same time when your spider start at the first time (because the queue is empty).
SCHEDULER_IDLE_BEFORE_CLOSE = 10

# Store scraped item in redis for post-processing.
ITEM_PIPELINES = [
   # 'scrapy_redis.pipelines.RedisPipeline',
    'whaledataspider.pipelines.DatabasePipelines.RedisPipeline',
    'whaledataspider.pipelines.FilePipelines.TxtFileStorePipeline',
   # 'myspider.pipelines.FilePipelines.CsvFileStorePipeline',
]

DOWNLOADER_MIDDLEWARES = {
    'whaledataspider.middleware.JsMiddleware.SeleniumMiddleware':110,
    'whaledataspider.middleware.ProxyMiddleware.ProxyMiddleware':100,

}


# Specify the host and port to use when connecting to Redis (optional).
REDIS_HOST = '10.1.252.38'
REDIS_PORT = 6379

# Specify the full Redis URL for connecting (optional).
# If set, this takes precedence over the REDIS_HOST and REDIS_PORT settings.
#REDIS_URL = 'redis://user:pass@hostname:9001'


#-----------------------------------------------------------------
#pipelines 抓取内容文件保存的目录。
File_Pipelines_Path = u'F:/test/python'


'''
    设置项目的环境变量。
'''
#设置配置文件的路径
CONFIG_PATH = 'D:/.../config'

IS_NEED_AGENT = True

proxy = "..."
proxy_type ="..."
proxy_auth_username = "..."
proxy_auth_password = "..."

#配置代理信息
service_args = [
        '--proxy=...',
        '--proxy-type=...',
        '--proxy-auth=...'
    ]
#浏览器路径
PhantomJS_Path="D:\Python27\Scripts\phantomjs.exe"


dcap={'platform': 'ANY',
        'browserName':'chrome',
      'version': '',
      'javascriptEnabled': True
      }

#PhantomJS等待元素加载时间。
PhantomJS_Wait=10

