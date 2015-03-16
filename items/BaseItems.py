#coding=utf8
__author__ = 'wanglu'

from scrapy import Item, Field
from scrapy.contrib.loader import ItemLoader

class BaseItem(Item):
    spider_type = Field()
    publish_time = Field()
    site_source = Field()
    site_type = Field()
    site_url = Field()
    task_id = Field()
    title = Field()
    content = Field()
    author = Field()
    catch_date = Field()



