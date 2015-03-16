#coding=utf8
__author__ = 'wanglu'

from whaledataspider.items.BaseItems import BaseItem
from scrapy.item import Item, Field
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import MapCompose, TakeFirst, Join

import whaledataspider.util.FilterOper as fo

import sys

reload(sys)
sys.setdefaultencoding("utf-8")



#新浪新闻的内容
class WeixinItem(BaseItem):

    def black(self):
        pass


#新浪新闻的内存过滤。
class WeixinItemLoader(ItemLoader):
    default_item_class = WeixinItem
    default_input_processor = MapCompose(lambda s: s.strip())
    default_output_processor = TakeFirst()
    description_out = Join()

    content_in = MapCompose(fo.removeBlankStr, fo.filterHtml)

    publish_time_in = MapCompose(fo.removeBlankStr,fo.getWeixinPublishTime)






