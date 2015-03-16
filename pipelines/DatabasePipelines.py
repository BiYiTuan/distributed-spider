#coding=utf8
__author__ = 'wanglu'

from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors

import redis
import scrapy_redis.connection as connection

from twisted.internet.threads import deferToThread

'''
Redis保存爬取记录。
'''
class RedisPipeline(object):
    """Pushes serialized item into a redis list/queue"""

    def __init__(self, server):
        self.server = server

    @classmethod
    def from_settings(cls, settings):
        server = connection.from_settings(settings)
        return cls(server)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        if spider.spider_type == "filter_spider":
            return deferToThread(self._process_item, item, spider)
        else:
            return item

    def _process_item(self, item, spider):
        key = self.item_key(item, spider)
        data = spider.spider_group_name+"-"+"content"+":"+spider.reRun+":"+item.get("site_url")
        self.server.lpush(key, data)
        return item

    def item_key(self, item, spider):
        """Returns redis key based on given spider"""
        return "%s:filterurl" % spider.task_id


class SestDao(object):

    def __init__(self):

        self.dbpool = adbapi.ConnectionPool('MySQLdb',
                              host='localhost',
                              db = 'dop',
                              user = 'root',
                              passwd = 'asiainfo',
                              cursorclass = MySQLdb.cursors.DictCursor,
                              charset = 'utf8',
                              use_unicode = False
        )


    def insert(self):
        query = self.dbpool.runInteraction(self.inert1,["name"])

    def inert1(self,tx,values):
        print(values[0])
        tx.execute("insert into test (name) values (%s)", (values[0]))
        tx.commit()

if __name__ == '__main__':

    sest = SestDao()
    sest.insert()

