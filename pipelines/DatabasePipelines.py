#coding=utf8
__author__ = 'wanglu'

from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors

import whaledataspider.settings as conf
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation
from hbase.ttypes import TRowResult,TCell

import whaledataspider.util.ConfigUtil as confUtil

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


class HBasePipeline(object):

    def __init__(self,transport,client):
        self.client= client
        self.transport = transport
        self.transport.open()
        self.tableName = "detail_item"


    @classmethod
    def from_settings(cls, settings):
        transport = TTransport.TBufferedTransport(
            TSocket.TSocket(conf.thrift_ip,conf.thrift_port)
        )
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        return cls(transport,client)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)


    def process_item(self, item, spider):
        if spider.spider_type == "content_spider":
            return deferToThread(self._process_item, item, spider)
        else:
            return item

    def _process_item(self, item, spider):
        cols,vals,key = self.item_key(item, spider)
        print cols
        print vals
        print key
        mutations = [Mutation(column=col, value=val) for col,val in zip(cols,vals)]
        self.client.mutateRow(self.tableName,confUtil.getMd5(key),mutations,None)
        return item

    def item_key(self, item, spider):
        """Returns redis key based on given spider"""
        cols = []
        vals = []
        for col in spider.itemKeys:
            if item.get(col):
                cols.append("detail:{}".format(col))
                vals.append(item.get(col).encode("utf-8"))
        return cols,vals,item.get("site_url")

class HBaseTest:

    def __init__(self):
        self.tableName="detail_item"
        self.transport = TTransport.TBufferedTransport(
            TSocket.TSocket(conf.thrift_ip,conf.thrift_port)
        )

        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

    def hbase_tables(self):

        tables = self.client.getTableNames()
        print tables

        cols =['detail:publish_time', 'detail:site_source', 'detail:site_type', 'detail:site_url', 'detail:task_id',
               'detail:author',
               'detail:catch_date'
        ]
        vals = ['2015-03-10 02:39', 'news.sina.com.cn', 'news', 'http://news.sina.com.cn/c/2015-03-10/023931587440.shtml', '-1',
                u'\u4eac\u534e\u65f6\u62a5'.encode("utf-8"),
                '2015-03-27'
        ]
        key = "http://news.sina.com.cn/c/2015-03-10/023931587440.shtml"
        print confUtil.getMd5(key)


        mutations = [Mutation(column=col, value=val) for col,val in zip(cols,vals)]
        self.client.mutateRow(self.tableName,confUtil.getMd5(key),mutations,None)

    def scanner(self):

        column = ["site_url","title","author"]
        i=0
        for c in column:
            column[i]="detail:{}".format(c)
            i=i+1

        scanner = self.client.scannerOpen(self.tableName,"",column,None)

        r = self.client.scannerGet(scanner)
        result =[]

        while r:
            print r
            for t in r:
                #print t.columns["detail:title"].value
                print t.columns["detail:site_url"].value
                #author
                print t.columns["detail:author"].value
            result.append(r)
            r= self.client.scannerGet(scanner)
        print "Scanner finished"
        print result


class SestDao(object):

    def __init__(self):

        self.dbpool = adbapi.ConnectionPool('MySQLdb',
                              host='localhost',
                              db = 'test',
                              user = 'root',
                              passwd = 'root',
                              cursorclass = MySQLdb.cursors.DictCursor,
                              charset = 'utf8',
                              use_unicode = False
        )


    def insert(self):
        print "namedsa"
        query = self.dbpool.runInteraction(self.inert1,["name"])

    def inert1(self,tx,values):
        print "dfadfas"
        print(values[0])
        tx.execute("insert into test (name) values (%s)", (values[0]))
        tx.commit()



if __name__ == '__main__':
    '''
    conn = MySQLdb.connect(host='localhost',user='root',passwd='root',db='test')
    cursor = conn.cursor()
    sql = "insert into test (name) values ('name')"
    cursor.execute(sql)

    cursor.close()
    conn.commit()
    conn.close()
    '''

    t = HBaseTest()
    #t.hbase_tables()
    t.scanner()