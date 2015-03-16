#coding=utf8
__author__ = 'wanglu'

import whaledataspider.settings as conf

import csv

from scrapy import signals

'''
写爬取数据到Txt中。
'''
class TxtFileStorePipeline(object):

    #对item进行处理的方法。
    def process_item(self, item, spider ):

        if spider.spider_type == "content_spider":
            filePath = "%s/%s_content.txt" % (conf.File_Pipelines_Path,spider.name.replace(":","_"))

            with open(filePath,'a') as f:
                for key in spider.itemKeys:
                    f.write(key)
                    f.write(":")
                    f.write(item[key])
                    f.write('\n')

                f.write('\n')
        return item


'''
下面实现参照 pipelines.RedisPipeline的实现。
有几个启动调用方法:
@classmethod
from_crawler(cls, crawler) 爬虫开启时调用。 注意返回值为该Pipeline对象。
上面方法，是Scrapy自动调用的。

还可以定义自己的classmethod方法，来根据配置完成更细的逻辑处理。

对象的实例中，还有两个触发方法。
spider_opened(self, spider)
spider_closed(self, spieder)
这两个方法，是当爬虫启动和关闭时调用的。可以实现一些全局资源的开启和释放。
比如：文件的打开和关闭。不用在 process_item时，每次都打开文件，提高效率。
注意：上面两个方法需要在from_crawler中注册。

process_item(self, item, spider)
该方法，当爬取到数据后调用。 保存结果。

'''
class CsvFileStorePipeline(object):

    def __init__(self,savePath):
        self.savePath = savePath

    @classmethod
    def from_settings(cls, settings):
        savePath = settings.get('File_Pipelines_Path',"F:/test")
        return cls(savePath)   #返回实例

    @classmethod
    def from_crawler(cls, crawler):
        #根据Scrapy的信号，注册两个调用方法。
        pipeline = cls.from_settings(crawler.settings)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    #当爬虫启动后，打开CSV文件获取写的流，并写入CSV的标题。
    def spider_opened(self, spider):
        #获取文件绝对路径
        self.filePath = "%s/%s_content.csv" % (self.savePath,spider.name)
        #获取csv流
        self.writer = csv.writer(file(self.filePath,'wb'))
        self.writer.writerow(spider.itemKeys)

    #当爬虫关闭时，关闭CSV文件。
    def spider_closed(self,spider):
        self.writer.writerow(["CSV File Close",self.writer.closed()])
        self.writer.close()


    #对item进行处理的方法。
    def process_item(self, item, spider ):
        self.writer.writerow(self.__getItemArr(item,spider))
        return item

    #将Item的数据转化为数组。
    def __getItemArr(self,item, spider):
        arr = []
        for key in spider.itemKeys:
            arr.append(item[key])
        return arr

