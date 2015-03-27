#coding=utf8
__author__ = 'wanglu'

from scrapy.http import HtmlResponse
from scrapy import log

'''
基于模拟器，处理JS的中间件。
'''
class SeleniumMiddleware(object):
    '''
    执行JS，返回执行完JS之后的Html
    '''

    def process_request(self,request, spider):
        if spider.dr:
            log.msg("开始使用SeleniumMiddleware处理JS", log.INFO)
            spider.dr.get(request.url)

            try:
                if spider.Wait_Element:
                    spider.dr.find_element_by_xpath(spider.Wait_Element)
            except Exception, e:
                log.msg("Spider %s, no element [%s] when waiting for it!" % (spider.name, spider.Wait_Element))
                log.msg(request.url,level=log.INFO)
                log.msg(spider.dr.page_source,level=log.INFO)

            #print spider.dr.page_source
            return HtmlResponse(request.url, body=spider.dr.page_source.encode('utf-8'))
        return None
