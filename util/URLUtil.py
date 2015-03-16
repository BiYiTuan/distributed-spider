#coding=utf8
__author__ = 'wanglu'

import urllib2
import whaledataspider.settings as conf


#存放跳转的URL
class RedirectUrl(object):
    def __init__(self,url):
        self.url =url

#当发生跳转时配置。
class RedirctHandler(urllib2.HTTPRedirectHandler):
  """docstring for RedirctHandler"""
  def http_error_301(self, req, fp, code, msg, headers):
    pass


  def http_error_302(self, req, fp, code, msg, headers):
    if 'location' in headers:
        newurl = headers.getheaders('location')[0]
        return RedirectUrl(newurl)

def getRedirectUrl(url,timeout=10):
    opener = None
    if conf.IS_NEED_AGENT:
        proxy_handler = urllib2.ProxyHandler({conf.proxy_type:conf.proxy})
        proxy_auth_handler = urllib2.HTTPBasicAuthHandler()
        proxy_auth_handler.add_password('realm','host',conf.proxy_auth_username,conf.proxy_auth_password)
        opener = urllib2.build_opener(proxy_handler,proxy_auth_handler,RedirctHandler)
    else:
        opener = urllib2.build_opener(RedirctHandler)

    response = None

    redirectUrl = None
    try:
        response = opener.open(url,timeout=timeout)
        if isinstance(response,RedirectUrl):
            if hasattr(response,"url"):
                redirectUrl=response.url
    except urllib2.URLError as e:
        if hasattr(e, 'code'):
            error_info = e.code
        if hasattr(e, 'reason'):
            error_info = e.reason
    finally:
        if response:
            if not isinstance(response,RedirectUrl):
                response.close()

    if response:
        return redirectUrl
    else:
        return


if __name__ == "__main__":
    url ="http://www.baidu.com/link?url=YW4u2Dyq7Ly7GdVI0nsXJ-uSxquUBqiX9JNWr6H3rwFEOBQxlV5agcvaQaElgfvebaHVGI8MJ4N-KnK-ZfezYmV_4W4LVI6zAE1x4zlYE6K"
    html = getRedirectUrl(url)
    print html