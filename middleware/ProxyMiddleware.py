#coding=utf8
__author__ = 'wanglu'

import base64
import whaledataspider.settings as conf


class ProxyMiddleware(object):

    # overwrite process request
    def process_request(self, request, spider):
        if conf.IS_NEED_AGENT:
            # Set the location of the proxy
            request.meta['proxy'] = conf.proxy

            # Use the following lines if your proxy requires authentication
            proxy_user_pass = conf.proxy_auth_username+":"+conf.proxy_auth_password
            # setup basic authentication for the proxy
            encoded_user_pass = base64.encodestring(proxy_user_pass)
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass


