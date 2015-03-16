#coding=utf8
__author__ = 'wanglu'

import re
import datetime

'''
清洗抓取字符串的方法。
'''

#清楚HTML标签。
def filterHtml(val):
    result, num = re.subn(r"<[^>]+>","",val)
    return result

#获取Baidu抓取内容的日期。
def baiduGetDate(val):
    #b=val.replace(" ","")
    c=re.findall(r'\d{4}-\d{1,2}-\d{1,2}',val)
    if len(c):
        return c[0]
    else:
        return ""


#获取字符串中的数组的，并返回第一个值。
def getNum(val):
    r = re.findall(r'\d+',val)
    if len(r):
        return r[0]
    else:
        return "0"

#去除字符串的空白符。
def removeBlankStr(val):
    p = re.compile('\s+')
    return re.sub(p,'',val)


#获取百度的搜索数量。
def getBaiduSearchNum(val):
    all = re.findall(r"\d",val)
    result = ""
    for a in all:
        result = "%s%s" % tuple([result,a])
    if result:
        return result
    else:
        return "0"

#获取新浪的URL
def findSinaNewsUrl(val):
    al  = re.findall(r"^http://news.sina.com.cn/\w+/\d{4}-\d{2}-\d{2}/\d+\.shtml",val)
    if len(al)>0:
        return al[0]
    else:
        return None

#转化新浪页面的发布日期
def getSinaPublishTime(val):
    try:
        d = datetime.datetime.strptime(val,'%Y年%m月%d日%H:%M')
        return d.strftime('%Y-%m-%d %H:%M')
    except:
        return ""

#转化新浪页面的发布日期
def getWeixinPublishTime(val):
    try:
        d = datetime.datetime.strptime(val,'%Y-%m-%d')
        return d.strftime('%Y-%m-%d %H:%M')
    except:
        return ""


#获取新浪微博中用户的信息，微博数、关注数，粉丝数
#粉丝数、关注数,微博数
def getWeiboCnUserInfo(val):

    s = re.findall(r"\[\d+\]",val)
    r = []
    count =0
    for num in s:
        if count>2:
            break;
        n = re.findall(r"\d+",num)
        if len(n)>0:
            r.append(int(n[0]))
        count = count+1

    if len(r)==3:
        weibo_num = r[0]
        r[0] = r[2]
        r[2] = weibo_num
        return r
    else:
        return [0,0,0]
