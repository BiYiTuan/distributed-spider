#coding=utf8
__author__ = 'wanglu'

from whaledataspider import settings as conf
import json
import sys
import os
import socket


#获取配置文件
def __getConfigFile(fileName):
    try:
        return __hasFile(conf.CONFIG_PATH,fileName)
    except Exception,e:
        raise e

#判断文件是否存在
def __hasFile(path,fileName):
    filePath=os.path.join(path,fileName)
    if os.path.isfile(filePath):
        return filePath
    else:
        raise RuntimeError("Error:file not exist in ",filePath)

#读取JSON配置文件
def getJsonStr(jsonName):
    try:
        jsonPath=__getConfigFile(jsonName)
        with file(jsonPath) as jsonFile:
            jsonStr=json.load(jsonFile)
        return jsonStr
    except Exception,e:
        raise e


#获取本机IP
def getLocalIp():
    myname = socket.getfqdn(socket.gethostname())
    myaddr = socket.gethostbyname(myname)
    return myaddr


#将字符串转化为JSON
def strToJson(val):
    return json.loads(val)

#将Json转化为字符串
def jsonToStr(val):
    return json.dumps(val)

if __name__ == "__main__":
    jsonStr=getJsonStr("weibo.json")
    print(jsonStr)
    print(jsonStr.get('weibo_com').get('author'))







