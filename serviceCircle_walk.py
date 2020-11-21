# -*- coding: utf-8 -*-
# import arcpy as ap
import urllib.request
import json
import math
import time
import os

#360/40076000*500*0.5
#500


def out_of_china(lng, lat):
    return not (lng > 73.66 and lng < 135.05 and lat > 3.86 and lat < 53.55)

def _transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
          0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret

def _transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
          0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
            math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
            math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret

def wgs84_to_gcj02(lng, lat):
    if out_of_china(lng, lat):  # 判断是否在国内
        return [lng, lat]
    dlat = _transformlat(lng - 105.0, lat - 35.0)
    dlng = _transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return (mglng, mglat)

def gcj02_to_wgs84(lng, lat):
    """
    GCJ02(火星坐标系)转GPS84
    :param lng:火星坐标系的经度
    :param lat:火星坐标系纬度
    :return:
    """
    if out_of_china(lng, lat):
        return [lng, lat]
    dlat = _transformlat(lng - 105.0, lat - 35.0)
    dlng = _transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return (lng * 2 - mglng, lat * 2 - mglat)

webKey_list = []
#规划方式：步行、骑行、公交、出租等
navigate_api = u'https://restapi.amap.com/v3/distance?'
negativeType = {
    "eu": 0,
    "drive": 1,
    "walk": 3
}
d0 = 360.0 / equator_leng
d_list = (d0*60*0.8, d0*60*5, d0*60*7)   #d

jsonFileType = (("route","paths",0,"duration"),("data","paths",0,"duration"),("route","transits",0,"duration"))

def Service(pnt,d,range_scan,index_key,index_negativeType,index_jsonType):
    beginPoint = pnt[1]   #社区点
    webKeyIndex = index_key
    x0 = (beginPoint[0] - d * range_scan / 2, beginPoint[1] - d * range_scan / 2)
    startTime = time.strftime('%d_%H%M%S', time.localtime(time.time()))
    fileName = './result/'+'points'+startTime+'_'+str(pnt[0])+'_wh'+'.txt'
    f = open(fileName,'w')
    # param_dest = "{:0,.6f},{:1,.6f}".format(beginPoint[0], beginPoint[1])
    param_dest = "{0},{1}".format(beginPoint[0], beginPoint[1])
    for i in range(range_scan + 1):
        dst_points = [(x0[0] + i * d, x0[1] + j * d) for j in range(range_scan + 1)]
        param_origin = "|".join(["{0},{1}".format(p[0], p[1]) for p in dst_points])
        finishFlag = True
        while finishFlag:
            try:
                url = navigate_api + "key={0}&origins={1}&destination={2}&output=json&type=3".format(webKey_list[webKeyIndex], param_origin, param_dest)
                result_all = json.load(urllib.request.urlopen(url))["results"]
                gd_all = [(result["duration"], result['distance']) for result in result_all]
                content = ["{0},{1},{2},{3}".format(d[0], d[1], g[0], g[1]) for (d, g) in zip(dst_points, gd_all)]
                f.write("\n".join(content))
                finishFlag = False
            except IOError:
                webKeyIndex += 1
                finishFlag = True
            except KeyError:
                webKeyIndex += 1
                finishFlag = True
            except RuntimeError:
                finishFlag = True
            except IndexError:
                finishFlag = False
        time.sleep(0.02)
    f.close()
    return (fileName,webKeyIndex)


if __name__ == '__main__':
    f_key = open('key.txt', 'r')
    line_key = f_key.readline()
    while line_key:
        webKey_list.append(line_key.replace('\n', ''))
        line_key = f_key.readline()
    f_key.close()
    while True:
        s = time.strftime('%Y%m%d',time.localtime(time.time()))
        if s == '20201119' or s == '20201120':
            print('start time-----'+time.strftime('%m%d_%H%M',time.localtime(time.time())))
            break
        else:
            print("等待中......")
            time.sleep(3)
    #pnt_path = u'./xlstoshp/小学连接2.shp'
    oringinPointList = []
    #with ap.da.SearchCursor(pnt_path,['FID','Lng_GD','Lat_GD'],'''"FID" > 155''') as cur:
        #for row in cur:
            #gcj02_pnt = wgs84_to_gcj02(float(row[1]),float(row[2]))
            #oringinPointList.append((int(row[0]),(gcj02_pnt[0],gcj02_pnt[1])))'''
    fp = open('XCQ.txt','r')    #--------------社区人口重心点----------------
    line_fp = fp.readline()
    while line_fp:
        pl = line_fp.replace('\n','').split(',')
        gcj02_pnt = wgs84_to_gcj02(float(pl[1]), float(pl[2]))
        oringinPointList.append((pl[0],(gcj02_pnt[0],gcj02_pnt[1])))
        line_fp = fp.readline()
    fp.close()
    ''' 开始爬虫
    '''
    start_time = time.strftime('%m-%d %H:%M:%S', time.localtime(time.time()))
    print(u"爬取开始，开始时间:{0}".format(start_time))
    num = 1
    currentTaskKeyIndex = 0
    fileNameList = []
    dealList = oringinPointList[0:]   #----------------
    for pnt in dealList:
        fileName, currentTaskKeyIndex = Service(pnt, d_list[0], 51, currentTaskKeyIndex % 138, 0, 0)
        fileNameList.append(fileName)
        print("---{0}--- finished ---currentTime:{1}".format(num,time.strftime('%H:%M:%S', time.localtime(time.time()))))
        num += 1
    end_time = time.strftime('%m-%d %H:%M:%S', time.localtime(time.time()))
    print(u"爬取结束，完成时间:{0}".format(end_time))
