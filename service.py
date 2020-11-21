# -*- coding: utf-8 -*-
import arcpy as ap
import urllib
import json
import math
import time
import os


x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626  # π
a = 6378245.0  # 长半轴
ee = 0.00669342162296594323  # 偏心率平方
equator_leng = 40076000  #赤道长

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

def doServiceCircle(fileName):
    code_name = os.path.basename(fileName).split('.')[0]
    #file_baseName = os.path.basename(fileName).split('.')[0]
    out_path = r'./service_circle'
    point_name = 'point_'+ code_name
    shpFile = out_path + '/'+point_name + '.shp'
    geometry_type = "POINT"
    spatial_reference = ap.SpatialReference(4326)
    ap.CreateFeatureclass_management(out_path, point_name + '.shp', geometry_type,spatial_reference=spatial_reference)
    ap.AddField_management(shpFile, "Distance", "LONG")
    ap.AddField_management(shpFile, "Time", "LONG")
    # Insert points into point feature class
    pnt_cur = ap.da.InsertCursor(shpFile, ["Shape@", 'Distance','Time'])
    f = open('./point/'+fileName,'r')
    line = f.readline()
    while line:
        lineList = line.split(',')
        pnt_wgs84 = gcj02_to_wgs84(float(lineList[0]), float(lineList[1]))
        pnt_arcgis = ap.Point(pnt_wgs84[0], pnt_wgs84[1])
        pnt_cur.insertRow([pnt_arcgis, lineList[3],lineList[2]])
        line = f.readline()
    f.close()
    del pnt_cur
    ap.CheckOutExtension("spatial")
    ras_spline = ap.sa.Spline(shpFile, "Time")

    ras_cal4 = (ras_spline <= 300)
    ap.MakeRasterLayer_management(ras_cal4, "lyr4")
    ap.SelectLayerByAttribute_management("lyr4", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_5min_'+code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr4", outPolygon, "NO_SIMPLIFY", "Value")

    ras_cal5 = (ras_spline <= 600)
    ap.MakeRasterLayer_management(ras_cal5, "lyr5")
    ap.SelectLayerByAttribute_management("lyr5", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_10min_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr5", outPolygon, "NO_SIMPLIFY", "Value")

    ras_cal6 = (ras_spline <= 900)
    ap.MakeRasterLayer_management(ras_cal6, "lyr6")
    ap.SelectLayerByAttribute_management("lyr6", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_15min_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr6", outPolygon, "NO_SIMPLIFY", "Value")

    '''ras_cal2 = (ras_spline <= 1000)
    ap.MakeRasterLayer_management(ras_cal2, "lyr2")
    ap.SelectLayerByAttribute_management("lyr2", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_1000m_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr2", outPolygon, "NO_SIMPLIFY", "Value")

    ras_cal3 = (ras_spline <= 2000)
    ap.MakeRasterLayer_management(ras_cal3, "lyr3")
    ap.SelectLayerByAttribute_management("lyr3", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_2000m_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr3", outPolygon, "NO_SIMPLIFY", "Value")'''

    '''ap.CheckOutExtension("spatial")
    ras_spline = ap.sa.Spline(shpFile, "Time")

    ras_cal4 = (ras_spline <= 300)
    ap.MakeRasterLayer_management(ras_cal4, "lyr4")
    ap.SelectLayerByAttribute_management("lyr4", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_5min_'+code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr4", outPolygon, "NO_SIMPLIFY", "Value")

    ras_cal5 = (ras_spline <= 600)
    ap.MakeRasterLayer_management(ras_cal5, "lyr5")
    ap.SelectLayerByAttribute_management("lyr5", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_10min_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr5", outPolygon, "NO_SIMPLIFY", "Value")

    ras_cal6 = (ras_spline <= 900)
    ap.MakeRasterLayer_management(ras_cal6, "lyr6")
    ap.SelectLayerByAttribute_management("lyr6", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_15min_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr6", outPolygon, "NO_SIMPLIFY", "Value")

    ras_cal7 = (ras_spline <= 900)
    ap.MakeRasterLayer_management(ras_cal7, "lyr7")
    ap.SelectLayerByAttribute_management("lyr7", "NEW_SELECTION", '"Value" = 1')
    outPolygon = out_path + '/polygon_20min_' + code_name + '.shp'
    ap.RasterToPolygon_conversion("lyr7", outPolygon, "NO_SIMPLIFY", "Value")
    
    
    '''


if __name__ == '__main__':
    fileNameList = os.listdir('./point')
    num = len(fileNameList)
    end_time = time.strftime('%m%d_%H%M%S', time.localtime(time.time()))
    print(u"爬取结束，完成时间:{0}".format(end_time))
    print(u"爬取结果转化中........")
    for file_name in fileNameList:
        doServiceCircle(file_name)
        num -= 1
        print(u"----剩余{0}个文件---".format(num))
    print(u'转化完成')
