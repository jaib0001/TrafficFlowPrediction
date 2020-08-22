#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
@author: ibisc
"""
import time
import urllib.request
import shutil
import tempfile
import urllib.request
import time
import datetime
import os
import csv, traceback
import sensors
import pytz
from elasticsearch import Elasticsearch
import sys
import json
from pprint import pprint
#import mapping_es
import logging


# Create path
def makepath(path):
    if not os.path.exists(path):
        try:
            os.mkdir(path)
        except OSError:
            print("Creation of the directory %s failed" % path)
        else:
            print("Successfully created the directory %s " % path)


# Eliminate leading zero for dates [vonIndex:bisIndex]
def rm_zero_win(par):
    if len(par) > 1:
        if par.startswith("0"):
            return par[1:]
    return par


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = mapping_es.mapping
    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


# generate Links
def create_links(f_tuple, t_tuple, interval):
    linkList = []
    #f_tuple = datetime.datetime.strptime(from_date, "%d.%m.%Y %H:%M %z")
    #t_tuple = datetime.datetime.strptime(to_date, "%d.%m.%Y %H:%M %z")
    while f_tuple <= t_tuple:
        # Reformat time to utc
        utc_time = f_tuple.astimezone(pytz.timezone("UTC"))
        # Set parameters
        year = utc_time.strftime('%y')
        month = rm_zero_win(utc_time.strftime('%m'))
        day = rm_zero_win(utc_time.strftime('%d'))
        hour = rm_zero_win(utc_time.strftime('%I'))
        minute = utc_time.strftime('%M')
        p = utc_time.strftime('%p')
        # Use Berlin Time for file name and system timestamp
        # Append link parameters
        linkList.append((year, month, day, hour, minute, p, utc_time))
        f_tuple = f_tuple + datetime.timedelta(minutes=interval)
    return linkList


def download_files(tuple, elasticSearch, fieldnames, file):
    year, month, day, hour, minute, p, utc_time = tuple
    print("Downloading... [ {}.{}.{} {}:{} {} {} ]".format(day, month, year, hour, minute, p, utc_time))
    url = "http://darmstadt.ui-traffic.de/resources/CSVExport?from=" + month + "%2F" + day + "%2F" + year + "+" + hour + "%3A" + minute + "+" + p + "&to=" + month + "%2F" + day + "%2F" + year + "+" + hour + "%3A" + minute + "+" + p
    fail = True
    tries = 0
    while fail:
        print("I try for the " + str(tries) + " time.")
        try:
            with urllib.request.urlopen(url) as response:
                # upload_to_es(tuple, response, elasticSearch, sensor_list)
                writeFile(tuple, response, fieldnames, file)
            fail = False
        except:
            traceback.print_exc()
            tries += 1
            #time.sleep(1)

"""
def upload_to_es(tuple, response, elasticSearch, sensor_list):
    year, month, day, hour, minute, p, utc_time = tuple
    iter_object = iter(response.read().decode("utf-8").split("\r\n"))
    next(iter_object)  # skip headline
    for line in iter_object:
        try:
            if line:
                row = line.split(";")
                datapoint = {}
                j_dict = dict(dict(zip(["Day", "Time", "Crossroad", "Sensor", "Interval", "Load", "Count"],
                                       [row[0], row[1], row[2], row[3], row[4], row[5], row[6]])))
                syn_crossroad_sensor = j_dict["Crossroad"].replace(" ", "0") + "_" + j_dict["Sensor"]
                if syn_crossroad_sensor in sensor_list:
                    datapoint["datatype"] = "original"
                    datapoint["crossroad_sensor"] = syn_crossroad_sensor
                    datapoint["name"] = sensor_list[syn_crossroad_sensor]["Name"]
                    datapoint["sensor_location"] = sensor_list[syn_crossroad_sensor]["Location"]
                    datapoint["crossroad"] = j_dict["Crossroad"]
                    datapoint["nr"] = j_dict["Sensor"]
                    datapoint["timestamp"] = utc_time.replace(tzinfo=None).isoformat()  # yyyy-MM-dd'T'HH:mm
                    datapoint["day_of_week_i"] = int(utc_time.strftime('%w'))
                    datapoint["load"] = int(j_dict["Load"])
                    datapoint["count"] = int(j_dict["Count"])
                    datapoint["sector_x"] = sensor_list[syn_crossroad_sensor]["sector"]["x"]
                    datapoint["sector_y"] = sensor_list[syn_crossroad_sensor]["sector"]["y"]
                    datapoint["sector_x_s"] = sensor_list[syn_crossroad_sensor]["sector_s"]["x"]
                    datapoint["sector_y_s"] = sensor_list[syn_crossroad_sensor]["sector_s"]["y"]
                    print(datapoint)
                    # {'Crossroad_Sensor': 'A102_1', 'Name': 'D1', 'Location': {'lat': '49.86888923', 'lon': '8.64919066'}, 'Crossroad': 'A102', 'Nr': '1', 'timestamp': '2019-05-30T17:55:00', 'Day_of_week_i': 4, 'Load': 1, 'Count': 1}
                    elasticSearch.index(index='traffic_data', body=datapoint)
        except:
            traceback.print_exc()
"""

def writeFile(tuple, response, fieldnames, file):
    year, month, day, hour, minute, p, utc_time = tuple
    print(tuple)
    iter_object = iter(response.read().decode("utf-8").split("\r\n"))
    # initialise a complete list of all sensors with NaN as default (missing) value
    to_write_json = {sen: "NaN" for sen in list(fieldnames)}
    next(iter_object)  # skip headline
    for line in iter_object:
        if line:
            row = line.split(";")
            j_dict = dict(dict(zip(["Day", "Time", "Crossroad", "Sensor", "Interval", "Load", "Count"],
                                   [row[0], row[1], row[2], row[3], row[4], row[5], row[6]])))
            syn_crossroad_sensor = j_dict["Crossroad"].replace(" ", "0") + "_" + j_dict["Sensor"]
            if syn_crossroad_sensor+"_count" in fieldnames:
                to_write_json[syn_crossroad_sensor + "_count"] = int(j_dict["Count"])
                to_write_json[syn_crossroad_sensor + "_load"] = int(j_dict["Load"])
    # write day of the week and hour of the day
    tz = pytz.timezone('Europe/Berlin')
    dt = tz.normalize(utc_time.astimezone(tz))
    to_write_json["timestamp"] = time.mktime(dt.timetuple())
    file.writerow(to_write_json)


def main():
    logging.basicConfig(level=logging.ERROR)
    # exampleurl = "http://darmstadt.ui-traffic.de/resources/CSVExport?from="+MONAT+"%2F"+TAG+"%2F"+JAHR+"+"+STUNDEN+"%3A"+MIN+"+"+H+"&to="+MONAT+"%2F"+TAG+"%2F"+JAHR+"+"+STUNDEN+"%3A"+MIN+"+"+H
    elasticSearch = Elasticsearch(['localhost'], port=9200)
    sensor_list = sensors.create_sensor_list()
    #sensor_list = sensors.enrich_with_sectors(sensor_list, 10)
    # Input as time zone Berlin
    from_date = datetime.datetime.strptime("20.05.2019 00:00", "%d.%m.%Y %H:%M").astimezone(pytz.timezone("Europe/Berlin"))
    to_date = datetime.datetime.strptime("04.08.2019 00:00", "%d.%m.%Y %H:%M").astimezone(pytz.timezone("Europe/Berlin"))
    # to_date = "20.05.2019 00:00 +0200"
    # interval in minutes
    interval = 5
    # connect to ES
    #es = connect_elasticsearch()
    # create Index mapping for ES
    #create_index(es, "traffic_data")
    file = open("lc3monate2.csv", "a", newline='')
    keylist = list(sensor_list.keys())
    # we need a load and a count column for each sensor
    load = lambda x: x + "_load"
    count = lambda x: x + "_count"
    fieldnames = [f(name) for name in keylist for f in(load,count)]
    # ... and one timestamp
    fieldnames.extend(["timestamp"])
    filewriter = csv.DictWriter(file, delimiter=',', quotechar='"', fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    filewriter.writeheader()
    linkList = create_links(from_date, to_date, interval)

    for link in linkList:
        download_files(link, elasticSearch, fieldnames, filewriter)
        time.sleep(1)
    file.close()


if __name__ == "__main__":
    main()
