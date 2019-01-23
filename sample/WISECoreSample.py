# encoding: utf-8

import sys
from setuptools import setup, find_packages

sys.path.insert(0, 'src')

import WISECore

import json
import threading
import time
from random import randint

g_strServerIP = "172.22.12.198"
g_iPort = 1883
g_strConnID = "admin"
g_strConnPW = "05155853"
g_strClientID = "00000001-0000-0000-0000-000BAB548765"
g_strMAC = "000BAB548765"
g_strTag = "RMM"
g_strHostName = "PY_SampleClient"
g_bReport = True
g_strReportData = '{"agentID":"%s","commCmd":2055,"handlerName":general,"content":{"opTS":{"$date":%d},"%s":{"%s":{"bn":"%s","e":[{"n":"data1","v":%d},{"n\":"data2","v":%d},{"n":"data3","v":%d}]}}},"sendTS":{"$date":%d}}'
g_iReportInterval = 5  #sec.
g_iHeartbeatRate = 60  #sec.
g_lockHeartbeat = threading.Lock()
g_stopHeartbeatFlag = threading.Event()
g_lockReportdata = threading.Lock()
g_stopReportDataFlag = threading.Event()


def SendReportData(wise, clientID):
    curTs = time.time() * 1000
    value1 = randint(0, 9)
    value2 = randint(0, 9)
    value3 = randint(0, 9)
    strDevReportTopic = "/wisepaas/device/%s/devinfoack"  #publish
    strTopic = strDevReportTopic % clientID
    strMessage = g_strReportData % (clientID, curTs, "SampleSensor",
                                    "MySensor", "MySensor", value1, value2,
                                    value3, curTs)
    wise.core_publish(strTopic, strMessage, False, 0)


def SendCapability(wise, clientID):
    curTs = time.time() * 1000
    value1 = randint(0, 9)
    value2 = randint(0, 9)
    value3 = randint(0, 9)
    strDevReportTopic = "/wisepaas/%s/%s/agentactionack"  #publish
    strTopic = strDevReportTopic % (g_strTag, clientID)
    strMessage = g_strReportData % (clientID, curTs, "SampleSensor",
                                    "MySensor", "MySensor", value1, value2,
                                    value3, curTs)
    wise.core_publish(strTopic, strMessage, False, 0)


class threadReconnect(threading.Thread):
    def __init__(self, wise, ip, port, id, pw):
        threading.Thread.__init__(self)
        self.wise = wise
        self.ip = ip
        self.port = port
        self.id = id
        self.pw = pw

    def run(self):
        self.wise.core_disconnect(False)
        self.wise.core_connect(self.ip, self.port, self.id, self.pw)


class threadReportData(threading.Thread):
    def __init__(self, event, wise, timer, func, clientID):
        threading.Thread.__init__(self)
        self.stopped = event
        self.wise = wise
        self.timer = timer
        self.func = func
        self.clientID = clientID

    def run(self):
        print("Start Reporting")
        while not self.stopped.wait(self.timer):
            print("Publish Report Data")
            if self.func != None:
                self.func(self.wise, self.clientID)
        self.stopped.clear()
        print("Stop Reporting")


class threadHeartbeat(threading.Thread):
    def __init__(self, event, wise, timer):
        threading.Thread.__init__(self)
        self.stopped = event
        self.wise = wise
        self.timer = timer

    def run(self):
        print("Start heart beating")
        while not self.stopped.wait(self.timer):
            print("Publish heartbeat")
            self.wise.core_heartbeat_send()
        self.stopped.clear()
        print("Stop heart beating")


# The callback for when the client receives a CONNACK response from the server.
def on_connect():
    print("Connected")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    
    strActionReqTopic = "/wisepaas/%s/%s/agentactionreq" % (
        g_strTag,
        g_strClientID)  #Subscrib "/wisepaas/<custom>/<devId>/agentactionreq"
    wise.core_subscribe(strActionReqTopic, 0)
    strServerCtrlTopic = "/wisepaas/device/+/agentctrlreq"  #Subscribe
    wise.core_subscribe(strServerCtrlTopic, 1)
    strActionReqTopic = "/wisepaas/%s/%s/agentactionreq" % (
        "device",
        g_strClientID)  #Subscrib "/wisepaas/<custom>/<devId>/agentactionreq"
    wise.core_subscribe(strActionReqTopic, 0)

    wise.core_device_register()

    global g_lockHeartbeat
    global g_stopHeartbeatFlag
    global g_threadHeartbeat
    global g_iHeartbeatRate
    g_lockHeartbeat.acquire()
    g_threadHeartbeat = threadHeartbeat(g_stopHeartbeatFlag, wise,
                                        g_iHeartbeatRate)
    g_threadHeartbeat.start()
    g_lockHeartbeat.release()

    global g_lockReportdata
    global g_threadReportData
    global g_lockReportdata
    global g_bReport
    g_lockReportdata.acquire()
    if g_bReport:
        g_threadReportData = threadReportData(g_stopReportDataFlag, wise,
                                              g_iReportInterval,
                                              SendReportData, g_strClientID)
        g_threadReportData.start()
    g_lockReportdata.release()


# The callback for when the client lostconnect from the server.
def on_lostconnect(rc):
    print("Lostconnected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.

    # this will stop the timer
    global g_stopHeartbeatFlag
    global g_threadHeartbeat
    global g_lockHeartbeat
    g_lockHeartbeat.acquire()
    g_stopHeartbeatFlag.set()
    g_threadHeartbeat.join()
    g_lockHeartbeat.release()

    global g_lockReportdata
    global g_threadReportData
    global g_lockReportdata
    global g_bReport
    g_lockReportdata.acquire()
    g_stopReportDataFlag.set()
    g_threadReportData.join()
    g_lockReportdata.release()


# The callback for when the client disconnect from the server.
def on_disconnect():
    print("Disconnected")

    # this will stop the timer
    global g_stopHeartbeatFlag
    global g_threadHeartbeat
    global g_lockHeartbeat
    g_lockHeartbeat.acquire()
    g_stopHeartbeatFlag.set()
    g_threadHeartbeat.join()
    g_lockHeartbeat.release()

    global g_lockReportdata
    global g_threadReportData
    global g_lockReportdata
    global g_bReport
    g_lockReportdata.acquire()
    g_stopReportDataFlag.set()
    g_threadReportData.join()
    g_lockReportdata.release()


# The callback for when a PUBLISH message is received from the server.
def on_message(topic, msg, userdata):
    print(topic + " " + msg)


# The callback for when a PUBLISH message is received from the server.
def on_get_timetick():
    return time.time() * 1000


def on_update(loginID, loginPW, port, path, md5, replyid, sessionid, clientid,
              userdata):
    print("ID: %s, PW: %s, PORT: %d, PATH: %s, MD5: %s" % (loginID, loginPW,
                                                           port, path, md5))
    return wise.core_action_response(replyid, sessionid, True, clientid)


def on_rename(name, replyid, sessionid, clientid, userdata):
    g_strHostName = name
    return wise.core_action_response(replyid, sessionid, True, clientid)


def on_server_reconnect(clientid, userdata):
    global g_strServerIP
    global g_iPort
    global g_strConnID
    global g_strConnPW
    threadReconnect(wise, g_strServerIP, g_iPort, g_strConnID,
                    g_strConnPW).start()
    #return wise.core_device_register()


def on_get_capability(msg, clientid, userdata):
    return SendCapability(wise, clientid)


def on_start_report(msg, clientid, userdata):
    root = json.loads(msg)
    reqItems = root['content']['requestItems']
    global g_iReportInterval
    global g_lockReportdata
    global g_bReport
    global g_stopReportDataFlag
    global g_threadReportData
    g_iReportInterval = root['content']['autoUploadIntervalSec']
    g_lockReportdata.acquire()
    if g_bReport:
        g_stopReportDataFlag.set()
        g_threadReportData.join()
    g_bReport = True
    g_threadReportData = threadReportData(g_stopReportDataFlag, wise,
                                          g_iReportInterval, SendReportData,
                                          g_strClientID)
    g_threadReportData.start()
    g_lockReportdata.release()


def on_stop_report(msg, clientid, userdata):
    global g_lockReportdata
    global g_bReport
    global g_stopReportDataFlag
    global g_threadReportData
    g_lockReportdata.acquire()
    if g_bReport:
        g_stopReportDataFlag.set()
        g_threadReportData.join()
    g_bReport = False
    g_lockReportdata.release()


def on_query_heartbeat(sessionid, clientid, userdata):
    global g_iHeartbeatRate
    return wise.core_heartbeatratequery_response(g_iHeartbeatRate, sessionid,
                                                 clientid)


def on_update_heartbeat(replyid, heartbeatrate, sessionid, clientid, userdata):
    global g_lockHeartbeat
    global g_stopHeartbeatFlag
    global g_threadHeartbeat
    global g_iHeartbeatRate
    g_iHeartbeatRate = heartbeatrate
    g_lockHeartbeat.acquire()
    g_stopHeartbeatFlag.set()
    g_threadHeartbeat.join()
    g_threadHeartbeat = threadHeartbeat(g_stopHeartbeatFlag, wise,
                                        g_iHeartbeatRate)
    g_threadHeartbeat.start()
    g_lockHeartbeat.release()
    return wise.core_action_response(replyid, sessionid, True, clientid)


wise = WISECore.Client(g_strClientID, g_strHostName, g_strMAC)
wise.core_tag_set(g_strTag)
wise.core_product_info_set(g_strMAC, '', '1.0.0', 'IPC', 'ARK-DS520',
                           'Advantech')
wise.core_connection_callback_set(on_connect, on_lostconnect, on_disconnect,
                                  on_message)
wise.core_time_tick_callback_set(on_get_timetick)
wise.core_action_callback_set(on_rename, on_update)
wise.core_server_reconnect_callback_set(on_server_reconnect)
wise.core_iot_callback_set(on_get_capability, on_start_report, on_stop_report)
wise.core_heartbeat_callback_set(on_query_heartbeat, on_update_heartbeat)
wise.core_connect(g_strServerIP, g_iPort, g_strConnID, g_strConnPW)

input("Please enter something to stop: ")
wise.core_disconnect(True)
print("finish")
