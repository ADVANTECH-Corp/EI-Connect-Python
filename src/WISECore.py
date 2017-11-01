# encoding: utf-8
import paho.mqtt.client as mqtt
import json

# Common Command ID
wise_unknown_cmd = 0
wise_agentinfo_cmd = 21
#--------------------------Global command define(101--130)--------------------------------
wise_cagent_update_req = 111
wise_cagent_update_rep = 112
wise_cagent_rename_req = 113
wise_cagent_rename_rep = 114
wise_cagent_osinfo_rep = 116
wise_server_control_req = 125
wise_server_control_rep = 126

wise_heartbeatrate_query_req = 127
wise_heartbeatrate_query_rep = 128
wise_heartbeatrate_update_req = 129
wise_heartbeatrate_update_rep = 130

wise_get_sensor_data_req = 523
wise_get_sensor_data_rep = 524
wise_set_sensor_data_req = 525
wise_set_sensor_data_rep = 526

wise_error_rep = 600

wise_get_capability_req = 2051
wise_get_capability_rep = 2052
wise_start_report_req = 2053
wise_start_report_rep = 2054
wise_report_data_rep = 2055
wise_stop_report_req = 2056
wise_stop_report_rep = 2057

strWillTopic = "/wisepaas/device/%s/willmessage" #publish
strDevInfoAck = "/wisepaas/device/%s/agentinfoack"	#publish
strDevReportTopic = "/wisepaas/device/%s/devinfoack"	#publish

strActionAckTopic = "/wisepaas/%s/%s/agentactionack"	#publish "/wisepaas/<custom>/<devId>/agentactionack"
strEventNotifyTopic = "/wisepaas/%s/%s/eventnotifyack"	#publish "/wisepaas/<custom>/<devId>/eventnotifyack"
strActionReqTopic = "/wisepaas/%s/%s/agentactionreq"	#Subscrib "/wisepaas/<custom>/<devId>/agentactionreq"
strServerCtrlTopic = "/wisepaas/device/+/agentctrlreq"	#Subscribe
strHeartbeatTopic = "/wisepaas/device/%s/notifyack"	#publish

strAgentInfo = "{\"content\":{\"parentID\":\"%s\",\"hostname\":\"%s\",\"sn\":\"%s\",\"mac\":\"%s\",\"version\":\"%s\",\"type\":\"%s\",\"product\":\"%s\",\"manufacture\":\"%s\",\"account\":\"%s\",\"passwd\":\"%s\",\"status\":%d,\"tag\":\"%s\"},\"commCmd\":1,\"agentID\":\"%s\",\"handlerName\":\"general\",\"sendTS\":{\"$date\":%d}}"
strResponse_Session = "{\"agentID\":\"%s\",\"commCmd\":%d,\"handlerName\":\"general\",\"content\":{\"result\":\"%s\"},\"sessionID\":\"%s\",\"sendTS\":{\"$date\":%d}}"
strResponse = "{\"agentID\":\"%s\",\"commCmd\":%d,\"handlerName\":\"general\",\"content\":{\"result\":\"%s\"},\"sendTS\":{\"$date\":%d}}"
strReportData = "{\"agentID\":\"%s\",\"commCmd\":2055,\"handlerName\":\"general\",\"content\":%s,\"sendTS\":{\"$date\":%d}}"
strResponseContent = "{\"agentID\":\"%s\",\"commCmd\":%d,\"handlerName\":\"%s\",\"content\":%s,\"sendTS\":{\"$date\":%d}}"
strHeartbeat = "{\"hb\":{\"devID\":\"%s\"}}"
strQueryHeartbeatResponse_session = "{\"agentID\":\"%s\",\"commCmd\":128,\"handlerName\":\"general\",\"content\":{\"heartbeatrate\":%d},\"sessionID\":\"%s\",\"sendTS\":{\"$date\":%d}}"
strQueryHeartbeatResponse = "{\"agentID\":\"%s\",\"commCmd\":128,\"handlerName\":\"general\",\"content\":{\"heartbeatrate\":%d},\"sendTS\":{\"$date\":%d}}"


# The callback for when the client receives a CONNACK response from the server.
def _on_mqtt_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    if rc == 0:
        if userdata._on_connect != None:
            userdata._on_connect()
    else:
        if userdata._on_lostconnect != None:
            userdata._on_lostconnect(rc)

# The callback for when the client disconnects from the broker.
def _on_mqtt_disconnect(client, userdata, rc):
    print("Disconnect with result code "+str(rc))
    if userdata._on_disconnect != None:
        userdata._on_disconnect()


# The callback for when a PUBLISH message is received from the server.
def _on_mqtt_message(client, userdata, msg):
    data = msg.payload.decode('ascii')
    topic = msg.topic
    clientID = msg.topic.split("/")[3]
    #print(msg.topic+" "+data)
    root = json.loads(data)

    try:
        strSessionID = root['sessionID']
    except:
        strSessionID = None

    try:
        if root['handlerName'] == "general":
            cmdid = root['commCmd']
            if cmdid == wise_cagent_update_req: 
                if userdata._on_update != None:
                    try:
                        return userdata._on_update(root['content']['params']['userName'], root['content']['params']['pwd'], root['content']['params']['port'], root['content']['params']['path'], root['content']['params']['md5'], wise_cagent_update_rep, strSessionID, clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

            elif cmdid == wise_cagent_rename_req:
                if userdata._on_rename != None:
                    try:
                        return userdata._on_rename(root['content']['devName'], wise_cagent_rename_rep, strSessionID, clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

            elif cmdid == wise_server_control_req:
                if userdata._on_server_reconnect != None:
                    try:
                        return userdata._on_server_reconnect(clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

            elif cmdid == wise_get_capability_req:
                if userdata._on_get_capability != None:
                    #try:
                        return userdata._on_get_capability(data, clientID, userdata._userdata)
                    #except:
                    #    print("Parsing failed! %s" % data)

            elif cmdid == wise_start_report_req:
                if userdata._on_start_report != None:
                    try:
                        return userdata._on_start_report(data, clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

            elif cmdid == wise_stop_report_req:
                if userdata._on_stop_report != None:
                    try:
                        return userdata._on_stop_report(data, clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

            elif cmdid == wise_heartbeatrate_query_req:
                if userdata._on_query_heartbeatrate != None:
                    try:
                        return userdata._on_query_heartbeatrate(strSessionID, clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

            elif cmdid == wise_heartbeatrate_update_req:
                if userdata._on_update_heartbeatrate != None:
                    try:
                        return userdata._on_update_heartbeatrate(wise_heartbeatrate_update_rep, root['content']['heartbeatrate'], strSessionID, clientID, userdata._userdata)
                    except:
                        print("Parsing failed! %s" % data)

    except:
        print("Not supported format! %s" % data)
    if userdata._on_msg_recv != None:
        return userdata._on_msg_recv(topic, data, userdata._userdata)

class Client(object):
    """
    This is the main class for use communicating with an MQTT broker.

    """
    def __init__(self, strClientID="", strHostName="", strMAC="", userdata=None):
        if strClientID == "":
            raise ValueError('A client id must be provided')

        self._strClientID = strClientID
        self._strHostName = strHostName
        self._strMAC = strMAC
        """ connection info """
        self._strServerIP=""
        self._iPort=1883
        self._bTLS = False
        self._strCaFile=""
        self._strCertFile=""
        self._strKeyFile=""
        """ product info """
        self._strSerialNum=""
        self._strVersion=""
        self._strType="IPC"
        self._strProduct=""
        self._strManufacture=""
        self._strTag=""
        self._strParentID=""
        """ account bind """
        self._strLoginID=""
        self._strLoginPW=""
        """ client status """
        self._bInited=True
        self._bConnected = False
        self._client = mqtt.Client()
        self._client.user_data_set(self)
        self._client.on_connect = _on_mqtt_connect
        self._client.on_disconnect = _on_mqtt_disconnect
        self._client.on_message = _on_mqtt_message
        self._userdata=userdata
        """ Callback function """
        self._on_connect=None
        self._on_lostconnect=None
        self._on_disconnect=None
        self._on_msg_recv=None
        self._on_rename=None
        self._on_update=None
        self._on_server_reconnect=None
        self._on_get_capability=None
        self._on_start_report=None
        self._on_stop_report=None
        self._on_query_heartbeatrate=None
        self._on_update_heartbeatrate=None
        self._on_get_timetick=None
        
    def __del__(self):
        if self._client!= None:
            if  self._bConnected:
                self._client.disconnect(True)
            self._client = None
            self._bConnected = False
        self._bInited=False
        pass 

    def core_tag_set(self, strTag):
        if not self._bInited:
            raise ValueError('No initialized')
        self._strTag = strTag


    def core_product_info_set(self, strSerialNum, strParentID, strVersion, strType, strProduct, strManufacture):
        if not self._bInited:
            raise ValueError('No initialized')
        self._strSerialNum = strSerialNum
        self._strParentID = strParentID
        self._strVersion = strVersion
        self._strType = strType
        self._strProduct = strProduct
        self._strManufacture = strManufacture

    def core_account_bind(self, strLoginID, strLoginPW):
        if not self._bInited:
            raise ValueError('No initialized')
        self._strLoginID = strLoginID
        self._strLoginPW = strLoginPW
    

    def core_connection_callback_set(self, on_connect, on_lostconnect, on_disconnect, on_msg_recv):
        if not self._bInited:
            raise ValueError('No initialized')
        self._on_connect = on_connect
        self._on_lostconnect = on_lostconnect
        self._on_disconnect = on_disconnect
        self._on_msg_recv = on_msg_recv
        
    def core_action_callback_set(self, on_rename, on_update):
        if not self._bInited:
            raise ValueError('No initialized')
        self._on_rename = on_rename
        self._on_update = on_update

    def core_server_reconnect_callback_set(self, on_server_reconnect):
        if not self._bInited:
            raise ValueError('No initialized')
        self._on_server_reconnect = on_server_reconnect

    def core_iot_callback_set(self, on_get_capability, on_start_report, on_stop_report):
        if not self._bInited:
            raise ValueError('No initialized')
        self._on_get_capability = on_get_capability
        self._on_start_report = on_start_report
        self._on_stop_report = on_stop_report

    def core_time_tick_callback_set(self, get_time_tick):
        if not self._bInited:
            raise ValueError('No initialized')
        self._on_get_timetick = get_time_tick

    def core_heartbeat_callback_set(self, on_query_heartbeatrate, on_update_heartbeatrate):
        if not self._bInited:
            raise ValueError('No initialized')
        self._on_query_heartbeatrate = on_query_heartbeatrate
        self._on_update_heartbeatrate = on_update_heartbeatrate

    def core_action_response(self, cmdid, sessionid, success, clientid):
        if not self._bInited:
             raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No Connected')
        ts = 0
        if self._on_get_timetick != None:
            ts = self._on_get_timetick()
        result=""
        if success:
            result = "SUCCESS"
        else:
            result = "FALSE"
        
        strTopic = strActionAckTopic % (self._strTag, self._strClientID)
        if sessionid == None:
            strPayload = strResponse % (clientid, cmdid, result, ts)
        else:
            strPayload = strResponse_Session % (clientid, cmdid, result, sessionid, ts)

        return self._client.publish(strTopic, strPayload)

    def core_heartbeatratequery_response(self, heartbeatrate, sessionid, clientid):
        if not self._bInited:
             raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No Connected')
        ts = 0
        if self._on_get_timetick != None:
            ts = self._on_get_timetick()

        strTopic = strActionAckTopic % (self._strTag, self._strClientID)
        if sessionid == None:
            strPayload = strQueryHeartbeatResponse % (clientid, heartbeatrate, ts)
        else:
            strPayload = strQueryHeartbeatResponse_session % (clientid, heartbeatrate, sessionid, ts)
        return self._client.publish(strTopic, strPayload)

    def core_tls_set(self, cafile, certfile, keyfile):
        if not self._bInited:
            raise ValueError('No initialized')
        self._bTLS = True
        self._strCaFile=cafile
        self._strCertFile=certfile
        self._strKeyFile=keyfile
        
    def core_connect(self, strServerIP, iServerPort, strConnID, strConnPW):
        if not self._bInited:
            raise ValueError('No initialized')
        if self._client != None:
            self._client.disconnect()
            self._client.loop_stop(True)
        self._client.will_clear()
        willtopic = strWillTopic % self._strClientID
        ts = 0
        if self._on_get_timetick != None:
            ts = self._on_get_timetick()
        willmsg = self._GenAgentInfo(0, ts)
        self._client.will_set(willtopic, willmsg)
        if self._bTLS:
            self._client.tls_insecure_set(False)
            self._client.tls_set(self._strCaFile, self._strCertFile, self._strKeyFile)
        self._client.username_pw_set(strConnID, strConnPW)
        self._client.connect(strServerIP, iServerPort)
        self._client.loop_start();

    def core_disconnect(self, bForce):
        if not self._bInited:
            raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No connected')
        self._client.loop_stop(bForce)
        ts = 0
        if self._on_get_timetick != None:
            ts = self._on_get_timetick()
        msg = self._GenAgentInfo(0, ts)
        topic = strDevInfoAck % self._strClientID
        self._client.publish(topic, msg)
        self._client.loop_write()
        self._client.disconnect()
        self._client.loop()

    def _GenAgentInfo(self, iStatus, ts):
        msg = strAgentInfo % (self._strParentID, self._strHostName, self._strSerialNum, self._strMAC, self._strVersion, self._strType, self._strProduct, self._strManufacture, self._strLoginID, self._strLoginPW, iStatus, self._strTag, self._strClientID, ts)
        return msg

    def core_device_register(self):
        if not self._bInited:
            raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No connected')
        ts = 0
        if self._on_get_timetick != None:
            ts = self._on_get_timetick()
        
        msg = self._GenAgentInfo(1, ts)
        topic = strDevInfoAck % self._strClientID
        return self._client.publish(topic, msg)

    def core_heartbeat_send(self):
        if not self._bInited:
            raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No connected')
        ts = 0
        if self._on_get_timetick != None:
            ts = self._on_get_timetick()
        
        msg = strHeartbeat % self._strClientID
        topic = strHeartbeatTopic % self._strClientID
        return self._client.publish(topic, msg)

    def core_publish(self, strTopic, strPayload, bRetain = False, iQos = 0):
        if not self._bInited:
            raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No connected')
        return self._client.publish(strTopic, strPayload, iQos, bRetain)
    
    def core_subscribe(self, strTopic, iQos = 0):
        if not self._bInited:
            raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No connected')
        return self._client.subscribe(strTopic, iQos)

    def core_unsubscribe(self, topic):
        if not self._bInited:
                raise ValueError('No initialized')
        if self._client == None:
            raise ValueError('No connected')
        return self._client.unsubscribe(strTopic)