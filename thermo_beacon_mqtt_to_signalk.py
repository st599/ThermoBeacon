#!/usr/bin/env python3

#
#   THERMOBEACON TO SIGNALK MQTT GATEWAY
#
#   Filename        :   thermo_beacon_mqtt_to_signalk.py
#   Description     :   Sends ThermoBeacon data to MQTT Gateway
#   Date            :   04/03/2026
#   Author          :   Ivko Kalchev, Simon Thompson
#   Copyright       :   (c) 2021 Ivko Kalchev, (c) Simon Thompson 2025
#   Dependencies    :   sys, re, json, asyncio, argparse, bleak, paho.mqtt.client
#

#
#   SYSTEM IMPORTS
#
import sys, re, json, asyncio
from argparse import ArgumentParser, Namespace
import bleak
import paho.mqtt.client as mqtt
from bleak import BleakClient, BleakScanner
import logging

#
#   AUTHOR IMPORTS
#
from tb_protocol import *

#
#   CONSTANTS
#

# create logger with 'spam_application'
logger = logging.getLogger('ThermoBeaconMQTTtoSignalK')
logger.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)
#Transmit Handle 0x0021
TX_CHAR_UUID = '0000fff5-0000-1000-8000-00805F9B34FB'
#Read Handle 0x0024
RX_CHAR_UUID = '0000fff3-0000-1000-8000-00805F9B34FB'

mqttPrefix = "W/signalk/"



#
#   FUNCTIONS
#
def mac_addr(x):
    x = x.lower()
    if not re.match("^(?:[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$", x):
        raise ValueError()
    return x

'''
Config parser for command line arguments
'''
# Moved in to function ST 04/03/26
def config_parser():
    logger.info("Parsing command line arguments")
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(help='action', dest='command', required=True)

    sub = subparsers.add_parser('scan', help = "Scan for ThermoBeacon devices")
    sub.add_argument('-mac', type=mac_addr, required=False)
    sub.add_argument('-t', type=int, default = 20, metavar='<Scan duration, seconds>', required=False)
    sub = subparsers.add_parser('identify', help = "Identify a device")
    sub.add_argument('-mac', type=mac_addr, required=True)
    sub = subparsers.add_parser('dump', help = "Dump logged data")
    sub.add_argument('-mac', type=mac_addr, required=True)
    sub = subparsers.add_parser('query', help = "Query device for details")
    sub.add_argument('-mac', type=mac_addr, required=True)
    sub.add_argument('-t', type=int, default = 3, metavar='<Query duration, seconds>', required=False)
    sub = subparsers.add_parser('mqtt', help = "Send data via mqtt")
    sub.add_argument('-mac', type=mac_addr, required=True)
    sub.add_argument('-t', type=int, default = 3, metavar='<Query duration, seconds>', required=False)
    sub.add_argument('-broker', required=True)
    sub.add_argument('-port', type=int, required=True)
    sub.add_argument('-topic', required=True)
    sub = subparsers.add_parser('signalk', help = "Send data to SignalK via mqtt")
    sub.add_argument('-mac', type=mac_addr, required=True)
    sub.add_argument('-t', type=int, default = 3, metavar='<Query duration, seconds>', required=False)
    sub.add_argument('-broker', required=True)
    sub.add_argument('-port', type=int, required=True)
    sub.add_argument('-mmsi', required=True)
    sub.add_argument('-location', required=True)
    sub.add_argument('--outside', action='store_true', help='Indicates that the sensor is outside the boat. ' \
    'This will change the SignalK path to "environment.outside" instead of "environment.inside"')

    args = parser.parse_args()

    return args


'''
asyncio scan for devices. This is used by the query function to find devices
'''
async def scan():
    scanner = BleakScanner(detection_callback)
    await scanner.start()
    await asyncio.sleep(540)
    await scanner.stop()


'''
callback for device detection. This is used by the scan function to find devices and print their details
'''
def detection_callback(device, advertisement_data):
    name = advertisement_data.local_name
    if name is None:
        return
    if name != 'ThermoBeacon':
        return
    msg = advertisement_data.manufacturer_data
    #print(device.rssi)
    for key in msg.keys():
        bvalue = msg[key]
        mac = device.address.lower()
        if len(bvalue)==18:
            data = TBAdvData(key, bvalue)
            print('[{0}] [{6:02x}] T= {1:5.2f}\xb0C, H = {2:3.2f}%, Button:{4}, Battery : {5:02.0f}%, UpTime = {3:8.0f}s'.\
                  format(mac, data.tmp, data.hum, data.upt, 'On ' if data.btn else 'Off', data.btr, data.id))
        else:
            data = TBAdvMinMax(key, bvalue)
            print('[{0}] [{5:02x}] Max={1:5.2f}\xb0C at {2:.0f}s, Min={3:5.2f}\xb0C at {4:.0f}s'.\
                  format(mac, data.max, data.max_t, data.min, data.min_t, data.id))


'''
dump logged data from the device. This is used by the dump function to retrieve logged data from the device and print it
'''
def dump(address):
    try:
        asyncio.run(_dump(address))
    except bleak.exc.BleakDBusError as dber:
        print(dber.dbus_error)
    except Exception as exc:
        print('///'+str(exc))

async def _dump(address):
    client = BleakClient(address)
    try:
        await client.connect(timeout=10)
        print('connectd')
    except Exception as exc:
        print('exception ' + str(exc))
        return
    try:
        print(client.is_connected)

        cmd = TBCmdQuery()
        await client.write_gatt_char(TX_CHAR_UUID, cmd.get_msg())
        data = await client.read_gatt_char(RX_CHAR_UUID)
        resp = TBMsgQuery(data)
        print('01:'+data.hex())

        await client.start_notify(RX_CHAR_UUID, dump_callback)
        cmd_dump = bytes([TB_COMMAND_DUMP, 0, 0, resp.count&0xff, (resp.count>>8)&0xff, (resp.count>>16)&0xff, 1])
        cnt = 0
        while cnt<resp.count:
            c = 15 if resp.count-cnt>15 else resp.count-cnt
            cmd = TBCmdDump(cnt, c)
            cmd_dump = cmd.get_msg()
            cnt += c
            #print('cmd ', cmd_dump.hex())
            await client.write_gatt_char(TX_CHAR_UUID, cmd_dump)
        await asyncio.sleep(.5)
        data = await client.read_gatt_char(RX_CHAR_UUID)
        print(data.hex())
    finally:
        await client.disconnect()

'''
callback for dump data. This is used by the dump function to retrieve logged data from the device and print it
'''
def dump_callback(sender: int, data: bytearray):
    if data is None:
        return
    try:
        hdata = data.hex()
        msg = TBMsgDump(data)
        print(msg.offset, msg.count, msg.data)
        #print(f"{sender}: {hdata}")
    except Exception as exc:
        print(str(exc))

'''
Identify the device. This is used by the identify function to send an identify command to the device and print the response
'''
def identify(address):
    try:
        asyncio.run(_identify(address))
    except bleak.exc.BleakDBusError as dber:
        print(dber.dbus_error)
    except Exception as exc:
        print('///'+str(exc))

async def _identify(address):
    client = BleakClient(address)
    try:
        await client.connect(timeout=10)
        print('connectd')
    except Exception as exc:
        print('exception ' + str(exc))
        return

    try:
        cmd = TBCmdIdentify()
        await client.write_gatt_char(TX_CHAR_UUID, cmd.get_msg())
    finally:
        await client.disconnect()

'''
Thanks to Andreas Schmitz (Andy) two new commands implemented below: query, mqtt.
The mqtt command queries the values and then publishes via mqtt
'''
def send_mqtt(SensorMac, SensorQueryDuration_s, broker, port, topic):
    Result = str(query(SensorMac, SensorQueryDuration_s))
    if len(Result) == 0:
        return
    client = mqtt.Client()
    client.connect(host = broker, port = port)
    #FIXME add user, passwd
    client.loop_start()
    client.publish(topic = topic, payload = Result, qos = 1)
    client.disconnect()

def send_signalk_via_mqtt(SensorMac, SensorQueryDuration_s, broker, port, mmsi, location, outside):
    # Generate the topic string based on the mmsi, location and whether the sensor is outside or inside the boat. 
    # The topic string will be in the format "W/signalk/{mmsi}/
    topicStr = mqttPrefix + str(mmsi) + "/environment/"
    if outside:
        topicStr += "outside/"
    else:
        topicStr += "inside/"
        topicStr += location + "/"

    # Call the query function to retrieve data from the device. The query function will return a dictionary 
    # with the data retrieved from the device.

#TODO - the query function is currently returning a string representation of the dictionary. This needs to be fixed to return the dictionary itself so that we can access the temperature and humidity values directly.
    logger.info("Querying device for data...")
    #Result = str(query(SensorMac, SensorQueryDuration_s))
    #if len(Result) == 0:
    #    return
    #temp_c = Result["temp"]
    #rel_hum = Result["relhum"]
    temp_c = 25.2
    rel_hum = 60.2

    # Send the data via mqtt to the SignalK server. The topic will be based on the mmsi, location and whether 
    # the sensor is outside or inside the boat. The payload will be the temperature and humidity values retrieved from the device.
    logger.info("Opening connection to MQTT Broker: " + broker + ":" + str(port))
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(host = broker, port = port)
    # Temperature
    topicTemp = topicStr + "temperature"
    logger.info("Publishing to topic: " + topicTemp + " value: " + str(temp_c))
    client.publish(topic = topicTemp, payload = temp_c, qos = 1)
    client.disconnect()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(host = broker, port = port)
    # Humidity  
    topicHum = topicStr + "relativeHumidity"
    logger.info("Publishing to topic: " + topicHum + " value: " + str(rel_hum))
    client.publish(topic = topicHum, payload = rel_hum, qos = 1)
    client.disconnect()
    
    logger.info("Data published to MQTT Broker and connection closed")

'''
The query function queries the device for details and returns the results as a dictionary. This is used by the mqtt function to retrieve data from the device and send it via mqtt
'''
def query(SensorMac, SensorQueryDuration_s):
    proxy = QueryProxy(SensorMac)

    try:
        asyncio.run(async_query(SensorQueryDuration_s, proxy))
    except KeyboardInterrupt:
        print()
        return
    return(proxy.QueryResults)

'''
asyncio query for device details. This is used by the query function to retrieve data from the device and store it in the QueryProxy object
'''
async def async_query(SensorQueryDuration_s, proxy):
    scanner = BleakScanner(proxy.query_callback)
    await scanner.start()
    await asyncio.sleep(SensorQueryDuration_s)
    await scanner.stop()

'''
QueryProxy class to store query results. This is used by the query function to store data retrieved from the device and return it as a dictionary
'''
class QueryProxy:
    def __init__(self, _target_mac):
        self.QueryResults = dict()
        self.TargetMac = _target_mac

    def query_callback(self, device, advertisement_data):
        name = advertisement_data.local_name
        if name is None:
            return
        if name != 'ThermoBeacon':
            return
        msg = advertisement_data.manufacturer_data
        for key in msg.keys():
            bvalue = msg[key]
            mac = device.address.lower()
            if mac != self.TargetMac:
                continue
            if len(bvalue)==18:
                self.QueryResults.clear()
                data = TBAdvData(key, bvalue)
                self.QueryResults["mac"] = mac
                self.QueryResults["temp"] = round(float(data.tmp),2)
                self.QueryResults["relhum"] = round(float(data.hum),2)
                self.QueryResults["button"] = data.btn
                self.QueryResults["battery"] = round(float(data.btr),2)
                self.QueryResults["uptime"] = int(data.upt)
                self.QueryResults["mac"] = mac
            else:
                pass
                            #This data contains min and max values - ignore.
                #data = TBAdvMinMax(key, bvalue)
                #print('[{0}] [{5:02x}] Max={1:5.2f}\xb0C at {2:.0f}s, Min={3:5.2f}\xb0C at {4:.0f}s'.\
                #      format(mac, data.max, data.max_t, data.min, data.min_t, data.id))



#
#   MAIN
#
def main():
    logger.info("Starting ThermoBeacon MQTT to SignalK Gateway")
    args = config_parser()
    cmd = args.command
    if cmd=='scan':
        logger.info("Scanning for ThermoBeacon devices...")
        try:
            asyncio.run(scan())
        except KeyboardInterrupt:
            print()
            return
    elif cmd=='identify':
        logger.info("Identifying device with MAC address: " + args.mac)
        identify(args.mac)
        return
    elif cmd=='dump':
        logger.info("Dumping data for device with MAC address: " + args.mac)
        dump(args.mac)
        return
    elif cmd=='query':
        logger.info("Querying device with MAC address: " + args.mac)
        Result = query(args.mac, args.t)
        print(Result)
    elif cmd=='mqtt':
        logger.info("Sending data via MQTT from device with MAC address: " + args.mac)
        send_mqtt(args.mac, args.t, args.broker, args.port, args.topic)
    elif cmd=='signalk':
        logger.info("Sending data to SignalK via MQTT from device with MAC address: " + args.mac)
        send_signalk_via_mqtt(args.mac, args.t, args.broker, args.port, args.mmsi, args.location, args.outside)
    else:
        logger.warning("Command not yet implemented")

if __name__ == '__main__':
    main()

