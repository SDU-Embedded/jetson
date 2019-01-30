#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import threading,curses,json,time,signal,sys,copy
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

_LOWPASS_COUNT = 1

class CameraOutput():
    def __init__(self, camera_ip):
        self.lock = threading.Lock()
        self.camera_ip = camera_ip
        self.num_of_obj = 0
        self.timestamp = datetime.utcnow()
        self.lowpass_count = 0
    
    def write_output(self,number,time):
        self.lock.acquire()
        self.num_of_obj = number
        self.timestamp = time
        self.lock.release()

    def read_output(self):
        self.lock.acquire()
        output = [self.num_of_obj, self.timestamp]
        self.lock.release()
        return output

    def get_lowpass_count(self):
        self.lock.acquire()
        tmp = self.lowpass_count
        self.lock.release()
        return tmp
    
    def reset_lowpass_count(self):
        self.lowpass_count = 0

    def increment_lowpass_count(self):
        self.lowpass_count += 1

class EventListener(threading.Thread):
    def __init__(self, servers,topic, camera_output_array):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
        self.servers = servers
        self.topic = topic

        self.camera_output_array = camera_output_array

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=self.servers,auto_offset_reset='latest',consumer_timeout_ms=1000)
        consumer.subscribe([self.topic])

        while not self.stop_event.is_set():
            for message in consumer:
                # Parse content of event
                event = json.loads(message.value)
	
                self.camera_output_array[event['Camera']].write_output(event['Objects']['Mongoose']['count'], datetime.utcnow().isoformat())
#	        print "Number of mongoose: {}".format(event['Objects']['Mongoose']['count'])  	
            
            if self.stop_event.is_set():
                break

        consumer.close()

class StreamSelector(threading.Thread):
    def __init__(self, list_of_camera_data, wait_time_sec, topic, event_lowpass_filter):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

        self.topic = topic
        self.list_of_camera_data = list_of_camera_data
        self.wait_time_sec = wait_time_sec

        self.highest_camera = 0
        self.highest_camera_value = 0
        self.highest_camera_timestamp = 0

        self.lowpass_filter = event_lowpass_filter
        self.lowpass_filter_count = 0
        self.lowpass_filter_active_camera = "" 
        self.event_emitter = EventEmitter(servers,topic)

    def update_highest(self, camera_ip, value, timestamp):
        self.highest_camera = camera_ip
        self.highest_camera_value = value
        self.highest_camera_timestamp = timestamp

    def read_highest(self):
        return [self.highest_camera, self.highest_camera_value, self.highest_camera_timestamp]

    def read_lowpass(self):
        return [self.lowpass_filter_count,self.lowpass_filter_active_camera]

    def increment_lowpass(self):
        self.lowpass_filter_count += 1

    def reset_lowpass(self, camera):
        self.lowpass_filter_count = 0
        self.lowpass_filter_active_camera = camera

    def reset_highest(self):
        self.highest_camera_value = 0
	
    def emit_highest_event(self):
        tmp = dict()
        tmp["@timestamp"] = self.highest_camera_timestamp
        tmp["camera"] = self.highest_camera
        tmp["value"] = self.highest_camera_value
	
        self.event_emitter.send(json.dumps(tmp))
	

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            time.sleep(self.wait_time_sec)
#            self.reset_highest()
            for camera_ip in self.list_of_camera_data:
		#look through all event listener topics to see which has the highest number of mongooses
                if self.list_of_camera_data[camera_ip].read_output()[0] >= self.read_highest()[1] and (self.read_lowpass()[1] == camera_ip and self.read_lowpass()[0] >= self.lowpass_filter):
                    self.update_highest(
                        camera_ip, 
                        self.list_of_camera_data[camera_ip].read_output()[0],
                        self.list_of_camera_data[camera_ip].read_output()[1]
                        )
                    self.emit_highest_event()
                    self.reset_lowpass(camera_ip)
                    self.reset_highest()
                elif self.read_highest()[0] == camera_ip and self.list_of_camera_data[camera_ip].read_output()[0] < self.read_highest()[1]:
                    self.update_highest(
                        camera_ip,
                        self.list_of_camera_data[camera_ip].read_output()[0],
                        self.list_of_camera_data[camera_ip].read_output()[1]
                        )
                    self.reset_lowpass(camera_ip)
                    print "lower: {}".format(self.read_lowpass())
                elif self.list_of_camera_data[camera_ip].read_output()[0] >= self.read_highest()[1]:
                    if self.read_lowpass()[1] != camera_ip:
                        self.update_highest(
                            camera_ip,
                            self.list_of_camera_data[camera_ip].read_output()[0],
                            self.list_of_camera_data[camera_ip].read_output()[1]
                            )
                        self.reset_lowpass(camera_ip)
                        print "first: {}".format(self.read_lowpass())
                    else:
                        self.increment_lowpass()
                        print "lowpass: {}, camera_out: {}".format(self.read_lowpass(),self.list_of_camera_data[camera_ip].read_output()[0])

                elif self.list_of_camera_data[camera_ip].read_output()[0] < self.read_highest()[1] and self.read_lowpass()[1] == camera_ip:
                    self.reset_lowpass(camera_ip)
#                    print "last: {}".format(self.read_lowpass())

            if self.stop_event.is_set():
                break
	   
class EventEmitter():
    def __init__(self, servers, topic):
        self.servers = servers
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.servers)

    def send(self, dat):
        print (dat)
        self.producer.send(self.topic, dat.encode())    
            
class EventProcessor():
    def __init__(self, servers):
        self.servers = servers
        self.wait_time_sec = 1
        self.listener_output_array = {}
	
        self.list_of_camera_data = {}
        self.list_of_camera_data['ff15::b8:27:eb:dd:bd:a0'] = CameraOutput('ff15::b8:27:eb:dd:bd:a0')
        self.list_of_camera_data['ff15::b8:27:eb:82:9c:08'] = CameraOutput('ff15::b8:27:eb:82:9c:08')
        self.list_of_camera_data['ff15::b8:27:eb:3a:c5:91'] = CameraOutput('ff15::b8:27:eb:3a:c5:91')
        self.list_of_camera_data['ff15::b8:27:eb:51:a1:58'] = CameraOutput('ff15::b8:27:eb:51:a1:58')
        self.list_of_camera_data['ff15::b8:27:eb:fc:19:7b'] = CameraOutput('ff15::b8:27:eb:fc:19:7b')
        self.list_of_camera_data['ff15::b8:27:eb:67:7f:2e'] = CameraOutput('ff15::b8:27:eb:67:7f:2e')

        self.outdoor_wall_listener = EventListener(self.servers, 'zoocam', self.list_of_camera_data)

        self.event_output = StreamSelector(self.list_of_camera_data,self.wait_time_sec,"zoocam_combined",_LOWPASS_COUNT)

        self.tasks = [self.outdoor_wall_listener,self.event_output]

    def signal_handler(self, sig, frame):
        print 'Signal trapped. Stopping tasks'
        self.stop_tasks()
        self.join_tasks()

    def start_tasks(self):
        for task in self.tasks:
            task.start()

    def stop_tasks(self):
        for task in self.tasks:
            task.stop()

    def join_tasks(self):
        for task in self.tasks:
            task.join()

if __name__ == "__main__":
    servers = 'manna,hou,bisnap'

    event_processor = EventProcessor(servers)

    signal.signal(signal.SIGINT, event_processor.signal_handler)
    event_processor.start_tasks()

    signal.pause()
