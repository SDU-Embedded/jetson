#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import threading,curses,json,time,signal,sys,copy,os, subprocess32
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

_IFACE="labvpn"

size_x="1280"
size_y="720"

output_size="video/x-raw, width=" + size_x + ",height=" + size_y

_VIDEO_CAPS="application/x-rtp, media=video, clock-rate=90000, encoding-name=H264, payload=96, format=I444, framerate=60/1, interlace-mode=progressive, pixel-aspect-ratio=1/1"

indoor_left="ff15::b8:27:eb:dd:bd:a0"
indoor_right="ff15::b8:27:eb:82:9c:08"
indoor_wall="ff15::b8:27:eb:3a:c5:91"
outdoor_left="ff15::b8:27:eb:dd:bd:a0"
outdoor_right="ff15::b8:27:eb:fc:19:7b"
outdoor_wall="ff15::b8:27:eb:67:7f:2e"

class EventListener(threading.Thread):
    def __init__(self, servers,topic):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
                
        self.servers = servers
        self.topic = topic

        self.active_camera = ""

        self.first_run = 1

    def update_camera(self, camera):
        self.active_camera = camera

    def ip_fetcher(self, camera):
        return {
            'outdoor_wall' : outdoor_wall,
            'outdoor_right' : outdoor_right,
            'outdoor_left' : outdoor_left,
            'indoor_wall' : indoor_wall,
            'indoor_right' : indoor_right,
            'indoor_left' : indoor_left, 
        }[camera]

    def stream_selector(self, camera):
        ip = self.ip_fetcher(camera)

        return ip#"-v udpsrc multicast-group=" + ip + " auto-multicast=true multicast-iface=" + _IFACE + " ! " + _VIDEO_CAPS + " ! rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! videoscale ! " + output_size + " ! queue2 ! autovideosink sync=false"

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=self.servers,auto_offset_reset='latest',consumer_timeout_ms=1000)
        consumer.subscribe([self.topic])

        while not self.stop_event.is_set():
            for message in consumer:
                # Parse content of event		
                event = json.loads(message.value)

                if self.active_camera != event['camera']:
#                    GSTREAMER_PIPELINE = self.stream_selector(event['topic'])

#                    print (GSTREAMER_PIPELINE)
                    
                    if self.first_run == 0:
                        p.kill()
                    else:
                        self.first_run = 0

                    self.update_camera(event['camera'])
                    p = subprocess32.Popen(["gst-launch-1.0",
                        "-v","udpsrc","multicast-group=" + event['camera'],
                         "auto-multicast=true", "multicast-iface=" + _IFACE, "!", _VIDEO_CAPS,
                        "!", "rtph264depay", "!", "h264parse", "!", "avdec_h264", "!"
                        "videoconvert", "!", "videoscale", "!", output_size, "!", "queue2", "!", 
                        "autovideosink", "sync=false"])
                        
                    print ("Active camera: {}".format(self.active_camera))
                    
		
            
            if self.stop_event.is_set():
                break

        consumer.close()  
            
class EventProcessor():
    def __init__(self, servers):
        self.servers = servers
        self.monitor_listener = EventListener(self.servers, "zoocam_combined")

        self.tasks = [self.monitor_listener]

    def signal_handler(self, sig, frame):
        print ('Signal trapped. Stopping tasks')
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
