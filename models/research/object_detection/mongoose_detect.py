import numpy as np
import os
import six.moves.urllib as urllib
import sys
import tarfile
import tensorflow as tf
import zipfile
import time
import copy
import re

from collections import defaultdict
from io import StringIO
from PIL import Image

from utils import label_map_util
from utils import visualization_utils as vis_util

import cv2
print cv2.__version__

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




#MODEL_NAME = 'Mongoose_inference_graph'
#MODEL_NAME = 'mongoose_inference_graph_outdoor_indoor'

NUM_CLASSES = 5
DETECTION_THRESHOLD = 0.5
DETECTION_CLASS = 1
IMAGE_BOX_SCALE_FACTOR = 1.1
GSTREAMER_PIPELINE = "udpsrc port=5000 ! application/x-rtp,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! jpegdec ! videoconvert ! video/x-raw, format=(string)BGR ! appsink"

GSTREAMER_PIPELINE_3 = "filesrc location=Image_test.jpg ! application/x-rtp,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! jpegdec ! videoconvert ! video/x-raw, format=(string)BGR ! appsink"



font = cv2.FONT_HERSHEY_SIMPLEX

def show_fps(fps):
	current_time = time.time()
#	print (current_time-fps.last_time)

	fps.fps = (1)/(current_time-fps.last_time)
	fps.accumulated_fps = (fps.fps + fps.accumulated_fps)

	fps.count += 1
	fps.average_fps = fps.accumulated_fps / fps.count

	fps.last_time = current_time
	
	return fps

def crop_image(image, boxes):
	(img_height,img_width,img_channel) = image.shape

	x1 = int((img_width/IMAGE_BOX_SCALE_FACTOR)*original_boxes[i][1])
	x2 = int(IMAGE_BOX_SCALE_FACTOR*img_width*original_boxes[i][3])
	y1 = int((img_height/IMAGE_BOX_SCALE_FACTOR)*original_boxes[i][0])
	y2 = int(IMAGE_BOX_SCALE_FACTOR*img_height*original_boxes[i][2])
	
	cropped_img = image[y1:y2, x1:x2 ]
	
	return cropped_img

class DetectedObjects(object):
	def __init__(self, boxes, scores, classes, num_detections):
		if boxes is None:
			self.boxes = []
			self.scores = []
			self.classes = []
			self.num_detections = []
		else:
			self.boxes = boxes
			self.scores = scores
			self.classes = classes
			self.num_detections = num_detections

	def reset(self):
		self.boxes = np.array([])
		self.scores = np.array([])
		self.classes = np.array([])
		self.num_detections = np.array([]) 

def createJsonObject(detected_objects, num_of_objects, number_of_classes, category_index, image_np, detection_threshold, camera_ip):
	current_class = 1
	
	json_string = "{\"Objects\":{"
	
	while current_class <= number_of_classes:
		temp_object = DetectedObjects(None,None,None,None)
		i = 0
		objects_detected = 0
		
		while i < num_of_objects:	
			if detected_objects.scores[0][i] > detection_threshold:
				if detected_objects.classes[0][i] == current_class:
					if objects_detected != 0:        	                		
						temp_object.boxes = np.concatenate((temp_object.boxes,[detected_objects.boxes[0][i]]),axis=0)

						temp_object.classes = np.append(temp_object.classes,detected_objects.classes[0][i])

						temp_object.scores = np.append(temp_object.scores,detected_objects.scores[0][i])

					else:
						temp_object.boxes = detected_objects.boxes[0][i]
						temp_object.boxes = np.expand_dims(temp_object.boxes,axis = 0)

						temp_object.classes = detected_objects.classes[0][i]

						temp_object.scores = detected_objects.scores[0][i]

					objects_detected += 1
			i += 1

		class_name = category_index[current_class]['name']						

		if current_class > 1:
			json_string += ","

		if objects_detected > 0 :
			coordinates = convert_boxes_to_coordinates(image_np,temp_object.boxes,objects_detected)
			json_string += "\"" + class_name + "\":{\"count\":" + str(objects_detected) + ",\"coordinates\":" + str(coordinates.tolist()) + ",\"scores\":" + str(temp_object.scores.tolist()) + "}"
		else:
			json_string += "\"" + class_name + "\":{\"count\": 0}"

		current_class += 1
	
	json_string += "}},\"Camera\":\"{}\"}}".format(camera_ip)
			
	
	return json_string

class TensorflowModel(object):
	def __init__(self, category_index, detection_graph):
			self.category_index = category_index
			self.detection_graph = detection_graph

class FPS(object):
	def __init__(self, last_time):
			self.time = 0
			self.last_time = last_time
			self.average_fps = 0
			self.fps = 0
			self.accumulated_fps = 0
			self.count = 0

def scale_coordinate(pixel_count_original, pixel_count_cropped, percentage_original,
		percentage_cropped, first_coordinate):
	cropped_coordinate = pixel_count_cropped * percentage_cropped
	original_coordinate = pixel_count_original*percentage_original

	if first_coordinate == 1:
		combined_coordinates = cropped_coordinate + original_coordinate
	else:
		combined_coordinates = original_coordinate - cropped_coordinate
		
	return combined_coordinates / pixel_count_original

def scale_box(original_image, cropped_image, original_box, scaled_box):
	(height,width,channel) = original_image.shape
	(cropped_height,cropped_width,cropped_channel) = cropped_image.shape

	y1 = scale_coordinate(height,cropped_height,
		original_box[0]/IMAGE_BOX_SCALE_FACTOR, scaled_box[0],1)
	y2 = scale_coordinate(height,cropped_height,
		original_box[2]*IMAGE_BOX_SCALE_FACTOR, 1-scaled_box[2],0)
	x1 = scale_coordinate(width,cropped_width,
		original_box[1]/IMAGE_BOX_SCALE_FACTOR, scaled_box[1],1)
	x2 = scale_coordinate(width,cropped_width,
		original_box[3]*IMAGE_BOX_SCALE_FACTOR, 1-scaled_box[3],0)

	return np.array([[y1,x1,y2,x2]])

def detect_objects(image,detection_graph,sess):
	image_tensor = detection_graph.get_tensor_by_name('image_tensor:0')
	# Each box represents a part of the image where a particular object was detected.
	boxes = detection_graph.get_tensor_by_name('detection_boxes:0')
	# Each score represent how level of confidence for each of the objects.
	# Score is shown on the result image, together with the class label.
	scores = detection_graph.get_tensor_by_name('detection_scores:0')
	classes = detection_graph.get_tensor_by_name('detection_classes:0')
	num_detections = detection_graph.get_tensor_by_name('num_detections:0')
	# Expand dimensions since the model expects images to have shape: [1, None, None, 3]
	image_np_expanded = np.expand_dims(image, axis=0)

      # Actual detection.
	(boxes, scores, classes, num_detections) = sess.run(
	  [boxes, scores, classes, num_detections],
	  feed_dict={image_tensor: image_np_expanded})

	return DetectedObjects(boxes,scores,classes,num_detections)	

def print_boxes(image, detected_objects, box_count,category_index):
	i = 0

	(crp_height,crp_width,crp_channel) = image.shape	

	while i < box_count:
		x1_1 = crp_width * detected_objects.boxes[i][1]
		x2_1 = crp_width * detected_objects.boxes[i][3]
		y1_1 = crp_height * detected_objects.boxes[i][0]
		y2_1 = crp_height * detected_objects.boxes[i][2]
		cv2.rectangle(image, (int(x1_1), int(y1_1)), (int(x2_1), int(y2_1)), 
		(0, 0, 255), 1);
		
		if len(np.shape(detected_objects.classes)) == 0:
			class_name = category_index[detected_objects.classes]['name']

			cv2.putText(image,class_name,
				(int(x1_1),int(y1_1)-20), font, 0.5,(0, 0, 255),1,cv2.LINE_AA)
			cv2.putText(image, "{}%".format(int(detected_objects.scores * 100)),
				(int(x1_1),int(y1_1)-5), font, 0.5,(0, 0, 255),1,cv2.LINE_AA)
		else:		
			class_name = category_index[detected_objects.classes[i]]['name']
			cv2.putText(image,class_name,
				(int(x1_1),int(y1_1)-20), font, 0.5,(0, 0, 255),1,cv2.LINE_AA)
			cv2.putText(image, "{}%".format(int(detected_objects.scores[i]*100)),
				(int(x1_1),int(y1_1)-5), font, 0.5,(0, 0,255),1,cv2.LINE_AA)

		i+=1

	return image

def convert_boxes_to_coordinates(image, boxes, box_count):
	i = 0

	coordinates = np.zeros((box_count,4))

	(crp_height,crp_width,crp_channel) = image.shape	
	while i < box_count:
		coordinates[i][0] = crp_width * boxes[i][1]
		coordinates[i][1] = crp_width * boxes[i][3]
		coordinates[i][2] = crp_height * boxes[i][0]
		coordinates[i][3] = crp_height * boxes[i][2]

		i+=1

	return coordinates

def setup_gstreamer(pipeline):
	return cv2.VideoCapture(pipeline,cv2.CAP_GSTREAMER)

def tensorflow_model_setup(path_to_ckpt, path_to_labels):
	sys.path.append("..")

	config = tf.ConfigProto()
	config.gpu_options.allow_growth = True
	tf.Session(config=config)

	# ## Load a (frozen) Tensorflow model into memory.
	detection_graph = tf.Graph()
	with detection_graph.as_default():
	  od_graph_def = tf.GraphDef()
	  with tf.gfile.GFile(path_to_ckpt, 'rb') as fid:
		serialized_graph = fid.read()
		od_graph_def.ParseFromString(serialized_graph)
		tf.import_graph_def(od_graph_def, name='')

	label_map = label_map_util.load_labelmap(path_to_labels)
	categories = label_map_util.convert_label_map_to_categories(label_map, max_num_classes=NUM_CLASSES, use_display_name=True)

	category_index = label_map_util.create_category_index(categories)

	return TensorflowModel(category_index, detection_graph)

def print_image(image, detected_objects, tensorflow_model):
	image_np = print_boxes(image, detected_objects, detected_objects.num_detections, tensorflow_model.category_index)

	(img_height,img_width,img_channel) = image_np.shape
	cv2.imshow('object detection', cv2.resize(image_np, (img_width,img_height)))

	if cv2.waitKey(1) & 0xFF == ord('q'):
		cv2.destroyAllWindows()
		return 1
	else:
		return 0

def filter_objects(detection_class, detected_objects, detection_threshold):
	filtered_detected_objects = DetectedObjects(None,None,None,None)
	i = 0		
	object_count = 0
	
	
	while detected_objects.scores[0][i] > detection_threshold:
#		if detected_objects.classes[0][i] == detection_class:
		if object_count != 0:
			filtered_detected_objects.boxes = np.concatenate(
				(filtered_detected_objects.boxes,[detected_objects.boxes[0][i]]),axis=0)

			filtered_detected_objects.classes = np.append(
				filtered_detected_objects.classes,detected_objects.classes[0][i])

			filtered_detected_objects.scores = np.append(
				filtered_detected_objects.scores,detected_objects.scores[0][i])
	
		else:
			filtered_detected_objects.boxes = detected_objects.boxes[0][i]
			filtered_detected_objects.boxes = np.expand_dims(filtered_detected_objects.boxes,axis = 0)

			filtered_detected_objects.classes = detected_objects.classes[0][i]

			filtered_detected_objects.scores = detected_objects.scores[0][0]

		object_count += 1

		i+=1
	
	filtered_detected_objects.num_detections = object_count
	return filtered_detected_objects

if __name__ == "__main__":
	if len(sys.argv)!=4: sys.exit("Missing arguments, please enter camera IP, network interface and path to tensorflow model")
	camera_ip = sys.argv[1]
	_IFACE = sys.argv[2]
	model_name = sys.argv[3]

	gstr_pipe = "udpsrc multicast-group=" + camera_ip + " auto-multicast=true multicast-iface=" + _IFACE + " ! " + _VIDEO_CAPS + " ! rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! videoscale ! " + output_size + " ! queue2 ! appsink sync=false"

	# Path to frozen detection graph. This is the actual model that is used for the object detection.
	path_to_ckpt = model_name + '/frozen_inference_graph.pb'

	# List of the strings that is used to add correct label for each box.
	#PATH_TO_LABELS = os.path.join('data', 'mscoco_label_map.pbtxt')
	path_to_labels = model_name + '/object_detection.pbtxt'

	count = 0
	last_object_count = 0
	fps = FPS(time.time())

	cap = setup_gstreamer(gstr_pipe)
	print "Returning: {}".format(cap)

	tensorflow_model = tensorflow_model_setup(path_to_ckpt,path_to_labels)

	last_time = time.time()

	with tensorflow_model.detection_graph.as_default():
	  with tf.Session(graph=tensorflow_model.detection_graph) as sess:
		while True:	
			ret, image_np = cap.read()	

			detected_objects = detect_objects(image_np,tensorflow_model.detection_graph,sess)
			json_string = createJsonObject(detected_objects, detected_objects.num_detections, NUM_CLASSES, tensorflow_model.category_index, image_np, DETECTION_THRESHOLD, camera_ip)

			filtered_detected_objects = filter_objects(DETECTION_CLASS, detected_objects, DETECTION_THRESHOLD) 			

			coordinates = convert_boxes_to_coordinates(image_np,filtered_detected_objects.boxes,filtered_detected_objects.num_detections)

			fps = show_fps(fps)

#			print "Fps: {}, average: {}".format(fps.fps,fps.average_fps)
#			if filtered_detected_objects.num_detections != last_object_count:
			if filtered_detected_objects.num_detections != 0:
				print(json_string)
#				print "{{count:{},coordinates:{},scores:{}}}".format(filtered_detected_objects.num_detections,coordinates.tolist(),filtered_detected_objects.scores.tolist())
			#else:
#				print "{{count:{},coordinates:[],scores:[]}}".format(filtered_detected_objects.num_detections)

#			print "count:{},scores:{},coordinates:{},FPS:{},Average FPS:{}".format(filtered_detected_objects.num_detections,filtered_detected_objects.scores,coordinates,fps.fps,fps.average_fps)
				


			last_object_count = filtered_detected_objects.num_detections 		

#			Uncomment belov if picture should be displayed
#			if(print_image(image_np, filtered_detected_objects, tensorflow_model) == 1):
#				break
			
			count+=1
