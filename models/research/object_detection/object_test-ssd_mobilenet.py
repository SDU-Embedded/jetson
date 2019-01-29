import numpy as np
import os
import six.moves.urllib as urllib
import sys
import tarfile
import tensorflow as tf
import zipfile
import time
import copy

from collections import defaultdict
from io import StringIO
from PIL import Image

from utils import label_map_util
from utils import visualization_utils as vis_util

import cv2

MODEL_NAME = 'Mongoose_low_res'
NUM_CLASSES = 4
DETECTION_THRESHOLD = 0.5
DETECTION_CLASS = 4
IMAGE_BOX_SCALE_FACTOR = 1.1
GSTREAMER_PIPELINE = "udpsrc port=5000 ! application/x-rtp,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! jpegdec ! videoconvert ! video/x-raw, format=(string)BGR ! appsink"
font = cv2.FONT_HERSHEY_SIMPLEX


def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

@static_vars(average_fps=0)
def show_fps(last_time,count):
	if count > 10:
		fps = (1)/(time.time()-last_time)
		show_fps.average_fps += fps
		print("fps: {}, average: {}".format(fps,show_fps.average_fps/(count-10)))

	return time.time()

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
		x1_1 = crp_width * temp_detected.boxes[0][i][1]
		x2_1 = crp_width * temp_detected.boxes[0][i][3]
		y1_1 = crp_height * temp_detected.boxes[0][i][0]
		y2_1 = crp_height * temp_detected.boxes[0][i][2]
		cv2.rectangle(image, (int(x1_1), int(y1_1)), (int(x2_1), int(y2_1)), 
		(0, 0, 255), 1);
		
		if len(np.shape(detected_objects.classes[0][i])) == 0:
			class_name = category_index[detected_objects.classes[0][i]]['name']

			cv2.putText(image,class_name,
				(int(x1_1),int(y1_1)-20), font, 0.5,(0, 0, 255),1,cv2.LINE_AA)
			cv2.putText(image, "{}%".format(int(detected_objects.scores[0][i] * 100)),
				(int(x1_1),int(y1_1)-5), font, 0.5,(0, 0, 255),1,cv2.LINE_AA)
		else:		
			class_name = category_index[detected_objects.classes[0][i][0]]['name']
			cv2.putText(image,class_name,
				(int(x1_1),int(y1_1)-20), font, 0.5,(0, 0, 255),1,cv2.LINE_AA)
			cv2.putText(image, "{}%".format(int(detected_objects.scores[0][i][0]*100)),
				(int(x1_1),int(y1_1)-5), font, 0.5,(0, 0,255),1,cv2.LINE_AA)

		i+=1

	return image

cap = cv2.VideoCapture(GSTREAMER_PIPELINE,cv2.CAP_GSTREAMER)

sys.path.append("..")

config = tf.ConfigProto()
config.gpu_options.allow_growth = True
session = tf.Session(config=config)

# # Model preparation 

# ## Variables
# 
# Any model exported using the `export_inference_graph.py` tool can be loaded here simply by changing `PATH_TO_CKPT` to point to a new .pb file.  

#MODEL_FILE = MODEL_NAME + '.tar.gz'
#DOWNLOAD_BASE = 'http://download.tensorflow.org/models/object_detection/'

# Path to frozen detection graph. This is the actual model that is used for the object detection.
PATH_TO_CKPT = MODEL_NAME + '/frozen_inference_graph.pb'

# List of the strings that is used to add correct label for each box.
#PATH_TO_LABELS = os.path.join('data', 'mscoco_label_map.pbtxt')
PATH_TO_LABELS = os.path.join('data', 'object-detection.pbtxt')

# ## Load a (frozen) Tensorflow model into memory.
detection_graph = tf.Graph()
with detection_graph.as_default():
  od_graph_def = tf.GraphDef()
  with tf.gfile.GFile(PATH_TO_CKPT, 'rb') as fid:
    serialized_graph = fid.read()
    od_graph_def.ParseFromString(serialized_graph)
    tf.import_graph_def(od_graph_def, name='')

label_map = label_map_util.load_labelmap(PATH_TO_LABELS)
categories = label_map_util.convert_label_map_to_categories(label_map, max_num_classes=NUM_CLASSES, use_display_name=True)
category_index = label_map_util.create_category_index(categories)

last_time = time.time()
count = 0
object_count = 0
detected_objects = 0
average_time = 0
last_time_2 = 0
average_count = 0

draw_average = 0
draw_last = 0
with detection_graph.as_default():
  with tf.Session(graph=detection_graph) as sess:
    while True:
	ret, image_np = cap.read()
	
	if count % 1 == 0:
		detected_objects = detect_objects(image_np,detection_graph,sess)

		print "Boxes: {}".format(detected_objects.boxes)
		i = 0		
		object_count = 0

		temp_detected = DetectedObjects(None,None,None,None)
		print detected_objects.classes
		while detected_objects.scores[0][i] > DETECTION_THRESHOLD:
			print "Scores: {}".format(detected_objects.scores[0][i])
			print "Class: {}".format(detected_objects.classes[0][i])

#			if detected_objects.classes[0][i] == DETECTION_CLASS:
			if object_count == 0:
				temp_detected.boxes = detected_objects.boxes[0][i]
				temp_detected.boxes = np.expand_dims(temp_detected.boxes,axis = 0)

				temp_detected.classes = detected_objects.classes[0][i]

				temp_detected.scores = detected_objects.scores[0][0]
			
			else:
				temp_detected.boxes = np.concatenate(
					(temp_detected.boxes,[detected_objects.boxes[0][i]]),axis=0)

				temp_detected.classes = np.append(
					temp_detected.classes,detected_objects.classes[0][i])

				temp_detected.scores = np.append(
					temp_detected.scores,detected_objects.scores[0][i])

			object_count += 1				
				#print "Box coordinates: {},{}".format(
				#print "Box: {}".format(original_boxes[i])
		
			i+=1
			
		
		temp_detected.classes = np.append(temp_detected.classes,1)
		temp_detected.scores = np.append(temp_detected.scores,0.1)

		if object_count	> 1:
			temp_detected.classes = np.expand_dims(temp_detected.classes,axis = 0)
			temp_detected.scores = np.expand_dims(temp_detected.scores,axis = 0)
			temp_detected.boxes = np.concatenate((temp_detected.boxes,np.array(
				[[0,0,0,0]])),axis=0)

		elif object_count == 1:
			temp_detected.classes = np.expand_dims(temp_detected.classes,axis = 0)
			temp_detected.classes = np.expand_dims(temp_detected.classes,axis = 0)
			temp_detected.scores = np.expand_dims(temp_detected.scores,axis = 0)
			temp_detected.scores = np.expand_dims(temp_detected.scores,axis = 0)
			temp_detected.boxes = np.concatenate((temp_detected.boxes,np.array(
				[[0,0,0,0]])),axis=0)
		
#		print "boxes: {}".format(temp_detected.boxes)		
		
		temp_detected.boxes = np.expand_dims(temp_detected.boxes,axis = 0)
		
		detected_objects = temp_detected		
		original_boxes = tuple(detected_objects.boxes[0].tolist())

	else:
		i = 0
#		if object_count > 0:
		detected_objects = DetectedObjects(None,None,None,None)
		while i < object_count:
			cropped_img = crop_image(image_np,original_boxes)

#			(crp_height,crp_width,crp_channel) = cropped_img.shape
#			cv2.imshow('small {}'.format(i), 
#				cv2.resize(cropped_img, (crp_width,crp_height)))
#			cv2.waitKey(1)
#			if count == 1:
#				cv2.imwrite("img_crop_{}.png".format(i),cropped_img)

			last_time_2 = time.time()
			temp_detected_object = detect_objects(
				cropped_img,detection_graph,sess)
			average_time += (time.time() - last_time_2)
			average_count += 1

			print "average: {}".format(average_time/average_count)
			box = temp_detected_object.boxes[0][0].tolist()

			final_box = scale_box(image_np,cropped_img,
				original_boxes[i],box)

			if i == 0:
				detected_objects.boxes = final_box

				detected_objects.classes = temp_detected_object.classes[0][0]

				detected_objects.scores = temp_detected_object.scores[0][0]
				
			else:
				detected_objects.boxes = np.concatenate(
					(detected_objects.boxes,final_box),axis=0)

				detected_objects.classes = np.append(
					detected_objects.classes,temp_detected_object.classes[0][0])

				detected_objects.scores = np.append(
					detected_objects.scores,temp_detected_object.scores[0][0])

			i += 1
		
		detected_objects.classes = np.append(detected_objects.classes,1)
		detected_objects.scores = np.append(detected_objects.scores,0.1)

		if object_count	> 1:
			temp_detected.classes = np.expand_dims(detected_objects.classes,axis = 0)
			temp_detected.scores = np.expand_dims(detected_objects.scores,axis = 0)
			temp_detected.boxes = np.concatenate((detected_objects.boxes,
				np.array([[0,0,0,0]])),axis=0)

		elif object_count == 1:
			detected_objects.classes = np.expand_dims(detected_objects.classes,axis = 0)
			detected_objects.classes = np.expand_dims(detected_objects.classes,axis = 0)
			detected_objects.scores = np.expand_dims(detected_objects.scores,axis = 0)
			detected_objects.scores = np.expand_dims(detected_objects.scores,axis = 0)
			detected_objects.boxes = np.concatenate((detected_objects.boxes,
				np.array([[0,0,0,0]])),axis=0)
				

		detected_objects.boxes = np.expand_dims(detected_objects.boxes,axis = 0)
		original_boxes = tuple(detected_objects.boxes[0].tolist())
	
	draw_last = time.time()
	image_np = print_boxes(image_np, detected_objects, object_count,category_index)
	draw_average += time.time() - draw_last
	if count > 0:
		print "Draw time: {}".format(draw_average/count)	
	
	if count == 0:
		cv2.imwrite("img{}.png".format(count),image_np)
	
	(img_height,img_width,img_channel) = image_np.shape
	cv2.imshow('object detection', cv2.resize(image_np, (img_width,img_height)))
	if cv2.waitKey(1) & 0xFF == ord('q'):
		cv2.destroyAllWindows()
	        break

	last_time = show_fps(last_time,count)
	count+=1 #increase count value
