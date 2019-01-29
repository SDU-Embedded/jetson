import numpy as np
import os
import six.moves.urllib as urllib
import sys
import tarfile
import tensorflow as tf
import zipfile
import time

from collections import defaultdict
from io import StringIO
from PIL import Image

import cv2
cap = cv2.VideoCapture("udpsrc port=5000 ! application/x-rtp,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! jpegdec ! videoconvert ! video/x-raw, format=(string)BGR ! appsink",cv2.CAP_GSTREAMER)

# This is needed since the notebook is stored in the object_detection folder.
sys.path.append("..")

config = tf.ConfigProto()
config.gpu_options.allow_growth = True
session = tf.Session(config=config)

# ## Object detection imports
# Here are the imports from the object detection module.

# In[3]:

from utils import label_map_util

from utils import visualization_utils as vis_util


# # Model preparation 

# ## Variables
# 
# Any model exported using the `export_inference_graph.py` tool can be loaded here simply by changing `PATH_TO_CKPT` to point to a new .pb file.  
# 
# By default we use an "SSD with Mobilenet" model here. See the [detection model zoo](https://github.com/tensorflow/models/blob/master/object_detection/g3doc/detection_model_zoo.md) for a list of other models that can be run out-of-the-box with varying speeds and accuracies.

# In[4]:

# What model to download.
MODEL_NAME = 'ssd_mobilenet_v1_coco_11_06_2017'
MODEL_FILE = MODEL_NAME + '.tar.gz'
#DOWNLOAD_BASE = 'http://download.tensorflow.org/models/object_detection/'

# Path to frozen detection graph. This is the actual model that is used for the object detection.
PATH_TO_CKPT = MODEL_NAME + '/frozen_inference_graph.pb'

# List of the strings that is used to add correct label for each box.
PATH_TO_LABELS = os.path.join('data', 'mscoco_label_map.pbtxt')

NUM_CLASSES = 90


# ## Download Model

# In[5]:

#opener = urllib.request.URLopener()
#opener.retrieve(DOWNLOAD_BASE + MODEL_FILE, MODEL_FILE)
#tar_file = tarfile.open(MODEL_FILE)
#for file in tar_file.getmembers():
 # file_name = os.path.basename(file.name)
  #if 'frozen_inference_graph.pb' in file_name:
   # tar_file.extract(file, os.getcwd())


# ## Load a (frozen) Tensorflow model into memory.

# In[6]:

detection_graph = tf.Graph()
with detection_graph.as_default():
  od_graph_def = tf.GraphDef()
  with tf.gfile.GFile(PATH_TO_CKPT, 'rb') as fid:
    serialized_graph = fid.read()
    od_graph_def.ParseFromString(serialized_graph)
    tf.import_graph_def(od_graph_def, name='')


# ## Loading label map
# Label maps map indices to category names, so that when our convolution network predicts `5`, we know that this corresponds to `airplane`.  Here we use internal utility functions, but anything that returns a dictionary mapping integers to appropriate string labels would be fine

# In[7]:

label_map = label_map_util.load_labelmap(PATH_TO_LABELS)
categories = label_map_util.convert_label_map_to_categories(label_map, max_num_classes=NUM_CLASSES, use_display_name=True)
category_index = label_map_util.create_category_index(categories)

# # Detection
	
# In[9]:

# For the sake of simplicity we will use only 2 images:
# image1.jpg
# image2.jpg
# If you want to test the code with your images, just add path to the images to the TEST_IMAGE_PATHS.

# Size, in inches, of the output images.

# In[10]:

last_time = time.time()

def show_fps(last_time):
	new_time = time.time()
	fps = (1)/(new_time-last_time)
	print("fps: {}".format(fps))
	last_time = new_time	
	return last_time

count = 0
first_time = 1
accumulated = 0
with detection_graph.as_default():
  with tf.Session(graph=detection_graph) as sess:
    while True:
	ret, image_np = cap.read()

	# Expand dimensions since the model expects images to have shape: [1, None, None, 3]
	image_np_expanded = np.expand_dims(image_np, axis=0)
	image_tensor = detection_graph.get_tensor_by_name('image_tensor:0')
	# Each box represents a part of the image where a particular object was detected.
	boxes = detection_graph.get_tensor_by_name('detection_boxes:0')
	# Each score represent how level of confidence for each of the objects.
	# Score is shown on the result image, together with the class label.
	scores = detection_graph.get_tensor_by_name('detection_scores:0')
	classes = detection_graph.get_tensor_by_name('detection_classes:0')
	num_detections = detection_graph.get_tensor_by_name('num_detections:0')
      # Actual detection.
	(boxes, scores, classes, num_detections) = sess.run(
	  [boxes, scores, classes, num_detections],
	  feed_dict={image_tensor: image_np_expanded})

	
      # Visualization of the results of a detection.
	time1 = time.time()
	vis_util.visualize_boxes_and_labels_on_image_array(
	  image_np,
	  np.squeeze(boxes),
	  np.squeeze(classes).astype(np.int32),
	  np.squeeze(scores),
	  category_index,
	  use_normalized_coordinates=True,
	  min_score_thresh=.5,
	  line_thickness=8)

	if first_time == 0:
		count+=1
		accumulated = (time.time() - time1) + accumulated
		print "Detect time: {}".format(accumulated/(count))
	else:
		first_time = 0
#	if first_time == 0:
#		new_time = time.time()
#		count+=1
#		fps = (1)/(new_time-last_time)
#		accumulated += fps
#		print("fps: {}".format(accumulated/count))
#		last_time = time.time()	
#	else: 
#		last_time = time.time()	
#		first_time = 0

	(img_height,img_width,img_channel) = image_np.shape
	cv2.imshow('object detection', cv2.resize(image_np, (img_width,img_height)))
	if cv2.waitKey(25) & 0xFF == ord('q'):
		cv2.destroyAllWindows()
	        break
