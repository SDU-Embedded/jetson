#!/bin/sh

/jetson/protoc/bin/protoc /jetson/models/research/object_detection/protos/*.proto --python_out=. 
export PYTHONPATH=$PYTHONPATH:/jetson/models/research:/jetson/models/research/slim
python /jetson/models/research/object_detection/mongoose_detect.py | kafkacat -b manna.mmmi-lab,hou.mmmi-lab,bisnap.mmmi-lab -t outdoor_wall
