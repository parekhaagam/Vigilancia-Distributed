# -*- coding: utf-8 -*-
"""
"""

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext

import base64
import json
import numpy as np
from io import StringIO
from io import BytesIO
from timeit import default_timer as timer
from PIL import Image
import datetime as dt
from random import randint
import time
import logging

import time
import collections
import cv2  

from core.services import SuspicionDetection
import vgconf
import vg_config

from flask import Flask, Response

logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)



def initilize_vigilancia_detector():
    objects_detector_prediction = []
    detector = SuspicionDetection.SuspicionDetection()
    detector.enable_yolo_detection()
    return detector

mainImage = None
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print("Starting ")
detector = initilize_vigilancia_detector()
print("Stoping")
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')
print('-------------------------------------------------------------------------')

# def load_image_into_numpy_array(image):
#        """Convert PIL image to numpy array."""
#        (im_width, im_height) = image.size
#        print("Image width = ",im_width,"image height = ",im_height)
#        return np.array(image.getdata()).reshape(
#            (299, 299, 3)).astype(np.uint8)

# def detect_objects(event):
#         """Use TensorFlow Model to detect objects."""
#         # Load the image data from the json into PIL image & numpy array
#         decoded = base64.b64decode(event['image'])
#         mainImage = decoded
#         #stream = StringIO(decoded)
#         #mainImage = Image.open(stream)
#         #mainImage = str.decode(event['image'])
#         #image_np = self.load_image_into_numpy_array(image)
#         #stream.close()

def handler(message):
        """Collect messages, detect object and send to kafka endpoint."""
        records = message.collect()
        to_process = {}
        dt_now = dt.datetime.now()
        for record in records:
            event = json.loads(record[1])
            logger.info('Received Message: ' + event['camera_id'] + ' - ' + event['timestamp'])
            dt_event = dt.datetime.strptime(event['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            delta = dt_now - dt_event
            if delta.seconds > 5:
                continue
            to_process[event['camera_id']] = event

        if len(to_process) == 0:
            logger.info('Skipping processing...')

        for key, event in to_process.items():
            logger.info('Processing Message: ' + event['camera_id'] + ' - ' + event['timestamp'])
            start = timer()
            logger.info('Image array length:' + str(len(event['image'])))
            
            #imgdata = base64.b64decode(event['image'])
            decoded = base64.b64decode(event['image'])
            #stream = StringIO(decoded)
            #image = Image.open(stream)
            # print(type(image))
            filename = 'C:\\Users\\hp\\Desktop\\codev1frame.jpg'  # I assume you have a way of picking unique filenames
            with open(filename, 'wb') as f:
                f.write(decoded)
            img = cv2.imread(filename)
            #imge_np = load_image_into_numpy_array(img)
            detector.detect(img)
            frame = detector.plot_objects(img)
            cv2.imwrite("abc.jpg",frame)
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            print('-------------------------------------------------------------------------')
            """
                Do call the suspicious detectivity classifier
            """
            
            logger.info('print ...................................................................')
               
            #detect_objects(event)
            end = timer()
            delta = end - start
            logger.info('Done after ' + str(delta) + ' seconds.')
       
@app.route('/')
def index():


    return Response(kafkastream(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

def kafkastream():
    #for message in consumer:
        #print(type(message.value))
    yield (b'--frame\r\n'
           b'Content-Type: image/jpeg\r\n\r\n' + mainImage + b'\r\n\r\n')




if __name__ == '__main__':
    vg_config.init()
    firstTimeImage = False
    firstTime = True
    #app.run(debug=True,port = 5000)
    conf = SparkConf().setAppName("building a warehouse")
    sc = SparkContext(conf=conf)

    sql = SQLContext(sc)
    stream = StreamingContext(sc, 1) # 1 second window
    print('ssc =================== {} {}');

    kafka_stream = KafkaUtils.createDirectStream(stream, \
                                           ["test"],
                                            {'metadata.broker.list': "localhost:9092"}
                                            )

    #lines = kafka_stream.map(lambda x: x[1])
    #logger.info('kafka stream size' + str(delta) + ' seconds.')
    kafka_stream.foreachRDD(handler)
    #handler(lines)
    stream.start()
    stream.awaitTermination()

    
    
