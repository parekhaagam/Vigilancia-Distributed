"""Object Detection on Spark using TensorFlow.

Consumes video frames from an Kafka Endpoint, process it on spark, produces
a result containing annotate video frame and sends it to another topic of the
same Kafka Endpoint.

"""

from __future__ import print_function
import base64
import json
import numpy as np
from io import StringIO
from timeit import default_timer as timer
from PIL import Image
import datetime as dt
from random import randint

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import cv2

import time
import collections
import vgconf

from core.services import SuspicionDetection


class Spark_Object_Detector():
    """Stream WebCam Images to Kafka Endpoint."""

    def __init__(self,
                 interval=10,
                 topic_to_consume='test',
                 topic_for_produce='resultstream',
                 kafka_endpoint='127.0.0.1:9092'):
        
        """ Initialize our yolo and firearm model"""

        self.detector = SuspicionDetection.SuspicionDetection()
        self.detector.enable_yolo_detection()
        self.detector.enable_firearm_detection()
        
        """Initialize Spark & TensorFlow environment."""
        
        self.topic_to_consume = topic_to_consume
        self.topic_for_produce = topic_for_produce
        self.kafka_endpoint = kafka_endpoint
        
        # Create Kafka Producer for sending results
        self.producer = KafkaProducer(bootstrap_servers=kafka_endpoint)

        sc = SparkContext(appName='FirmArmDetection')
        self.ssc = StreamingContext(sc, interval)  # , 3)

        # Make Spark logging less extensive
        log4jLogger = sc._jvm.org.apache.log4j
        log_level = log4jLogger.Level.ERROR
        log4jLogger.LogManager.getLogger('org').setLevel(log_level)
        log4jLogger.LogManager.getLogger('akka').setLevel(log_level)
        log4jLogger.LogManager.getLogger('kafka').setLevel(log_level)
        self.logger = log4jLogger.LogManager.getLogger(__name__)
        self.objects_detector_prediction = []
        self.objects_detected_view_text=""

    def _update_predictions(self):
        self.objects_detector_prediction = self.detector.get_yolo_prediction()
        self.firearm_detector_prediction = (
            self.detector.get_firearm_detector_prediction())
        self.activity_detector_prediction = (
            self.detector.get_activity_detector_prediction())
        self.event_detector_prediction = (
            self.detector.get_event_detector_prediction())

        self.detected_objects = []
        if self.objects_detector_prediction:
            self.detected_objects.extend(self.objects_detector_prediction)
        if self.firearm_detector_prediction:
            self.detected_objects.extend(self.firearm_detector_prediction)

        if self.detected_objects:
            self._update_detected_objects(self.detected_objects)

    def _update_detected_objects(self, objects_prediction):
        parsed_objects = [p['label'] for p in objects_prediction]
        parsed_objects_dict = collections.Counter(parsed_objects)
        detected_suspicious_objects = False
        objects = ''

        for (obj, count) in parsed_objects_dict.items():
            objects += '%s (%d)\n' % (obj, count)
            if obj in vgconf.SUSPICIOUS_OBJECTS_LIST:
                detected_suspicious_objects = True

        self.objects_detected_view_text = objects

        """ Do when suspicious object is detected """
        # Start alert if suspicious object is detected.
        # if detected_suspicious_objects:
        #     self._start_alert()

    def start_processing(self):
        """Start consuming from Kafka endpoint and detect objects."""
        kvs = KafkaUtils.createDirectStream(self.ssc,
                                            [self.topic_to_consume],
                                            {'metadata.broker.list': self.kafka_endpoint}
                                            )
        kvs.foreachRDD(self.handler)
        self.ssc.start()
        self.ssc.awaitTermination()

    
    def detect_objects(self, event):
        """Use Yolo and Incepiton Model to detect objects."""
        
        decoded = base64.b64decode(event['image'])
        
        # TODO: Picking unique filenames or find a way to send it to kafka  

        filename = 'C:\\Users\\hp\\Desktop\\codev1frame.jpg'  # I assume you have a way of picking unique filenames
        with open(filename, 'wb') as f:
            f.write(decoded)
        img = cv2.imread(filename)

        # Prepare object for sending to endpoint
        result = {'timestamp': event['timestamp'],
                  'camera_id': event['camera_id'],
                  'image': self.get_box_plot(img),
                  'prediction': self.objects_detected_view_text
                  }
        return json.dumps(result)

    def get_box_plot(self,img):
        self.detector.detect(img)
        frame = self.detector.plot_objects(img)
        self._update_predictions()
        img_str = cv2.imencode('.jpeg', frame)[1]
        img_as_text = base64.b64encode(img_str).decode('utf-8')
        return img_as_text

    def handler(self, timestamp, message):
        """Collect messages, detect object and send to kafka endpoint."""
        records = message.collect()
        # For performance reasons, we only want to process the newest message
        # for every camera_id
        to_process = {}
        self.logger.info( '\033[3' + str(randint(1, 7)) + ';1m' +  # Color
            '-' * 25 +
            '[ NEW MESSAGES: ' + str(len(records)) + ' ]'
            + '-' * 25 +
            '\033[0m' # End color
            )
        dt_now = dt.datetime.now()
        for record in records:
            event = json.loads(record[1])
            self.logger.info('Received Message: ' +
                             event['camera_id'] + ' - ' + event['timestamp'])
            dt_event = dt.datetime.strptime(
                event['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            delta = dt_now - dt_event
            print("timestamp = " + str(dt_event))
            if delta.seconds > 5:
                continue
            to_process[event['camera_id']] = event

        if len(to_process) == 0:
            self.logger.info('Skipping processing...')

        for key, event in to_process.items():
            self.logger.info('Processing Message: ' +
                             event['camera_id'] + ' - ' + event['timestamp'])
            start = timer()
            detection_result = self.detect_objects(event)
            self.logger.info('prediction: ' + self.objects_detected_view_text)
            end = timer()
            delta = end - start
            self.logger.info('Done after ' + str(delta) + ' seconds.')
            self.producer.send(self.topic_for_produce, detection_result.encode('utf-8'))
            self.logger.info('Sent image to Kafka endpoint.')
            self.producer.flush()

if __name__ == '__main__':
    sod = Spark_Object_Detector(
        interval=1,
        topic_to_consume='test',
        topic_for_produce='resultstream',
        kafka_endpoint='127.0.0.1:9092')
    sod.start_processing()