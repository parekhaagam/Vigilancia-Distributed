from kafka import KafkaProducer
import cv2
import base64
import json
import time
import datetime as dt
import logging
from camera import VideoCamera
from flask import Flask, render_template, Response

# class Webcam_Producer():

#     def __init__(self,
#                  interval: int = 3,
#                  source=0,
#                  camera_id: str = 'camera_generic',
#                  topic: str = 'pyturestream',
#                  server: str = 'localhost:9092'):

#         logger.info('-'*50)
#         logger.info('Initialized camera "{camera_id}" with source {source}.')
#         logger.info('Send to "{topic}" on "{server}" every {interval} sec.')

#         # Class Variables
#         self.interval = interval  # Interval for Photos in Seconds
#         self.video_source = source
#         self.camera_id = camera_id
#         self.server = server  # Host + Port of Kafka Endpoint
#         self.topic = topic

#         # Connection to Kafka Enpoint
#         try:
#             self.producer = KafkaProducer(bootstrap_servers=self.server,
#                                           value_serializer=lambda m: json.dumps(m).encode('utf8'))
#         except Exception as e:
#             logger.error(e)

#         # Start Streaming...
#         logger.info('-'*50)
#         self.stream_video()

#     def stream_video(self):
#         """Start streaming video frames to Kafka forever."""
#         logger.info('Start capturing frames every {self.interval} sec.')
#         video = cv2.VideoCapture('v2.mp4')
#         logger.info('video open status: {video.isOpened()}')
#         count_frame = 1
#         while video.isOpened():
#             success, image = video.read()
#             timestamp = dt.datetime.now().isoformat()
#             if count_frame == 30:
#                 if success is True:
#                     jpg = cv2.imencode('.jpeg', image)[1]
#                     jpg_as_text = base64.b64encode(jpg).decode('utf-8')
#                     result = {
#                         'image': jpg_as_text,
#                         'timestamp': dt.datetime.now().isoformat(),
#                         'camera_id': self.camera_id
#                     }
#                     count_frame = 0
#                     self.send_to_kafka(result)
#                 else:
#                     logger.error('Could not read image from source {self.video_source}!')
#             count_frame = count_frame + 1
#             time.sleep(self.interval)
#         video.release()

#     def send_to_kafka(self, data):

#         self.producer.send(self.topic, data)
#         logger.info('Sent image to Kafka endpoint.')

# if __name__ == '__main__':
#     # Set source='demo.mp4' for streaming video file
#     Webcam_Producer(interval=0.3,
#                     source='v1.mp4',
#                     camera_id='holger_cam',
#                     server='localhost:9092',
#                     topic='test')

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def gen(camera):
    while True:
        frame,raw_frame = camera.get_frame()
        camera.stream_video(raw_frame)
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n\r\n')

@app.route('/video_feed')
def video_feed():
    return Response(gen(VideoCamera()),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
