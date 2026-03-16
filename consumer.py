import cv2
import time
import base64
from kafka import KafkaProducer, KafkaConsumer
from apache_beam.io.kafka import ReadFromKafka
import json
import apache_beam as beam
import numpy as np
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

class DecodeMessage(beam.DoFn):
    def process(self, element):
        key, value = element
        msg = json.loads(value.decode("utf-8"))
        yield msg

def parseMessage(msg):
    timestamp = msg['timestamp']
    frame_id = msg['frame_id']
    fps = msg['frame_rate']
    # frame_bytes = msg['frame_bytes']

    # frame_bytes = base64.b64decode(frame_bytes)
    # numpy_arr = np.frombuffer(frame_bytes, np.uint8)
    return f"Received Frame {frame_id} (fps={fps}) at {'timestamp'}"# with shape {numpy_arr.shape}"

options = PipelineOptions()
# options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as p:
    frames = (
        p
        | "ReadFromKafka" >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': 'localhost:9092', 
                'auto.offset.reset': 'earliest',
                'group.id': 'test-group',
                # 'fetch.max.bytes': 5242880,      # 5 MB, max bytes per fetch
                # 'max.partition.fetch.bytes': 5242880,  # 5 MB per partition
                # 'enable.auto.commit': 'false',
                },
            topics=['quickstart-events'],
            #Below option is only for testing purposes on DirectRunner, 
            #this treats data as bounded for DirectRunner, 
            #without it we see 'No message retrieved, increase consumer poll timeout' warning messages 
            #and no message is ever received.
            #---- Below points are invalid if we assign timestamps before windowing -----
            # NOTE: It's value should be the duration of video given that the frames are received in real time
            # So for windowing to work as expected both producer.py and consumer.py should start and end at the same time.(video duration)
            # This (and FixedWindows(1)) ensures we get 1 frame per second of the video (if 13secs video we get 13 frames)
            # However, producer may take more time to execute, so we can increase the value 
            # to ensure that consumer consumes all frames while the producer is running
            # We will get more than 13frames but it's a tradeoff
            # TODO: To see how sampling works in case of DataflowRunner
            #----------------------------------------------------------------------------
            max_read_time=25, 
            # key_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
            # value_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
            # Set a longer polling timeout (default is 2s)
            # consumer_poll_timeout_ms=10000  # 10 seconds
        )
        | "Decode" >> beam.ParDo(DecodeMessage())
        | "AddTimestamps" >> beam.Map(lambda ele: beam.window.TimestampedValue(ele, ele['timestamp']))
        | "FixedWindowForSampling" >> beam.WindowInto(FixedWindows(1))
        | "Sampling" >> beam.combiners.Latest.Globally().without_defaults()
        | "Map" >> beam.Map(parseMessage)
        | "LogElements" >> beam.LogElements(with_window=True)
        )

# rtsp_url = 'rtsp://demo:demo@ipvmdemo.dyndns.org:5541/onvif-media/media.amp?'
# vid = cv2.VideoCapture()
# for i in range(10):
    # success, frame = vid.read()
    # print(f'Frame extracted: {success}')
# vid.release()
