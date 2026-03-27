import cv2
import supervision as sv
from ultralytics import YOLO
import base64
import json
import uuid
import numpy as np
import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics.metricbase import Distribution
from apache_beam.coders import IterableCoder, FloatCoder, BooleanCoder
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp, Duration
from apache_beam.transforms.userstate import (
    CombiningValueStateSpec, 
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec, 
    on_timer, 
    )

class DecodeMessage(beam.DoFn):
    def process(self, element):
        key, value = element
        msg = json.loads(value.decode("utf-8"))
        yield msg

def parseMessage(msg):
    timestamp = msg['timestamp']
    frame_id = msg['frame_id']
    fps = msg['frame_rate']
    frame_bytes = msg['frame_bytes']

    frame_bytes = base64.b64decode(frame_bytes)
    numpy_arr = np.frombuffer(frame_bytes, np.uint8)
    return f"Received Frame {frame_id} (fps={fps}) at {timestamp} with shape {numpy_arr.shape}"

class DetectMovement(beam.DoFn):
    _distribution = Distribution
    events_received = Metrics.counter('DetectMovementDoFn', 'events_received')
    events_processed = Metrics.counter('DetectMovementDoFn', 'events_processed')
    sessions_processed = Metrics.counter('DetectMovementDoFn', 'sessions_processed')
    EVENTS_BAG = BagStateSpec('frames', beam.coders.PickleCoder())
    ACTIVE_STATE = ReadModifyWriteStateSpec('active', BooleanCoder())
    EXPIRY_TIMER = TimerSpec('expiry_timer', time_domain=beam.TimeDomain.WATERMARK)
    MAX_TIMESTAMP = CombiningValueStateSpec('max_timestamp_seen', 
                                            IterableCoder(FloatCoder()),
                                            lambda ele : max(ele, default=0))
    
    def __init__(self):
        self._distribution = Metrics.distribution('My sessions DoFn', 'movement_duration')

    def setup(self):
        import cv2
        self.cv2 = cv2
        self.background_gray = self.cv2.imread('highway_backgroud_gray.jpg', self.cv2.IMREAD_GRAYSCALE)

    def detectMovementInVideo(self, frame):
        gray = self.cv2.cvtColor(frame, self.cv2.COLOR_BGR2GRAY)
        gray = self.cv2.GaussianBlur(gray, (21, 21), 0)

        diff = self.cv2.absdiff(self.background_gray, gray)
        _, thresh = self.cv2.threshold(diff, 25, 255, self.cv2.THRESH_BINARY)
        contours, _ = self.cv2.findContours(thresh, self.cv2.RETR_EXTERNAL, self.cv2.CHAIN_APPROX_SIMPLE)

        for contour in contours:
            if self.cv2.contourArea(contour) > 500:
                (x, y, w, h) = self.cv2.boundingRect(contour)
                self.cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)

        if thresh.sum() > 0:
            return {'frame': frame,
                    'movement': True}
        else:
            return {'frame': frame,
                    'movement': False}

    def create_session(self, events_bag):
        events_count = 0
        first_event, last_event = None, None
        for ele in events_bag.read():
            events_count += 1
            self.events_processed.inc()
            if not first_event or first_event['timestamp'] > ele['timestamp']:
                first_event = ele
            if not last_event or last_event['timestamp'] < ele['timestamp']:
                last_event = ele

        start_time = first_event['timestamp']
        end_time = last_event['timestamp']
        duration = end_time - start_time
        self._distribution.update(duration)
        session_id = uuid.uuid4()
        return {'session_id': session_id, 
                'start_time': start_time, 
                'end_time': end_time, 
                'duration': duration, 
                'events_processed': events_count}

    def process(self, element, element_timestamp: Timestamp = beam.DoFn.TimestampParam,
                events_bag=beam.DoFn.StateParam(EVENTS_BAG),
                active_state=beam.DoFn.StateParam(ACTIVE_STATE),
                max_timestamp_seen=beam.DoFn.StateParam(MAX_TIMESTAMP),
                expiry_timer=beam.DoFn.TimerParam(EXPIRY_TIMER)):
        key, msg = element
        timestamp = msg['timestamp']
        frame_id = msg['frame_id']
        fps = msg['frame_rate']
        frame_bytes = msg['frame_bytes']
        frame_bytes = base64.b64decode(frame_bytes)
        frame_array = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
        print('Frame:', frame_id, frame.shape)

        result = self.detectMovementInVideo(frame)
        if result['movement']:
        #if msg['movement']:
            print(f'Movement detected in frame_id: {frame_id}, timestamp: {element_timestamp.to_utc_datetime()}')
            events_bag.add(element[1])
            self.events_received.inc()
            max_timestamp_seen.add(element_timestamp.seconds())
            if not active_state.read():
                active_state.write(True)
            expiration_time = Timestamp(max_timestamp_seen.read()) + Duration(seconds=30)
            print(f'Setting timer to expire at: {expiration_time.to_utc_datetime()}')
            expiry_timer.set(expiration_time)
        # If session is active but the movement stopped, we end the session and clear the state
        else:
            if active_state.read():
                session = self.create_session(events_bag)
                self.sessions_processed.inc()
                events_bag.clear()
                max_timestamp_seen.clear()
                active_state.clear()
                expiry_timer.clear()
                yield f'End session since movement stopped. Here are the details: {session}'

    @on_timer(EXPIRY_TIMER)
    def expiry_timer_callback(self, events_bag=beam.DoFn.StateParam(EVENTS_BAG),
                            active_state=beam.DoFn.StateParam(ACTIVE_STATE),
                            max_timestamp_seen=beam.DoFn.StateParam(MAX_TIMESTAMP)):
        session = self.create_session(events_bag)
        self.sessions_processed.inc()
        print(f'Expiry timer fired at: {Timestamp.now().to_utc_datetime()}, max_timestamp_seen: {max_timestamp_seen.read()}')
        events_bag.clear()
        max_timestamp_seen.clear()
        active_state.clear()
        yield f'End session since no movement detected for 30 seconds. Here are the details: {session}'


options = PipelineOptions(**{'num_workers': 0})
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
            max_read_time=30,
            #max_num_records=1000, 
            # key_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
            # value_deserializer='org.apache.kafka.common.serialization.ByteArrayDeserializer',
            # Set a longer polling timeout (default is 2s)
            # consumer_poll_timeout_ms=10000  # 10 seconds
        )
        | "Decode" >> beam.ParDo(DecodeMessage())
        | "AddTimestamps" >> beam.Map(lambda ele: beam.window.TimestampedValue(ele, ele['timestamp']))
        | "FixedWindowForSampling" >> beam.WindowInto(FixedWindows(1))
        | "Sampling" >> beam.combiners.Latest.Globally().without_defaults()
        | "GlobalWindows" >> beam.WindowInto(beam.window.GlobalWindows())
        #| "Map" >> beam.Map(parseMessage)
        #| "LogElements" >> beam.LogElements(with_window=True)
        | "Keys" >> beam.WithKeys(lambda ele: 'video')
        | "Session" >> beam.ParDo(DetectMovement())
        | "LogElements2" >> beam.LogElements()#with_window=True)
        )

# rtsp_url = 'rtsp://demo:demo@ipvmdemo.dyndns.org:5541/onvif-media/media.amp?'
# vid = cv2.VideoCapture()
# for i in range(10):
    # success, frame = vid.read()
    # print(f'Frame extracted: {success}')
# vid.release()


# rtsp_url = 'rtsp://demo:demo@ipvmdemo.dyndns.org:5541/onvif-media/media.amp?'
# vid = cv2.VideoCapture()
# for i in range(10):
    # success, frame = vid.read()
    # print(f'Frame extracted: {success}')
# vid.release()
