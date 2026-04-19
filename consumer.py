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
    START_TIME = ReadModifyWriteStateSpec('start_time', FloatCoder())
    LAST_SEEN_TIME = ReadModifyWriteStateSpec('last_seen_time', FloatCoder())
    INACTIVITY_TIMER = TimerSpec('inactivity_timer', time_domain=beam.TimeDomain.WATERMARK)
    MAX_DURATION_TIMER = TimerSpec('max_duration_timer', time_domain=beam.TimeDomain.WATERMARK)

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

    def create_session(self, reason, events_bag, start_time, last_seen_time):
        events_count = sum(1 for _ in events_bag.read()) #events_bag.read().size()
        _end_time = last_seen_time.read()
        _start_time = start_time.read()
        duration = _end_time - _start_time
        self._distribution.update(duration)
        session_id = uuid.uuid4()
        return {'session_id': session_id, 
                'reason': reason,
                'start_time': _start_time, 
                'end_time': _end_time, 
                'duration': duration, 
                'events_processed': events_count}

    def process(self, element, element_timestamp: Timestamp = beam.DoFn.TimestampParam,
                events_bag=beam.DoFn.StateParam(EVENTS_BAG),
                start_time=beam.DoFn.StateParam(START_TIME),
                last_seen_time=beam.DoFn.StateParam(LAST_SEEN_TIME),
                inactivity_timer=beam.DoFn.TimerParam(INACTIVITY_TIMER),
                max_duration_timer=beam.DoFn.TimerParam(MAX_DURATION_TIMER)):
        key, msg = element
        timestamp = msg['timestamp']
        frame_id = msg['frame_id']
        fps = msg['frame_rate']
        frame_bytes = msg['frame_bytes']
        frame_bytes = base64.b64decode(frame_bytes)
        frame_array = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
        print(f"[{msg['movement']}] Frame:", frame_id)

        #result = self.detectMovementInVideo(frame)
        # if there is presence of an object in the frame
        #if result['movement']:
        if msg['movement']:
            events_bag.add(element[1])
            self.events_received.inc()
            # If it's the first movement event
            if start_time.read() is None:
                print(f'Movement detected in frame_id: {frame_id}, timestamp: {element_timestamp.to_utc_datetime()}')
                start_time.write(element_timestamp.seconds())
                # Max session duration of 5 minutes
                # This is to ensure that we don't have infinitely long sessions in case of continuous movement
                max_duration_expiry = Timestamp(element_timestamp.seconds()) + Duration(seconds=2*60)
                max_duration_timer.set(max_duration_expiry)
                print(f'Setting max duration timer to expire at: {max_duration_expiry.to_utc_datetime()}')
            last_seen_time.write(element_timestamp.seconds())
            print(f'Movement ongoing inframe_id: {frame_id}, timestamp: {element_timestamp.to_utc_datetime()}')
            # If no movement for 30 seconds, we consider session ended
            # This is to account for object occlusion or brief exit from the frame, we don't want to end the session immediately
            inactivity_expiry = Timestamp(element_timestamp.seconds()) + Duration(seconds=60)
            inactivity_timer.set(inactivity_expiry)
            print(f'Setting inactivity timer to expire at: {inactivity_expiry.to_utc_datetime()}')

    def _clear_state(self, events_bag, start_time, last_seen):
        events_bag.clear()
        start_time.clear()
        last_seen.clear()
    
    @on_timer(INACTIVITY_TIMER)
    def on_inactivity_timeout(self, events_bag=beam.DoFn.StateParam(EVENTS_BAG),
                            start_time=beam.DoFn.StateParam(START_TIME),
                            last_seen_time=beam.DoFn.StateParam(LAST_SEEN_TIME)):
        if start_time.read() is not None:
            reason = 'Inactivity'
            session = self.create_session(reason, events_bag, start_time, last_seen_time)
            self.sessions_processed.inc()
            print(f'Inactivity timer fired at: {Timestamp.now().to_utc_datetime()}')
            self._clear_state(events_bag, start_time, last_seen_time)
            yield f'End session since no movement detected for 30 seconds. Here are the details: {session}'

    @on_timer(MAX_DURATION_TIMER)
    def on_max_duration_timeout(self, events_bag=beam.DoFn.StateParam(EVENTS_BAG),
                                        start_time=beam.DoFn.StateParam(START_TIME),
                                        last_seen_time=beam.DoFn.StateParam(LAST_SEEN_TIME)):
        if start_time.read() is not None:
            reason = 'Max Duration'
            session = self.create_session(reason, events_bag, start_time, last_seen_time)
            self.sessions_processed.inc()
            print(f'Max duration timer fired at: {Timestamp.now().to_utc_datetime()}')
            self._clear_state(events_bag, start_time, last_seen_time)
            yield f'End session since max duration of 5 minutes reached. Here are the details: {session}'


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
