import os
import cv2
import time
import base64
from kafka import KafkaProducer
import json

#$ bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
topic = 'quickstart-events'
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    max_request_size=5242880,  # 5MB
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

HOME_PATH = os.getenv('HOME')
PROJECT_ROOT = os.path.join(HOME_PATH, 'random', 'traffic-monitoring')
video_path = os.path.join(PROJECT_ROOT, 'inputs/highway.mp4')

cap = cv2.VideoCapture(video_path)
fps = cap.get(cv2.CAP_PROP_FPS)
nframes = cap.get(cv2.CAP_PROP_FRAME_COUNT)
frame_interval = 1.0 / fps

frame_id = 0
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:# or frame_id==9:
        break

    print('Sending Frame:', frame_id+1)
    frame = cv2.resize(frame, None, fx=0.5, fy=0.5)

    if frame_id==0:
        bg_gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        blur_gray_frame = cv2.GaussianBlur(bg_gray_frame, (21, 21), 0)
        _ = cv2.imwrite('street_backgroud_gray.jpg', blur_gray_frame)

    # Encode frame as JPEG bytes
    _, buffer = cv2.imencode('.jpg', frame)
    frame_bytes = base64.b64encode(buffer).decode('utf-8')

    #if frame_id < 30:
    #    movement = 1
    #elif frame_id < 150:
    #    movement = 1
    #elif frame_id < 250:
    #    movement = 1
    #elif frame_id < 300:
    #    movement = 1
    #else:
    #   movement = 1

    message = {
        "timestamp": time.time(),
        "frame_id": frame_id,
        "frame_rate": fps,
        "frame_bytes": frame_bytes,
        #"bg_frame_bytes": bg_frame_bytes
        #"movement": movement,
    }

    future = producer.send(topic, message)
    frame_id += 1
    # time.sleep(frame_interval)  # simulate real-time streaming
    time.sleep(frame_interval)  # simulate real-time streaming
    try:
        record_metadata = future.get(timeout=10)
        print(f'Message sent successfully at {fps} fps')
        print('Topic:', record_metadata.topic)
    except Exception as e:
        print("Error sending message:", e)

cap.release()
producer.flush()
producer.close()

