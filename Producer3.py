# producer.py
from kafka import KafkaProducer
import json
from googleapiclient.discovery import build
import time

API_KEY = 'AIzaSyB_U6EjL7rbIUBVOEiv8i-it0ur7sMZAdE'
VIDEO_IDS = ['MX3EsyJNIAU', '01VzjUUJjkk', 'n6CedDYDCvA']

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

youtube = build('youtube', 'v3', developerKey=API_KEY)

seen_ids = set()

def ambil_semua_komentar(video_id):
    next_page_token = None
    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                order="time",
                pageToken=next_page_token,
                textFormat="plainText"
            )
            response = request.execute()
        except Exception as e:
            print(f"[ERROR] Gagal ambil komentar dari {video_id}: {e}")
            break

        for item in response.get('items', []):
            komentar_id = item['id']
            komentar = item['snippet']['topLevelComment']['snippet']['textDisplay']
            if komentar_id not in seen_ids:
                seen_ids.add(komentar_id)
                print("[KIRIM]", komentar)
                producer.send('komentar-topic', value=komentar)

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

# Ambil komentar awal dari semua video
for vid in VIDEO_IDS:
    ambil_semua_komentar(vid)

# Poll komentar baru setiap 10 detik
while True:
    for vid in VIDEO_IDS:
        ambil_semua_komentar(vid)
    time.sleep(10)
