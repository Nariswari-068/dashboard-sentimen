# producer.py
from kafka import KafkaProducer
import json
from googleapiclient.discovery import build
import time

API_KEY = 'AIzaSyBPQ9lZXoAE64RGGWpbj6BNFxEzkKt2tL8'
VIDEO_ID = 'MX3EsyJNIAU'
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
youtube = build('youtube', 'v3', developerKey=API_KEY)

seen_ids = set()

def ambil_semua_komentar():
    next_page_token = None
    while True:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=VIDEO_ID,
            maxResults=100,
            order="time",  # boleh diganti jadi "relevance" kalau ingin urutan beda
            pageToken=next_page_token,
            textFormat="plainText"
        )
        response = request.execute()

        for item in response['items']:
            komentar_id = item['id']
            komentar = item['snippet']['topLevelComment']['snippet']['textDisplay']
            if komentar_id not in seen_ids:
                seen_ids.add(komentar_id)
                print("[KIRIM]", komentar)
                producer.send('komentar-topic', value=komentar)

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

# Panggil sekali untuk ambil semua komentar lama
ambil_semua_komentar()

# Lanjut polling komentar baru setiap 15 detik
while True:
    ambil_semua_komentar()
    time.sleep(10)