from kafka import KafkaConsumer
import json
import re
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from pymongo import MongoClient
import nltk
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from nltk.corpus import stopwords

# --- Setup awal ---
nltk.download('stopwords')
stop_words = set(stopwords.words('indonesian'))
stemmer = StemmerFactory().create_stemmer()

# MongoDB
mongo_client = MongoClient("mongodb+srv://Nariswari-068:UPNVeteran30@sentimen.zjoyizd.mongodb.net/")
db = mongo_client['sentimen_db']
collection = db['komentar_yt']

# Load IndoBERT sentimen
model_name = "mdhugol/indonesia-bert-sentiment-classification"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)
sentiment_pipeline = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

# Kafka Consumer
consumer = KafkaConsumer(
    'komentar-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    group_id='sentimen-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("✅ Consumer Kafka berjalan...\n")

# --- Fungsi bantu ---
def bersihkan_teks(teks):
    teks = teks.lower()
    teks = re.sub(r"http\S+|www.\S+", "", teks)
    teks = re.sub(r"[^\w\s]", "", teks)
    teks = re.sub(r"\s+", " ", teks).strip()
    return teks

def preprocess(teks):
    teks = bersihkan_teks(teks)
    kata2 = teks.split()
    kata2 = [k for k in kata2 if k not in stop_words]
    teks = " ".join(kata2)
    teks = stemmer.stem(teks)
    return teks

label_map = {'LABEL_0': 'positif', 'LABEL_1': 'netral', 'LABEL_2': 'negatif'}

# --- Loop utama ---
for msg in consumer:
    komentar = msg.value

    # Cek apakah komentar benar-benar string
    if not isinstance(komentar, str):
        print("❌ Bukan string. Lewat.")
        continue

    print(f"\n📥 Komentar diterima: {komentar}")

    # Preprocessing komentar
    komentar_bersih = preprocess(komentar)
    print(f"🧹 Bersih: {komentar_bersih}")

    # Analisis sentimen (dengan pemotongan aman)
    hasil = sentiment_pipeline(komentar_bersih[:450], truncation=True)[0]
    label = label_map.get(hasil['label'], 'netral')
    skor = round(hasil['score'], 3)

    print(f"🔍 Sentimen: {label} (Skor: {skor})")

    # Simpan ke MongoDB
    doc = {
        "komentar_asli": komentar,
        "komentar_bersih": komentar_bersih,
        "sentimen": label,
        "skor": skor
    }
    collection.insert_one(doc)
    print("✅ Disimpan ke MongoDB.")
