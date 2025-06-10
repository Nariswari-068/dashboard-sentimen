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
mongodb+srv://Nariswari-068:<UPNVeteran30>@sentimen.zjoyizd.mongodb.net/db = mongo_client['sentimen_db']
collection = db['komentar_vasektomi']

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

def tangani_negasi(teks):
    tokens = teks.split()
    hasil = []
    skip = False
    for i in range(len(tokens)):
        if skip:
            skip = False
            continue
        if tokens[i] in ['tidak', 'bukan', 'kurang', 'belum']:
            if i + 1 < len(tokens):
                hasil.append(tokens[i] + '_' + tokens[i + 1])
                skip = True
            else:
                hasil.append(tokens[i])
        else:
            hasil.append(tokens[i])
    return " ".join(hasil)

def custom_stemming(teks):
    hasil = []
    for kata in teks.split():
        if "_" in kata and kata.startswith(("tidak", "bukan", "kurang", "belum")):
            hasil.append(kata)
        else:
            hasil.append(stemmer.stem(kata))
    return " ".join(hasil)

def preprocess(teks):
    teks = bersihkan_teks(teks)
    teks = tangani_negasi(teks)  # Tangani negasi dulu
    kata2 = teks.split()
    kata2 = [k for k in kata2 if k not in stop_words]
    teks = " ".join(kata2)
    teks = custom_stemming(teks)  # Stem dengan pengecualian negasi
    return teks

def ekstrak_kata_kunci(teks_bersih):
    kata2 = teks_bersih.split()
    kata2 = [k for k in kata2 if k not in stop_words and len(k) > 3]
    return list(set(kata2))

# Map hanya 2 kelas: positif dan negatif
# Misal model label 0 dan 2 dianggap negatif, label 1 positif (sesuaikan jika perlu)
label_map = {
    'LABEL_0': 'negatif',
    'LABEL_1': 'positif',
    'LABEL_2': 'negatif'
}

NEGASI_KEYWORDS = {
    'tidak_setuju', 'bukan_setuju', 'kurang_setuju', 'belum_setuju',
    'menolak', 'tidak_mendukung', 'jangan', 'tidak_bagus', 'tidak_benar',
    'tidak_efektif', 'tidak_baik'
}

def post_process_sentimen(label, skor, komentar_bersih):
    tokens = set(komentar_bersih.split())
    if tokens.intersection(NEGASI_KEYWORDS):
        # Kalau ada kata negasi kuat, pasti negatif
        return 'negatif', max(skor, 0.75)
    # Kalau label positif tetap positif
    if label == 'positif':
        return 'positif', skor
    # Sisanya negatif
    return 'negatif', skor

# --- Loop utama ---
for msg in consumer:
    komentar = msg.value
    print(f"\n📥 Komentar diterima: {komentar}")

    # Preprocessing
    komentar_bersih = preprocess(komentar)
    print(f"🧹 Bersih: {komentar_bersih}")

    # Analisis Sentimen
    hasil = sentiment_pipeline(komentar_bersih)[0]
    raw_label = hasil['label']
    skor = round(hasil['score'], 3)

    # Map ke 2 kelas saja
    label = label_map.get(raw_label, 'negatif')

    # Post processing sentimen dengan rule negasi
    label, skor = post_process_sentimen(label, skor, komentar_bersih)

    print(f"🔍 Sentimen (2 kelas): {label} (Skor: {skor})")

    # Ekstrak keywords
    keywords = ekstrak_kata_kunci(komentar_bersih)

    # Simpan ke MongoDB
    doc = {
        "komentar_asli": komentar,
        "komentar_bersih": komentar_bersih,
        "keywords": keywords,
        "sentimen": label,
        "skor": skor
    }
    collection.insert_one(doc)
    print("✅ Disimpan ke MongoDB.")
