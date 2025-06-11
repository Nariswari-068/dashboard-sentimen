import streamlit as st
from pymongo import MongoClient
import os
import pandas as pd
import altair as alt
import sys
import subprocess

print("Python version:", sys.version)

try:
    import pymongo
    print("pymongo version:", pymongo.__version__)
except ModuleNotFoundError:
    print("pymongo not found, installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo"])
    import pymongo
    print("pymongo installed:", pymongo.__version__)


st.set_page_config(page_title="Dashboard Sentimen Vasektomi", layout="wide")

st.markdown(
    """
    <style>
    body {
        background: linear-gradient(120deg, #89CFF0, #FFC0CB);
        background-attachment: fixed;
    }
    .stApp {
        background: transparent;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Koneksi MongoDB ---
mongo_client = MongoClient("mongodb+srv://Nariswari-068:UPNVeteran30@sentimen.zjoyizd.mongodb.net/")
db = mongo_client['sentimen_db']
collection = db['komentar_yt']

# --- Judul dan Header ---
st.title("📊 Dashboard Analisis Sentimen Publik")
st.markdown("_Analisis real-time sentimen masyarakat terkait kebijakan vasektomi melalui komentar YouTube._")

# --- Ambil data ---
@st.cache_data(ttl=30)
def ambil_data():
    data = list(collection.find().sort("_id", -1))
    return pd.DataFrame(data)

df = ambil_data()
df = df.sort_values(by='_id', ascending=False)

if df.empty:
    st.warning("❌ Belum ada data komentar yang tersedia.")
    st.stop()

# --- Bersihkan data ---
if '_id' in df.columns:
    df = df.drop(columns=['_id'])

# --- Filter kata kunci  ---
kata_filter = st.text_input("🔍 Filter komentar berdasarkan kata (opsional)", "")
if kata_filter:
    df = df[df['komentar_asli'].str.contains(kata_filter, case=False, na=False)]

# --- Statistik ---
total_komentar = len(df)
positif_pct = round((df['sentimen'] == 'positif').mean() * 100, 2)
netral_pct = round((df['sentimen'] == 'netral').mean() * 100, 2)
negatif_pct = round((df['sentimen'] == 'negatif').mean() * 100, 2)

col1, col2, col3, col4 = st.columns(4)
col1.metric("💬 Total Komentar", total_komentar)
col2.metric("😊 Positif", f"{positif_pct}%")
col3.metric("😐 Netral", f"{netral_pct}%")
col4.metric("😠 Negatif", f"{negatif_pct}%")

# --- Visualisasi Sentimen ---
st.subheader("📊 Distribusi Sentimen")
sentimen_count = df['sentimen'].value_counts().reset_index()
sentimen_count.columns = ['Sentimen', 'Jumlah']

chart = alt.Chart(sentimen_count).mark_bar().encode(
    x=alt.X(
        'Sentimen:N',
        sort=['positif', 'netral', 'negatif'],
        axis=alt.Axis(labelAngle=0)
    ),
    y='Jumlah:Q',
    color=alt.Color('Sentimen:N', scale=alt.Scale(
        domain=['positif', 'netral', 'negatif'],
        range=['#008000', '#1E90FF', '#B22222']
    ))
).properties(
    width=700,
    height=400,
    background='transparent'
).configure_axis(
    labelColor='white',
    titleColor='white'
).configure_legend(
    labelColor='white',
    titleColor='white'
)

st.altair_chart(chart, use_container_width=True)

# --- Tabel Komentar ---
st.subheader("🗂️ Komentar Terbaru")
st.dataframe(df[['komentar_asli', 'sentimen', 'skor']], use_container_width=True, height=400)
