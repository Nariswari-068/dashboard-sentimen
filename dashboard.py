import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt

# --- Koneksi MongoDB ---
mongo_client = MongoClient("mongodb+srv://Nariswari-068:UPNVeteran30@sentimen.zjoyizd.mongodb.net/")
<<<<<<< HEAD
db = mongo_client['sentimen_db']
=======
db = client['sentimen_db']
>>>>>>> f5670aefc9ef2762211a826074859f14629e4b6b
collection = db['komentar_vasektomi']

# --- Judul dan Header ---
st.set_page_config(page_title="Dashboard Sentimen Vasektomi", layout="wide")
st.title("📊 Dashboard Sentimen Komentar: Vasektomi")
st.markdown("_Analisis real-time sentimen masyarakat terkait kebijakan vasektomi melalui komentar YouTube._")

# --- Ambil data ---
@st.cache_data(ttl=30)
def ambil_data():
    data = list(collection.find().sort("_id", -1).limit(500))  # Maks 500 komentar terbaru
    return pd.DataFrame(data)

df = ambil_data()

if df.empty:
    st.warning("❌ Belum ada data komentar yang tersedia.")
    st.stop()

# --- Bersihkan data ---
if '_id' in df.columns:
    df = df.drop(columns=['_id'])

# --- Filter kata kunci (opsional) ---
kata_filter = st.text_input("🔍 Filter komentar berdasarkan kata (opsional)", "")
if kata_filter:
    df = df[df['komentar_asli'].str.contains(kata_filter, case=False, na=False)]

# --- Statistik ---
col1, col2, col3 = st.columns(3)
col1.metric("💬 Total Komentar", len(df))
positif_pct = round((df['sentimen'] == 'positif').mean() * 100, 2)
col2.metric("😊 Proporsi Positif", f"{positif_pct}%")
col3.metric("😠 Proporsi Negatif", f"{round((df['sentimen'] == 'negatif').mean() * 100, 2)}%")

# --- Visualisasi Sentimen ---
st.subheader("📊 Distribusi Sentimen")
sentimen_count = df['sentimen'].value_counts().reset_index()
sentimen_count.columns = ['Sentimen', 'Jumlah']

chart = alt.Chart(sentimen_count).mark_bar().encode(
    x=alt.X('Sentimen:N', sort=['positif', 'negatif']),
    y='Jumlah:Q',
    color=alt.Color('Sentimen:N', scale=alt.Scale(domain=['positif', 'negatif'], range=['green', 'red']))
).properties(width=700, height=400)

st.altair_chart(chart, use_container_width=True)

# --- Tabel Komentar ---
st.subheader("🗂️ Komentar Terbaru")
st.dataframe(df[['komentar_asli', 'sentimen', 'skor']], use_container_width=True, height=400)

# --- Footer ---
st.markdown("""---  
🛠️ Dibuat oleh **Nariswari** | Model: IndoBERT Sentiment | Backend: Kafka + MongoDB | UI: Streamlit  
""")
