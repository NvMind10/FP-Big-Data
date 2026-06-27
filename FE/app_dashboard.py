# app_dashboard.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
import time
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# 1. KONFIGURASI TEMA VISUAL (Royal Blue & Peach)
# ==========================================
HEX_ROYAL_BLUE = "#1E3A8A"
HEX_PEACH      = "#FFDBB5"
HEX_LIGHT_BG   = "#292626"
HEX_DARK_BG    = "#AB9E9E"

st.set_page_config(page_title="ScamShield V2 Analytics", layout="wide", page_icon="🛡️")

st.markdown(f"""
    <style>
    .stApp {{ background-color: {HEX_LIGHT_BG}; }}
    h1, h2, h3 {{ color: {HEX_ROYAL_BLUE}; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }}
    .metric-box {{
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        border-top: 5px solid {HEX_ROYAL_BLUE};
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        text-align: center;
    }}
    .metric-val {{ font-size: 24px; font-weight: bold; color: {HEX_ROYAL_BLUE}; }}
    .metric-lbl {{ font-size: 14px; color: #64748B; margin-bottom: 5px; text-transform: uppercase; }}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. KONEKSI MONGODB
# ==========================================
MONGO_URI        = os.getenv("MONGO_URI")
MONGO_DB         = os.getenv("MONGO_DB", "scamshield")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "telegram_logs")

@st.cache_resource
def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def load_data(limit: int = 500) -> pd.DataFrame:
    """Ambil data terbaru dari MongoDB, kembalikan sebagai DataFrame."""
    client = get_client()
    col    = client[MONGO_DB][MONGO_COLLECTION]
    docs   = list(col.find().sort("_id", -1).limit(limit))

    if not docs:
        return pd.DataFrame(columns=["sender", "text", "prediction", "timestamp", "group"])

    df = pd.DataFrame(docs)

    # Normalisasi kolom wajib
    for col_name, default in [
        ("sender",     "unknown"),
        ("text",       ""),
        ("prediction", "✅ AMAN"),
        ("group",      "Telegram Public Group"),
    ]:
        if col_name not in df.columns:
            df[col_name] = default

    if "timestamp" not in df.columns:
        df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df[["sender", "text", "prediction", "timestamp", "group"]]


# ==========================================
# 3. VALIDASI KONEKSI
# ==========================================
if not MONGO_URI:
    st.error("❌ **MONGO_URI** tidak ditemukan! Pastikan file `.env` sudah diisi dan diletakkan di folder yang sama.")
    st.stop()

try:
    df_current = load_data(limit=500)
except Exception as e:
    st.error(f"❌ Gagal terhubung ke MongoDB: `{e}`")
    st.stop()

if df_current.empty:
    st.warning("⚠️ Koleksi MongoDB kosong. Belum ada data prediksi yang masuk.")
    

# ==========================================
# 4. TAMPILAN HEADER DASHBOARD
# ==========================================
st.title("🛡️ ScamShield V2 — Frontend Dashboard Analitik")
st.markdown("### Pemantauan Distribusi Cyber-Scam Telegram Secara Real-Time")
st.caption(f"Sumber data: `{MONGO_DB}.{MONGO_COLLECTION}` · Terakhir diperbarui: {datetime.now().strftime('%H:%M:%S')}")
st.write("---")

# ==========================================
# 5. KALKULASI & KARTU METRIK
# ==========================================
total_trafik = len(df_current)
total_scam   = len(df_current[df_current["prediction"] == "🚨 SCAM"])
total_aman   = len(df_current[df_current["prediction"] == "✅ AMAN"])
rasio_scam   = (total_scam / total_trafik) * 100 if total_trafik > 0 else 0

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.markdown(f'<div class="metric-box"><div class="metric-lbl">Total Pesan Diproses</div><div class="metric-val">{total_trafik}</div></div>', unsafe_allow_html=True)
with m2:
    st.markdown(f'<div class="metric-box" style="border-top-color:#EF4444;"><div class="metric-lbl">Pesan Terdeteksi SCAM</div><div class="metric-val" style="color:#EF4444;">🚨 {total_scam}</div></div>', unsafe_allow_html=True)
with m3:
    st.markdown(f'<div class="metric-box" style="border-top-color:#10B981;"><div class="metric-lbl">Pesan Terdeteksi Aman</div><div class="metric-val" style="color:#10B981;">✅ {total_aman}</div></div>', unsafe_allow_html=True)
with m4:
    st.markdown(f'<div class="metric-box" style="border-top-color:#F59E0B;"><div class="metric-lbl">Rasio Ancaman Grup</div><div class="metric-val" style="color:#F59E0B;">{rasio_scam:.1f}%</div></div>', unsafe_allow_html=True)

st.write("")
st.write("")

# ==========================================
# 6. GRAFIK VISUALISASI
# ==========================================
col_left, col_right = st.columns(2)

with col_left:
    st.write("### 📊 Rasio Distribusi Kategori")
    if total_trafik == 0:
        st.info("Belum ada data untuk ditampilkan.")
    else:
        fig, ax = plt.subplots(figsize=(6, 4))
        fig.patch.set_facecolor(HEX_DARK_BG)
        ax.set_facecolor(HEX_DARK_BG)
        colors = [HEX_ROYAL_BLUE, HEX_PEACH]
        ax.pie(
            [total_aman, total_scam],
            labels=["Aman / Valid", "Scam Penipuan"],
            autopct="%1.1f%%",
            startangle=90,
            colors=colors,
            wedgeprops={"edgecolor": "white", "linewidth": 2},
        )
        ax.axis("equal")
        st.pyplot(fig)

with col_right:
    st.write("### 👤 Akun Penipu Paling Aktif (Top 5)")
    df_scam_only = df_current[df_current["prediction"] == "🚨 SCAM"]
    top_scammers = df_scam_only["sender"].value_counts().head(5)

    if not top_scammers.empty:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        fig2.patch.set_facecolor(HEX_DARK_BG)
        ax2.set_facecolor(HEX_DARK_BG)
        top_scammers.plot(kind="barh", color=HEX_ROYAL_BLUE, ax=ax2)
        ax2.invert_yaxis()
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)
        ax2.set_xlabel("Jumlah Pesan Terkirim")
        st.pyplot(fig2)
    else:
        st.info("Belum ada data serangan scam yang masuk.")

st.write("---")

# ==========================================
# 7. TABEL LIVE FEED
# ==========================================
st.write("### 📋 Aliran Tangkapan Data Terbaru (Live Ingestion Feed)")
st.dataframe(df_current.head(10), use_container_width=True)

if st.button("🔄 Refresh Data dari MongoDB"):
    st.cache_resource.clear()
    st.rerun()

# Auto-refresh setiap 5 detik
time.sleep(5)
st.rerun()