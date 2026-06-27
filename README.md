# ScamShield V2: Sistem Deteksi Real-Time Lowongan Kerja Palsu di Grup Telegram Menggunakan IndoBERT

![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white) ![Rich](https://img.shields.io/badge/CLI-Rich-000000?style=flat-square) ![Telethon](https://img.shields.io/badge/Listener-Telethon-2CA5E0?style=flat-square&logo=telegram&logoColor=white)
<br>
![Apache Spark](https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white) ![Apache Kafka](https://img.shields.io/badge/Ingestion-Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white) ![MongoDB Atlas](https://img.shields.io/badge/Database-MongoDB%20Atlas-47A248?style=flat-square&logo=mongodb&logoColor=white) ![IndoBERT](https://img.shields.io/badge/ML-IndoBERT-F9AB00?style=flat-square)

---

## 📌 Latar Belakang

Grup Telegram lowongan kerja menjadi salah satu kanal favorit pencari kerja di Indonesia — sekaligus target favorit modus penipuan lowongan kerja palsu (*job scam*). Korban biasanya diminta membayar "biaya admin", mengirim data pribadi, atau diarahkan ke tautan phishing, dengan kerugian yang terus berulang karena moderasi manual di puluhan grup sekaligus mustahil dilakukan secara konsisten.

ScamShield V2 hadir sebagai sistem pemantauan otomatis yang membaca pesan masuk di puluhan grup Telegram lowongan kerja secara real-time, lalu mengklasifikasikan setiap pesan (teks maupun poster gambar) sebagai **🚨 SCAM** atau **✅ AMAN** menggunakan model *deep learning* IndoBERT yang sudah dilatih khusus pada data SMS spam dan berita hoax berbahasa Indonesia.

---

## 🚀 Penjelasan Aplikasi

ScamShield V2 mengandalkan **Apache Kafka** sebagai antrian pesan dan **Apache Spark Structured Streaming** sebagai mesin pemrosesan inti yang menjalankan inferensi model IndoBERT secara streaming. Pesan masuk diambil oleh listener **Telethon** (termasuk OCR via Tesseract untuk poster gambar lowongan), dipublikasikan ke topik Kafka, lalu dikonsumsi oleh Spark untuk diklasifikasikan dan disimpan ke **MongoDB Atlas** sebagai basis data terpusat.

Pada sisi antarmuka, hasil deteksi ditampilkan melalui dua mode: dashboard analitik web berbasis **Streamlit** (metrik, grafik distribusi, dan tabel live feed), serta monitor log interaktif berbasis terminal menggunakan **Rich**.


---

## 👥 Anggota Tim & Kontribusi


| No. | Nama | NRP | Peran Kelompok | Tanggung Jawab Teknis Utama |
|---|---|---|---|---|
| 1 | **Muhammad Fachry Shalahuddin Rusamsi** | 5027241031 | Database Administrator | Mengelola klaster MongoDB Atlas (`scamshield.telegram_logs`) dan skrip admin di `database-storage/`. |
| 2 | **Abiyyu Raihan Putra Wikanto** | 5027241042 | Data Engineer | Listener Telegram (Telethon) + ekstraksi OCR poster gambar lowongan. |
| 3 | **Daniswara Fausta Novanto** | 5027241050 | UI/UX & Frontend Developer | Membangun dashboard (`frontend/app_dashboard.py`) dan CLI monitor (`frontend/app_cli.py`); mengoordinasikan integrasi keseluruhan sistem. |
| 4 | **Muhammad Khairul Yahya** | 5027241092 | ML & NLP Specialist | Melatih model IndoBERT pada dataset SMS spam + hoax Indonesia (`ml-nlp/AI_SCAMSHIELD.ipynb`). |
| 5 | **Muhammad Huda Rabbani** | 5027241098 | Big Data Developer | Membangun pipeline `big-data-processing/spark_pipeline.py` — konsumsi Kafka, inferensi model via UDF, dan penulisan hasil streaming. |


---

## 📁 Struktur Folder (Monorepo)

```
├── /FE                # Dashboard Streamlit & CLI Monitor 
│   ├── app_dashboard.py
│   └── app_cli.py
├── /ml-nlp                    # [ML & NLP Specialist] Training IndoBERT + Listener Telegram & OCR
│   └── AI_SCAMSHIELD.ipynb
├── /big-data-processing        # [Big Data Developer] Kafka Consumer + Spark Structured Streaming
│   ├── spark_pipeline.py
│   └── requirements.txt
├── /database-storage           # [Database & Storage Engineer] Admin MongoDB Atlas + Sample Output
│   ├── ScamShield_Database_Admin.ipynb
│   └── sample_output/          # Contoh hasil klasifikasi (output asli Spark)
│       ├── part-00000-00aca37f-...c000.csv
│       ├── part-00000-0ce57249-...c000.csv
│       └── part-00000-0ef21029-...c000.csv
├── .env.example                # Template variabel environment (copy → .env, isi sendiri)
├── .gitignore
└── README.md
```

---

## 🛠️ Cara Menjalankan Aplikasi

### 1. ML/NLP & Telegram Listener (`ml-nlp/AI_SCAMSHIELD.ipynb`)

Notebook ini berisi training model IndoBERT (dijalankan di Google Colab dengan GPU) serta listener Telethon untuk memantau grup Telegram secara real-time.

1. Buka `ml-nlp/AI_SCAMSHIELD.ipynb` di Google Colab.
2. Jalankan sel training secara berurutan — model hasil training akan tersimpan ke folder `scamshield_model_final/` (di Google Drive Anda; **tidak** disertakan di repo karena ukurannya besar).
3. Pada sel konfigurasi Telethon, **wajib** ganti `api_id`/`api_hash` yang ada saat ini (sudah pernah ter-expose di chat/dokumen lain) dengan kredensial baru dari [my.telegram.org](https://my.telegram.org). Idealnya pindahkan ke `.env`/Colab Secrets, jangan hardcode lagi di sel.

### 2. Big Data Processing (`big-data-processing/spark_pipeline.py`)

Prasyarat: Apache Kafka + Zookeeper sudah berjalan secara lokal (`localhost:9092`) dengan topik `scamshield_stream` sudah dibuat, serta folder model hasil training dari langkah 1 sudah ditaruh di lokasi yang sesuai.

```bash
cd big-data-processing
pip install -r requirements.txt

# di root repo: copy .env.example -> .env, isi MONGO_URI dan MODEL_PATH
python spark_pipeline.py
```

### 3. Database & Storage (`database-storage/`)

- MongoDB Atlas dengan database `scamshield`, collection `telegram_logs` (dibuat otomatis saat insert pertama oleh `spark_pipeline.py`).
- `ScamShield_Database_Admin.ipynb` berperan sebagai **dokumen referensi skema & utilitas admin** (misal pembuatan index), bukan bagian dari pipeline otomatis saat runtime.

### 4. Frontend Dashboard & CLI (`frontend/`)

```bash
cd frontend
pip install streamlit pandas numpy matplotlib rich

python -m streamlit run app_dashboard.py        # dashboard web
python app_cli.py                     # CLI monitor (mock data)
```

---


