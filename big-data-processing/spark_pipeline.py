import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()

from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

# =========================================================
# KONFIGURASI MONGODB (dipindah ke .env -- JANGAN hardcode di sini)
# =========================================================

load_dotenv()  # baca file .env di root project

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError(
        "MONGO_URI tidak ditemukan. Buat file .env (copy dari .env.example) "
        "dan isi MONGO_URI dengan connection string MongoDB Atlas Anda."
    )

# =========================================================
# LOAD MODEL SEKALI SAJA
# =========================================================

print("🧠 Loading IndoBERT Model...")

import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification

MODEL_PATH = os.getenv("MODEL_PATH", "./scamshield_model_final")

tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)

print("✅ Model Loaded")

# =========================================================
# SPARK SESSION
# =========================================================

print("🚀 Memulai Apache Spark Pipeline...")

spark_version = pyspark.__version__

scala_version = "2.13" if spark_version.startswith("4") else "2.12"

kafka_connector = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

spark = (
    SparkSession.builder
    .appName("ScamShield_MongoDB_Pipeline")
    .master("local[1]")
    .config(
        "spark.jars.packages",
        kafka_connector
    )
    .config(
        "spark.sql.execution.arrow.pyspark.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =========================================================
# AI PREDICTION FUNCTION
# =========================================================

def tebak_scam_internal(teks):

    if not teks or not teks.strip():
        return "AMAN"

    inputs = tokenizer(
        teks,
        return_tensors="pt",
        truncation=True,
        padding=True,
        max_length=128
    )

    with torch.no_grad():
        outputs = model(**inputs)

    probs = F.softmax(outputs.logits, dim=1).squeeze()

    if probs.dim() == 0:
        pred_id = torch.argmax(outputs.logits).item()
    else:
        pred_id = torch.argmax(probs).item()

    return "SCAM" if pred_id == 1 else "AMAN"

tebak_scam_udf = udf(
    tebak_scam_internal,
    StringType()
)

# =========================================================
# READ KAFKA STREAM
# =========================================================

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    .option(
        "subscribe",
        os.getenv("KAFKA_TOPIC", "scamshield_stream")
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)

# =========================================================
# PARSE JSON
# =========================================================

schema = StructType([
    StructField("sender", StringType(), True),
    StructField("text", StringType(), True)
])

parsed_df = (
    df.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)

# =========================================================
# AI ANALYSIS
# =========================================================

final_df = parsed_df.withColumn(
    "status_ai",
    tebak_scam_udf(col("text"))
)

# =========================================================
# CONSOLE OUTPUT
# =========================================================

console_output = (
    final_df.writeStream
    .outputMode("append")
    .format("console")
    .start()
)

# Tempelkan fungsi ini tepat di atas fungsi write_to_mongodb milik Huda

def tampilkan_log_scamshield(waktu, pengirim, grup, teks, status):
    # Menentukan skema warna berdasarkan hasil prediksi AI
    if "SCAM" in status:
        warna_status = "bold red"
        badge = "[📊 SCAM DETECTED]"
        border_color = "red"
    else:
        warna_status = "bold green"
        badge = "[✅ SAFE MESSAGE]"
        border_color = "green"
        
    # Menyusun konten teks log di terminal
    konten = Text()
    konten.append(f"🕒 Waktu   : {waktu}\n", style="cyan")
    konten.append(f"👤 Pengirim: {pengirim}\n", style="yellow")
    konten.append(f"👥 Grup    : {grup}\n", style="magenta")
    konten.append(f"💬 Pesan   : \"{teks}\"\n\n", style="white")
    konten.append(f"🔬 Status Validation: ", style="bold white")
    konten.append(f"{status}", style=warna_status)
    
    # Cetak log menggunakan komponen Panel Box dari Rich Library
    console.print(
        Panel(
            konten,
            title=f"[bold white]{badge}[/bold white]",
            expand=False,
            border_style=border_color
        )
    )

# =========================================================
# MONGODB WRITER
# =========================================================

def write_to_mongodb(batch_df, batch_id):
    rows = batch_df.collect()
    if len(rows) == 0:
        return

    client = MongoClient(MONGO_URI)
    db = client["scamshield"]
    collection = db["telegram_logs"]
    docs = []

    # Ambil penanda waktu saat ini untuk log terminal
    waktu_sekarang = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in rows:
        # 1. TAMPILKAN LOG VISUAL WARNA-WARNI ANDA DI SINI (KODE KAMU BERAKSI):
        tampilkan_log_scamshield(
            waktu=waktu_sekarang,
            pengirim=row["sender"],
            grup="Telegram Public Group",
            teks=row["text"],
            status=row["status_ai"]
        )

        # 2. Pembentukan dokumen MongoDB bawaan kelompok (tetap biarkan seperti aslinya)
        docs.append({
            "timestamp": datetime.now().isoformat(),
            "sender": row["sender"],
            "group": "telegram_group",
            "message_type": "text",
            "message_text": row["text"],
            "status_ai": row["status_ai"],
            "confidence_score": None,
            "source": "spark_stream"
        })

    collection.insert_many(docs)
    print(f"✅ Batch {batch_id} - Successfully Synced to Cloud MongoDB & CLI Stream")

# =========================================================
# STREAM -> MONGODB
# =========================================================

mongo_output = (
    final_df.writeStream
    .outputMode("append")
    .foreachBatch(write_to_mongodb)
    .start()
)

# =========================================================
# RUN
# =========================================================

print("🔥 Kafka → Spark → MongoDB Aktif")

spark.streams.awaitAnyTermination()
