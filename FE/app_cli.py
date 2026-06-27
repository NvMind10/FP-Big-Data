# app_cli.py
import time
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

console = Console()

THEME_BLUE = "bold royal_blue1"
THEME_PEACH = "bold sandy_brown"

# ==========================================
# KONEKSI MONGODB
# ==========================================
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB   = os.getenv("MONGO_DB", "scamshield")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "telegram_logs")

def get_mongo_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    return client, db[MONGO_COLLECTION]


def tampilkan_log_scamshield(waktu, pengirim, grup, teks, status):
    if status == "🚨 SCAM":
        status_print = f"[bold red]{status} [blink](BAHAYA)[/blink][/bold red]"
        border_color = "red"
        action_text = f"[{THEME_PEACH}]Tindakan:[/{THEME_PEACH}] [bold red]Auto-Append ke dataset_2026.csv (Continuous Learning)[/bold red]"
    else:
        status_print = f"[bold green]{status} (AMAN)[/bold green]"
        border_color = "green"
        action_text = f"[{THEME_PEACH}]Tindakan:[/{THEME_PEACH}] [dim green]Abaikan / Teruskan pesan[/dim green]"

    table = Table(
        title=f"🛡️ [bold white]ScamShield V2 - Live Stream Ingestion[/bold white]",
        title_style="on blue",
        box=None,
    )
    table.add_column("Komponen Data", style=THEME_BLUE, width=18)
    table.add_column("Nilai / Isi Teks", style="white")

    table.add_row("🕒 Waktu Sistem", waktu)
    table.add_row("👤 Akun Pengirim", pengirim)
    table.add_row("💬 Grup Telegram", grup)
    table.add_row("📝 Isi Teks / OCR", f'[italic]"{teks}"[/italic]')
    table.add_row("🤖 Hasil Analisis AI", status_print)

    console.print(Panel(table, border_style=border_color, expand=False))
    console.print(action_text)
    console.print("-" * 60)


def stream_dari_mongodb(poll_interval: int = 5, limit: int = 20):
    """
    Ambil N dokumen terbaru dari MongoDB, tampilkan satu per satu,
    lalu polling setiap `poll_interval` detik untuk dokumen baru.
    """
    client, col = get_mongo_collection()
    console.print(Panel(
        f"[{THEME_BLUE}]ScamShield V2 - Terminal UI Module Started[/{THEME_BLUE}]\n"
        f"[💡] Terhubung ke MongoDB: [bold]{MONGO_DB}.{MONGO_COLLECTION}[/bold]\n"
        f"[💡] Polling setiap {poll_interval} detik...",
        border_style="blue",
    ))

    seen_ids = set()

    try:
        while True:
            # Ambil dokumen terbaru yang belum ditampilkan
            cursor = col.find().sort("_id", -1).limit(limit)
            baru = [doc for doc in cursor if doc["_id"] not in seen_ids]

            if baru:
                for doc in reversed(baru):   # urutan kronologis
                    seen_ids.add(doc["_id"])
                    tampilkan_log_scamshield(
                        waktu    = str(doc.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))),
                        pengirim = doc.get("sender", "unknown"),
                        grup     = doc.get("group", "Telegram Public Group"),
                        teks     = doc.get("text", ""),
                        status   = doc.get("prediction", "✅ AMAN"),
                    )
                    time.sleep(1)
            else:
                console.print(f"[dim]Menunggu data baru... (last check: {datetime.now().strftime('%H:%M:%S')})[/dim]")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        console.print("\n[bold yellow]Simulasi terminal dihentikan.[/bold yellow]")
    finally:
        client.close()


if __name__ == "__main__":
    if not MONGO_URI:
        console.print("[bold red]❌ MONGO_URI tidak ditemukan! Pastikan file .env sudah diisi.[/bold red]")
        exit(1)

    stream_dari_mongodb(poll_interval=5, limit=20)