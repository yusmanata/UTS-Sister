# Pub-Sub Log Aggregator

Sebuah layanan Pub-Sub log aggregator berbasis Python (FastAPI + asyncio) yang menerima event/log dari *publisher*, kemudian diproses oleh *consumer* dengan mengedepankan prinsip **idempotency** (tidak memproses ulang event yang sama) menggunakan basis data SQLite lokal.

## Struktur Direktori

```text
.
├── src/
│   ├── __init__.py
│   ├── main.py          # FastAPI endpoints (Pintu masuk/Aggregator API)
│   ├── models.py        # Validasi skema (Pydantic Event JSON)
│   ├── consumer.py      # Background worker (Subscriber internal queue)
│   ├── store.py         # Persistent SQLite deduplication store
│   └── publisher.py     # Simulasi Publisher (At-least-once delivery)
├── tests/
│   ├── __init__.py
│   └── test_main.py     # Unit & Stress Tests menggunakan pytest
├── Dockerfile           # Konfigurasi container Aggregator
├── docker-compose.yml   # Konfigurasi multi-container (Opsional/Bonus)
└── requirements.txt     # Dependensi proyek
```

## Instruksi Run Singkat (Single Container)
Build: 
```bash
docker build -t uts-aggregator .
```
Run: 
```bash
docker run -p 8080:8080 uts-aggregator
```

## Cara Build/Run (Bonus Docker Compose - Disarankan)
Untuk menjalankan **Aggregator** berserta **Simulator Publisher** (yang otomatis menembakkan trafik beserta duplikasi) dalam satu jaringan virtual:
```bash
docker compose up --build
```

Aplikasi dapat diakses melalui:
- API URL: `http://localhost:8080`
- Dokumentasi Otomatis (Swagger UI): `http://localhost:8080/docs`

## Endpoints

1. **`POST /publish`**
   - Menerima log. Bisa mengirimkan sebuah *object* JSON atau sebuah *Array/List* dari object JSON (Batch).
   - *Event JSON minimal*:
     ```json
     {
       "topic": "sensor-suhu",
       "event_id": "uuid-1234",
       "timestamp": "2023-10-25T14:30:00Z",
       "source": "sensor_1",
       "payload": {"celcius": 28}
     }
     ```
   - *Response*: `202 Accepted`

2. **`GET /events?topic={nama_topic}`**
   - Mengambil daftar semua log unik yang telah berhasil disimpan untuk suatu topik.
   - *Response*:
     ```json
     {
       "topic": "sensor-suhu",
       "events": [ ... ]
     }
     ```

3. **`GET /stats`**
   - Menampilkan agregasi metrik aplikasi dan performa *deduplication*.
   - *Response*:
     ```json
     {
       "received": 100,
       "unique_processed": 80,
       "duplicate_dropped": 20,
       "topics": ["sensor-suhu"],
       "uptime_seconds": 124.5
     }
     ```

## Asumsi Arsitektur

1. **Asynchronous In-Memory Queue**: Demi meraih *high throughput*, endpoint `/publish` tidak langsung berinteraksi dengan database (SQLite). Ia menaruh payload ke `asyncio.Queue` dan membalikkan respon seketika (Fast response). Database ditangani perlahan di balik layar oleh `consumer.py`.
2. **Local Persistensi**: Sesuai arahan soal, file `aggregator.db` digenerate di root proyek (atau di dalam container).
3. **Idempotency Limit**: Deduplikasi berpatok pada constraint kombinasi unik kolom tabel `UNIQUE(topic, event_id)` yang dicek dan dijaga ketat oleh SQLite *Engine*.

## Laporan Reliability & Ordering (Bagian C)

### At-Least-Once Delivery & Toleransi Crash
Sistem mengadopsi SQLite yang menjamin data tetap terproteksi. Apabila *container* mati atau direstart:
1. Metrik di `/stats` diselamatkan ke disk secara real-time.
2. Ketika *publisher* mengirim ulang event lama dengan ID identik, sistem tidak akan menyimpannya karena *state* event yang ada di database tidak ikut hilang pasca restart.

### Total Ordering (Dibutuhkan atau Tidak?)
Dalam skala dan konteks log aggregator ini, **Total Ordering secara global (antar lintas topik) TIDAK dibutuhkan dan tidak efisien.**
- **Alasan**: Sebagai layer agregasi infrastruktur (*seperti Kafka/PubSub murni*), fokus utamanya adalah `high-availability` (data selamat & tak ganda). Memaksa data dari berbagai ragam *publisher*/topik untuk ditata/diurutkan ketat secara sinkron saat dimasukkan (*write time*) akan mematikan performa secara eksponensial.
- **Solusi**: Pengurutan (*ordering*) sejatinya dapat dilimpahkan pada *Downstream Consumer* (client pengguna `/events`) yang bisa dengan mudah menggunakan parameter *timestamp* milik event tersebut untuk melakukan *Temporal Partial Ordering* di level pembacaan (*read time*).
