# ─────────────────────────────────────────────────────────────
#  Kafka Producer & Consumer Dockerfile
#  Temel imaj: Python 3.10 slim (küçük boyutlu imaj)
# ─────────────────────────────────────────────────────────────

FROM python:3.10-slim

# Çalışma dizinini ayarla
WORKDIR /app

# Sistem bağımlılıklarını kur (derleme gerektiren paketler için gcc vb.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*    # Önbelleği temizle (imaj boyutunu küçültür)

# Python bağımlılık dosyasını kopyala
COPY requirements.txt .

# Python paketlerini yükle
RUN pip install --no-cache-dir -r requirements.txt

# Kaynak kodları kopyala
COPY producer.py .
COPY consumer.py .

# Varsayılan olarak producer.py çalışsın (docker-compose'da bu konteyner producer olarak kullanılacak)
CMD ["python", "-u", "producer.py"]