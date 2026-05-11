import os
import json
import time
import random
import uuid
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer

#  Spotify Kafka Producer

# Çevresel değişkenlerden yapılandırma ayarlarını alıyoruz
# Eğer Docker'dan gelmiyorsa varsayılan değerleri kullanıyoruz
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'spotify-tracks')
CSV_PATH = os.getenv('CSV_PATH', '../../data/raw/dataset.csv')
MESSAGES_PER_SECOND = int(os.getenv('MESSAGES_PER_SECOND', '50'))

def create_producer():
    """
    Kafka Producer örneği oluşturur ve döndürür.
    Mesajları JSON formatında serileştirir.
    """
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Gönderilen veriyi JSON olarak encode ediyoruz (UTF-8)
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def simulate_stream():
    """
    CSV dosyasını okuyarak veri akışını simüle eder ve Kafka'ya gönderir.
    """
    print(f"[{datetime.now()}] Kafka Broker'a bağlanılıyor: {KAFKA_BROKER}")
    
    # Kafka Producer'ı başlatıyoruz
    producer = create_producer()
    print(f"[{datetime.now()}] Producer başarıyla bağlandı. Hedef topic: {TOPIC_NAME}")
    
    # Veri setini Pandas ile okuyoruz (Eğer dosya yoksa hata verebilir, try-except eklenebilir)
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"[{datetime.now()}] Veri seti yüklendi: {CSV_PATH} ({len(df)} satır)")
    except Exception as e:
        print(f"HATA: Veri seti okunamadı! Yol: {CSV_PATH}. Hata: {e}")
        return

    # Gönderim hızı ayarı için iki mesaj arası bekleme süresi
    sleep_time = 1.0 / MESSAGES_PER_SECOND
    
    message_count = 0
    
    # Veri setindeki her bir satır üzerinde dönüyoruz
    for index, row in df.iterrows():
        # Her mesajda istenilen alanları oluşturuyoruz
        # user_id rastgele UUID olarak üretiliyor (simülasyon amaçlı)
        message = {
            "kafka_timestamp": datetime.utcnow().isoformat(),
            "user_id": str(uuid.uuid4()),
            "event_type": "track_played",
            "track_id": row.get("track_id", ""),
            "track_name": row.get("name", ""),
            "artists": row.get("artists", ""),
            "year": int(row["year"]) if str(row.get("year", "")).isdigit() else 0,
            "track_genre": row.get("track_genre", ""),
            "popularity": int(row.get("popularity", 0)),
        }
        
        # Mesajı Kafka topic'ine gönderiyoruz
        producer.send(TOPIC_NAME, value=message)
        message_count += 1
        
        # Belirli aralıklarla (örneğin her 500 mesajda bir) ekrana log basıyoruz
        if message_count % 500 == 0:
            print(f"[{datetime.now()}] {message_count} mesaj gönderildi...")
            # Tüm mesajların iletildiğinden emin olmak için buffer'ı temizliyoruz
            producer.flush()
            
        # Gönderim hızını ayarlamak için bekliyoruz
        time.sleep(sleep_time)

    # İşlem bitince kalan tüm mesajları gönderip bağlantıyı kapatıyoruz
    producer.flush()
    producer.close()
    print(f"[{datetime.now()}] Simülasyon tamamlandı. Toplam {message_count} mesaj gönderildi.")

if __name__ == "__main__":
    # Başlangıçta servisin hazır olması için kısa bir süre bekliyoruz
    print("Producer başlatılıyor, 5 saniye bekleniyor...")
    time.sleep(5)
    simulate_stream()