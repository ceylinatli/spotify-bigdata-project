import os
import json
from kafka import KafkaConsumer

#  Spotify Kafka Consumer (Test Amaçlı)

# Çevresel değişkenlerden yapılandırma ayarlarını alıyoruz
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'spotify-tracks')

def consume_stream():
    """
    Kafka topic'ini dinler ve gelen mesajları ekrana yazdırır.
    """
    print(f"Kafka Consumer başlatılıyor... Broker: {KAFKA_BROKER}, Topic: {TOPIC_NAME}")
    
    # Kafka Consumer'ı başlatıyoruz
    # auto_offset_reset='earliest' ile baştan itibaren okumasını sağlayabiliriz, 'latest' sadece yenileri okur.
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',             # Sadece yeni gelen mesajları oku
        enable_auto_commit=True,                # Okunan mesajların offet'lerini otomatik kaydet
        group_id='test-consumer-group',         # Tüketici grubu ID'si
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # JSON verisini Python sözlüğüne çevir (Deserialize)
    )
    
    print("Mesajlar dinleniyor... Çıkmak için CTRL+C'ye basın.\n" + "-"*50, flush=True)
    
    try:
        # Sonsuz döngü ile topic'teki yeni mesajları bekliyoruz
        for message in consumer:
            data = message.value
            # Gelen veriyi formatlı bir şekilde ekrana yazdırıyoruz
            print(f"YENİ MESAJ ALINDI: {data}", flush=True)
    except KeyboardInterrupt:
        # Kullanıcı CTRL+C ile durdurduğunda devreye girer
        print("\nConsumer durduruldu.")
    finally:
        # Program kapanırken consumer bağlantısını kapatıyoruz
        consumer.close()

if __name__ == "__main__":
    consume_stream()