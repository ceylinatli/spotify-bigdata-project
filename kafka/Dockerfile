# ─────────────────────────────────────────────────────────────
#  Kafka Broker Dockerfile
#  NOT: Bu Dockerfile çoğunlukla docker-compose'daki hazır
#       Confluent imajı ile değiştirilir. Özel yapılandırma
#       gerekirse burası kullanılır.
# ─────────────────────────────────────────────────────────────

FROM confluentinc/cp-kafka:7.5.0

# Kafka yapılandırma dosyasını kopyala (özel ayarlar gerekirse)
# COPY kafka.properties /etc/kafka/kafka.properties

# Kafka varsayılan portları
EXPOSE 9092 29092

# Kafka broker'ı başlat
CMD ["/etc/confluent/docker/run"]
