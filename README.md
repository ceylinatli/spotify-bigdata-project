# 🎵 Spotify Büyük Veri Pipeline Projesi

> **Ders:** Büyük Veri Analizine Giriş  
> **Öğretim Üyesi:** Dr. Ayşe Gül Eker  
> **Teslim Tarihi:** 13 Mayıs 2026  
> **GitHub:** https://github.com/ceylinatli/spotify-bigdata-project

---

## 👥 Grup Üyeleri

| İsim | Numara | Görev |
|------|--------|-------|
| Sabri Eren Yeşildağ | 220201013 | Docker kurulumu + Kafka Producer/Consumer |
| Kezban Ceylin Atlı | 220201047 | Spark Structured Streaming + Delta Lake |
| Zehra Karabektaş | 220201121 | EDA + Feature Engineering |
| Tolga Ferhan Küçük | 220201009 | ML Modelleri + MLflow + Dashboard |

---

## 📌 Proje Amacı

Spotify Tracks Dataset (114.000 şarkı, 113 müzik türü) üzerinde Docker, Kafka, Spark, Delta Lake ve MLflow teknolojilerini kullanarak uçtan uca bir büyük veri pipeline'ı kurmak ve `track_genre` (müzik türü) sınıflandırması yapmak.

---

## 🗄 Veri Seti

**Spotify Tracks Dataset**  
📥 Kaggle'dan indirin: https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset

İndirdikten sonra şuraya kaydedin:
```
data/raw/dataset.csv
```

> **Not:** `dataset.csv` dosyası büyük olduğu için GitHub'a yüklenmemiştir. `.gitignore` ile hariç tutulmuştur.

**Veri Seti Özellikleri:**
- 114.000 şarkı, 21 kolon (orijinal CSV: 20 müzik özelliği + 1 isimsiz index kolonu) — Kafka sonrası 23 alan (+ `kafka_timestamp`, `user_id`, `event_type`)
- 113 farklı müzik türü (`track_genre`)
- Sayısal özellikler: `popularity`, `duration_ms`, `danceability`, `energy`, `loudness`, `tempo`, `speechiness`, `acousticness`, `instrumentalness`, `liveness`, `valence`, `key`, `mode`, `time_signature`
- Kategorik/hedef alanlar: `track_genre`, `explicit`
- Kafka ek alanları: `kafka_timestamp`, `user_id`, `event_type`

---

## 🏗 Proje Mimarisi

```
dataset.csv
    ↓
Kafka Producer (500 msg/sn)
    ↓
Kafka Topic: spotify-tracks
    ↓
Spark Structured Streaming (awaitTermination=480sn, trigger=30sn)
    ↓
Delta Lake (delta-lake-data Docker volume)
    ├── Bronze  → 114.000 satır  (ham veri + kafka_timestamp)
    ├── Silver  →  89.740 satır  (temizlenmiş veri)
    └── Gold    →     113 tür    (özet istatistikler)
              ↓
         Gold/features (Feature Engineering çıktısı — 89.740 satır, 28 kolon)
    ↓
EDA + Feature Engineering
    ↓
ML Modelleri + MLflow (http://localhost:5000)
    ↓
Dashboard (11 PNG görsel → dashboard/ klasörü)
```

---

## 🛠 Kullanılan Teknolojiler

| Teknoloji | Versiyon | Kullanım Amacı |
|-----------|----------|----------------|
| Docker + Compose | latest | Tüm servislerin konteynerizasyonu |
| Apache Kafka | Confluent 7.5.0 | Gerçek zamanlı veri akışı simülasyonu |
| Apache Zookeeper | Confluent 7.5.0 | Kafka koordinasyon servisi |
| Apache Spark | 3.5.0 | Structured Streaming + MLlib |
| Delta Lake | `delta-spark_2.12:3.2.0` JAR | Bronze/Silver/Gold veri depolama |
| MLflow | 2.11.0 | Deney takibi, model loglama |
| PySpark MLlib | 3.5.0 | 5 sınıflandırma modeli |
| JupyterLab | pyspark-notebook:spark-3.5.0 | Notebook ortamı |
| Python | 3.10 | Geliştirme dili |
| Matplotlib | 3.7.1 | Görselleştirme |
| Seaborn | 0.12.2 | Görselleştirme |
| pandas | 2.1.4 | Veri işleme |
| NumPy | 1.24.3 | Sayısal hesaplama |
| scikit-learn | 1.3.0 | Metrik hesaplama |
| kafka-python | 2.0.2 | Kafka Producer/Consumer |

---

## 📁 Klasör Yapısı

```
spotify-bigdata-project/
│
├── kafka/
│   ├── Dockerfile                   # Kafka Broker Dockerfile (Confluent imajı)
│   └── producer/
│       ├── Dockerfile               # Producer/Consumer Python konteyneri
│       ├── producer.py              # CSV → Kafka topic (500 msg/sn)
│       ├── consumer.py              # Test amaçlı Kafka dinleyici
│       └── requirements.txt         # kafka-python, pandas, python-dotenv
│
├── spark/
│   ├── Dockerfile                   # jupyter/pyspark-notebook:spark-3.5.0 tabanlı
│   └── notebooks/
│       ├── adim3_spark_delta.ipynb  # Kafka → Delta Lake (Bronze/Silver/Gold)
│       ├── adim4_eda.ipynb          # Keşifsel Veri Analizi
│       ├── adim5_feature_engineering.ipynb  # 5 yeni özellik üretimi
│       ├── adim6_ml_mlflow.ipynb    # 5 ML modeli + MLflow entegrasyonu
│       └── adim7_dashboard.ipynb    # 11 görsel üretimi
│
├── mlflow/
│   └── Dockerfile                   # python:3.10-slim + mlflow==2.11.0
│
├── dashboard/
│   ├── data/                        # adim6 tarafından üretilen ara CSV/JSON dosyaları
│   │   ├── model_metrics.csv
│   │   ├── rf_feature_importance.csv
│   │   ├── gbt_feature_importance.csv
│   │   ├── roc_curve_points.csv
│   │   ├── best_confusion_matrix.csv
│   │   ├── best_model_predictions.csv
│   │   ├── best_model_metadata.json
│   │   └── label_mapping.csv
│   ├── gorsel1_model_karsilastirma.png
│   ├── gorsel2_feature_importance.png
│   ├── gorsel3_confusion_matrix.png
│   ├── gorsel4_roc_curve.png
│   ├── gorsel5_zaman_serisi.png
│   ├── gorsel6_popularity_histogram.png
│   ├── gorsel7_tur_dagilimi_pie.png
│   ├── gorsel8_gercek_vs_tahmin.png
│   ├── gorsel9_scatter.png
│   ├── gorsel10_tur_energy.png
│   └── gorsel11_korelasyon.png
│
├── data/
│   └── raw/
│       └── dataset.csv              # Kaggle'dan indirilecek (git'e dahil değil)
│
├── docs/
│   └── screenshots/                 # Docker ve servis ekran görüntüleri
│   │   ├── docker-compose-up-build.png
│   │   ├── docker-desktop.png
│   │   ├── docker-ps.png
│   │   ├── jupyterlab.png
│   │   ├── kafka-logs.png
│   │   └── kafka-producer-logs.png
│   ├── teknik_rapor_spotify.pdf     # Proje teknik raporu
│   └── spotify_sunum.pptx           # Proje sunumu
│
├── docker-compose.yml               # 6 servis tanımı
├── .gitignore
└── README.md
```

> **Not:** `delta-lake/`, `mlruns/`, `ml_artifacts/` klasörleri Docker volume ve `.gitignore` ile yönetilir, git'e dahil değildir.

---

## 🐳 Docker Servisleri ve Portları

| Servis | Konteyner Adı | Port | Açıklama |
|--------|--------------|------|----------|
| Zookeeper | zookeeper | 2181 | Kafka koordinasyon servisi |
| Kafka Broker | kafka | 9092 (host), 29092 (iç) | Mesaj kuyruğu |
| Spark Master (JupyterLab) | spark-master | 8888 | Ana notebook ortamı |
| Spark Worker (JupyterLab) | spark-worker | 8889 | İkinci Jupyter instance |
| Kafka Producer | kafka-producer | — | CSV → Kafka (500 msg/sn) |
| MLflow | mlflow | 5000 | Deney takip arayüzü |

**Docker Network:** `spotify-net` (bridge)  
**Docker Volumes:**
- `delta-lake-data` → `/home/jovyan/delta-lake` (Bronze/Silver/Gold verileri)
- `mlflow-data` → `/mlflow` (MLflow deney kayıtları)
- `./dashboard` → `/home/jovyan/dashboard` (PNG çıktıları)
- `./mlruns` → `/home/jovyan/mlruns` (Notebook içi MLflow yedek)
- `./ml_artifacts` → `/home/jovyan/ml_artifacts` (Model ara çıktıları)
- `./data` → `/home/jovyan/data` (Ham CSV verisi)
- `./spark/notebooks` → `/home/jovyan/notebooks` (Notebook dosyaları)

---

## 🚀 Kurulum ve Çalıştırma

### 1. Repoyu Klonla

```bash
git clone https://github.com/ceylinatli/spotify-bigdata-project.git
cd spotify-bigdata-project
```

### 2. Veri Setini İndir

Kaggle'dan `dataset.csv` dosyasını indirip şuraya koy:

```
data/raw/dataset.csv
```

### 3. Docker Servislerini Başlat

```bash
docker-compose up --build -d
```

İlk build yaklaşık 5-10 dakika sürebilir (JAR paketleri ve pip bağımlılıkları indirilir).

### 4. Servislerin Durumunu Kontrol Et

```bash
docker ps
```

Tüm servisler `Up` görünmeli:

```
zookeeper      ✅ Up
kafka          ✅ Up
spark-master   ✅ Up
spark-worker   ✅ Up
kafka-producer ✅ Up (simülasyon bitince Exited (0) olması normaldir)
mlflow         ✅ Up
```

### 5. Kafka Producer Loglarını Kontrol Et

```bash
docker logs kafka-producer
```

Şuna benzer çıktı görünmeli:
```
Producer başlatılıyor, 5 saniye bekleniyor...
[...] Kafka Broker'a bağlanılıyor: kafka:29092
[...] 500 mesaj gönderildi...
[...] 1000 mesaj gönderildi...
[...] Simülasyon tamamlandı. Toplam 114000 mesaj gönderildi.
```

### 6. JupyterLab'ı Aç

Tarayıcıda aç: **http://localhost:8888**

`notebooks/` klasörüne gir ve notebook'ları sırayla çalıştır:

| Sıra | Notebook | Açıklama | Süre |
|------|----------|----------|------|
| 1 | `adim3_spark_delta.ipynb` | Kafka → Delta Lake Bronze/Silver/Gold | ~8 dk |
| 2 | `adim4_eda.ipynb` | Keşifsel Veri Analizi | ~5 dk |
| 3 | `adim5_feature_engineering.ipynb` | 5 yeni özellik üretimi | ~3 dk |
| 4 | `adim6_ml_mlflow.ipynb` | 5 ML modeli eğitimi + MLflow | ~15-20 dk |
| 5 | `adim7_dashboard.ipynb` | 11 görsel üretimi | ~5 dk |

### 7. MLflow Arayüzünü Aç

Tarayıcıda aç: **http://localhost:5000**

Tüm model deneyleri burada listelenmiş olmalı.

---

## 📊 Delta Lake Sonuçları

| Katman | Yol (Docker Volume) | Satır Sayısı | Açıklama |
|--------|-------------------|-------------|----------|
| 🥉 Bronze | `/home/jovyan/delta-lake/bronze` | 114.000 | Ham veri + `kafka_timestamp` kolonu |
| 🥈 Silver | `/home/jovyan/delta-lake/silver` | 89.740 | Temizlenmiş veri |
| 🥇 Gold | `/home/jovyan/delta-lake/gold` | 113 tür | Tür bazında özet istatistikler |
| 🥇 Gold/features | `/home/jovyan/delta-lake/gold/features` | 89.740 (28 kolon) | Feature Engineering çıktısı |

**Temizlenen kayıt:** 24.260  
**Temizleme aşamaları:**
1. `dropna()` — tüm kolonlar → 113.999 satır (1 null silindi)
2. `dropDuplicates(["track_id"])` → 89.740 satır (24.259 duplike silindi)
3. Popularity filtresi (0-100) → 89.740 satır (değişmedi)
4. `explicit` → `"True"/"False"` → `1/0` (format düzeltmesi)

---

## 📊 EDA Bulguları

- **Popularity dağılımı:** Ortalama 33.2, sola yığılmış dağılım
- **Energy–Loudness korelasyonu:** r=0.76 (güçlü pozitif)
- **Acousticness–Energy korelasyonu:** r=-0.72 (güçlü negatif) → `energy_acoustic_contrast` özelliğine gerekçe
- **Hip-hop:** `speechiness` değeriyle tüm türlerden belirgin ayrışma
- **Tür dengesizliği:** Reggaeton (72), indie (134) az temsil edilen türler
- **Tempo:** 120-130 BPM civarında bimodal dağılım
- **Kafka timestamp:** Tüm veri tek oturumda aktarıldı, anlamlı zaman serisi elde edilemedi

---

## 🛠 Feature Engineering — Üretilen 5 Özellik

| Özellik | Formül | Amaç |
|---------|--------|------|
| `energy_danceability_ratio` | `energy / (danceability + 0.0001)` | Enerjik ama az dans edilebilir şarkıları ayırt eder. En yüksek: sleep türü |
| `loudness_normalized` | `(loudness - min) / (max - min)` | -49.53/+4.53 dB → 0-1 normalizasyon. Ortalama: 0.759 |
| `tempo_category` | yavaş/orta/hızlı (<100 / 100-140 / >140 BPM) | Tempo sayısalını kategoriye çevirir |
| `energy_acoustic_contrast` | `energy - acousticness` | Yüksek değer: metal/rock/grindcore. Düşük değer: sleep/piano/classical. RF önem sıralamasında orta-yüksek katkı sağlar |
| `dancefloor_score` | `(danceability + valence) / 2` | Dans edilebilirlik ve pozitif duygu tonunu birleştirir. En yüksek: children/kids. En düşük: sleep, show-tunes, romance |

**Gold/features katmanına yazılan:** 89.740 satır, 28 kolon

---

## 🤖 ML Modelleri ve Sonuçlar

### Eğitilen Modeller

| Model | Accuracy | F1-Score | AUC-ROC | Durum |
|-------|----------|----------|---------|-------|
| Logistic Regression | 0.466 | 0.449 | 0.920 | ✅ |
| Decision Tree Classifier | 0.481 | 0.480 | 0.899 | ✅ |
| **Random Forest Classifier** | **0.560** | **0.547** | **0.944** | ✅ **EN İYİ** |
| GBT Classifier | 0.506 | 0.503 | 0.928 | ✅ |
| Naive Bayes | 0.299 | 0.256 | 0.855 | ✅ |

### Feature Importance (Random Forest)

| Sıra | Özellik | Önem Skoru |
|------|---------|------------|
| 1 | popularity | 0.2417 |
| 2 | instrumentalness | 0.0925 |
| 3 | danceability | 0.0909 |
| 4 | acousticness | 0.0899 |
| 5 | speechiness | 0.0875 |
| 6 | dancefloor_score | 0.0873 |
| 7 | energy_acoustic_contrast | 0.0800 |
| 8 | key | 0.0049 |
| 9 | mode | 0.0033 |
| 10 | time_signature | 0.0027 |

### En Çok Karışan Türler (Confusion Matrix)

- breakbeat → chicago-house
- sleep → iranian
- idm → study
- malay → cantopop
- heavy-metal → black-metal

### MLflow Entegrasyonu

Her model için ayrı MLflow deneyi oluşturulmuştur:
- `Kisi4_Logistic_Regression`
- `Kisi4_Decision_Tree_Classifier`
- `Kisi4_Random_Forest_Classifier`
- `Kisi4_GBT_Classifier`
- `Kisi4_Naive_Bayes`

Loglanan bilgiler: `log_param()`, `log_metric()`, `log_artifact()`, `mlflow.spark.log_model()`

---

## 📊 Dashboard Görselleri (11 PNG)

| Görsel | Dosya | Açıklama |
|--------|-------|----------|
| 1 | `gorsel1_model_karsilastirma.png` | 5 modelin Accuracy/F1/AUC karşılaştırması |
| 2 | `gorsel2_feature_importance.png` | Random Forest özellik önem sıralaması |
| 3 | `gorsel3_confusion_matrix.png` | En iyi modelin karışıklık matrisi |
| 4 | `gorsel4_roc_curve.png` | 5 modelin ROC eğrisi karşılaştırması |
| 5 | `gorsel5_zaman_serisi.png` | Kafka timestamp saatlik veri akışı |
| 6 | `gorsel6_popularity_histogram.png` | Şarkı popülerlik dağılımı |
| 7 | `gorsel7_tur_dagilimi_pie.png` | En popüler 10 müzik türü pasta grafiği |
| 8 | `gorsel8_gercek_vs_tahmin.png` | Gerçek vs tahmin edilen türler |
| 9 | `gorsel9_scatter.png` | Popularity vs Danceability scatter plot |
| 10 | `gorsel10_tur_energy.png` | Türlere göre ortalama energy |
| 11 | `gorsel11_korelasyon.png` | Tüm sayısal özellikler korelasyon heatmap |

---

## 🔧 Karşılaşılan Zorluklar ve Çözümler

| Zorluk | Çözüm |
|--------|-------|
| Kafka ve Delta Lake JAR paketleri eksikti | `spark.jars.packages` ile spark-sql-kafka ve `delta-spark_2.12:3.2.0` JAR eklendi |
| `pip install delta-spark` ile JAR çakışıyordu | pip paketi Dockerfile'dan kaldırıldı, yalnızca JAR kullanıldı |
| Docker volume izin hatası (root/jovyan) | Dockerfile'a `mkdir` ve `chown` komutları eklendi |
| ENV satırı çok satıra bölününce `NullPointerException` | ENV tek satır olarak yazıldı |
| `explicit` kolonu `Boolean` yerine `String` geldi | `when("True", 1).when("False", 0)` ile dönüştürüldü |
| Spark bellek hatası (113 tür, yüksek bellek) | Model parametreleri hafifletildi, örnekleme yapıldı |
| Delta Lake `timestamp` kolon adı uyumsuzluğu | `kafka_timestamp` ve `timestamp` kolonlarının ikisi de desteklendi |
| MLflow artifact kaydında `/mlflow` izin hatası | MLflow Dockerfile'a `--serve-artifacts` ve `--artifacts-destination /mlflow/artifacts` eklendi |

---

## 📝 Önemli Notlar

- `data/raw/dataset.csv` GitHub'a yüklenmemiştir, Kaggle'dan indirilmelidir
- `delta-lake/`, `mlruns/`, `ml_artifacts/` klasörleri `.gitignore` ile hariç tutulmuştur
- Delta Lake verileri `delta-lake-data` adlı kalıcı Docker volume'da saklanır
- Notebook'lar **mutlaka sırayla** çalıştırılmalıdır (adim3 → adim4 → adim5 → adim6 → adim7)
- adim6 çalıştırılmadan adim7 çalışmaz (adim6 CSV çıktılarını üretir)
- Docker tüm bağımlılıkları otomatik yükler, manuel kurulum gerekmez
- MLflow arayüzüne **http://localhost:5000** adresinden erişilebilir
- JupyterLab'a **http://localhost:8888** adresinden şifresiz erişilebilir

---

## 📄 Proje Dokümanları

| Dosya | Açıklama |
|-------|----------|
| `docs/teknik_rapor.pdf` | 8 bölümlü teknik rapor |
| `docs/spotify_sunum.pptx` | 13 slaytlık proje sunumu |
| `docs/screenshots/` | Docker ve servis ekran görüntüleri |

---

*Büyük Veri Analizine Giriş — Dönem Projesi | Mayıs 2026 | Dr. Ayşe Gül Eker*
