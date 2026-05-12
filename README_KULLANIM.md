# Spotify Big Data Projesi - MLflow ve Dashboard

Bu yönergeler proje kök klasörü için hazırlanmıştır. Notebooklar `spark/notebooks`, dashboard çıktıları `dashboard`, yardımcı scriptler ise `tools` klasörü altındadır.

## 1. Proje Klasörüne Geç

```powershell
cd C:\Users\HP\Documents\GitHub\spotify-bigdata-project
```

## 2. Sanal Ortam Kur

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

PowerShell izin hatası verirse bir kez şunu çalıştır:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

## 3. VS Code İçin Kernel Ekle

```powershell
python -m ipykernel install --user --name spotify-bigdata --display-name "Spotify BigData"
```

Notebook açınca kernel olarak `Spotify BigData` seç.

## 4. Delta Lake Veri Yolu

Notebooklar Delta tablosunu otomatik bulmaya çalışır. İlk beklenen yol:

```text
delta-lake\gold\features
```

Yedek olarak şu yollar da denenir:

```text
data\delta\spotify_tracks
delta\spotify_tracks
spark\data\delta\spotify_tracks
```

Delta tablon farklı yerdeyse notebook başındaki `DELTA_TABLE_PATH` değişkenini kendi klasör yoluna göre düzenle.

## 5. Notebook Çalıştırma Sırası

Önce ML notebookunu çalıştır:

```text
spark\notebooks\adim6_ml_mlflow.ipynb
```

Bu notebook 5 modeli eğitir, metrikleri hesaplar, MLflow'a loglar ve dashboard için ara CSV dosyalarını `dashboard\data` altına üretir.

Sonra dashboard notebookunu çalıştır:

```text
spark\notebooks\adim7_dashboard.ipynb
```

Bu notebook 11 görseli `dashboard` klasörüne PNG olarak kaydeder.

## 6. MLflow UI Açma

ML notebooku bittikten sonra yeni bir PowerShell terminalinde:

```powershell
cd C:\Users\HP\Documents\GitHub\spotify-bigdata-project
.\.venv\Scripts\Activate.ps1
mlflow ui --backend-store-uri .\mlruns --host 127.0.0.1 --port 5000
```

Tarayıcıda `http://127.0.0.1:5000` adresini aç.

## 7. Commit Edilecek Dosyalar

```text
README_KULLANIM.md
requirements.txt
spark/notebooks/adim6_ml_mlflow.ipynb
spark/notebooks/adim7_dashboard.ipynb
tools/create_notebooks.py
```

Notebooklar çalıştırıldıktan sonra oluşan görseller:

```text
dashboard/gorsel1_model_karsilastirma.png
dashboard/gorsel2_feature_importance.png
dashboard/gorsel3_confusion_matrix.png
dashboard/gorsel4_roc_curve.png
dashboard/gorsel5_zaman_serisi.png
dashboard/gorsel6_popularity_histogram.png
dashboard/gorsel7_tur_dagilimi_pie.png
dashboard/gorsel8_gercek_vs_tahmin.png
dashboard/gorsel9_scatter.png
dashboard/gorsel10_tur_energy.png
dashboard/gorsel11_korelasyon.png
```
