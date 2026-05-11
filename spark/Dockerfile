#  Spark + Jupyter Dockerfile
#  Temel imaj: jupyter/pyspark-notebook (Spark 3.5 + PySpark dahil)

FROM jupyter/pyspark-notebook:spark-3.5.0

# Root yetkisiyle çalış
USER root

# Sadece eksik olan paketleri ekle
# (matplotlib, seaborn, scikit-learn imajda zaten var — kontrol edilmeden ekleme)
RUN pip install --no-cache-dir --quiet \
    delta-spark==3.2.0 \
    mlflow==2.11.0 \
    kafka-python==2.0.2 \
    findspark==2.0.1

# Temel imajdaki bozuk Jupyter lab eklentileri ve arşivleri için düzeltme
RUN find /opt/conda/share/jupyter -type f -size 0c -delete || true

# Notebook çalışma dizinini oluştur
RUN mkdir -p /home/jovyan/notebooks && \
    chown -R jovyan:users /home/jovyan/notebooks

# Jovyan kullanıcısına geri dön
USER jovyan

WORKDIR /home/jovyan