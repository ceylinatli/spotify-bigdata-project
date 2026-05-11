# ─────────────────────────────────────────────────────────────
#  Spark + Jupyter Dockerfile
#  Temel imaj: jupyter/pyspark-notebook (Spark 3.5 + PySpark dahil)
# ─────────────────────────────────────────────────────────────

FROM jupyter/pyspark-notebook:spark-3.5.0

# Root yetkisiyle çalış
USER root

# Tüm gerekli paketleri açıkça ekliyoruz (varsayıma dayanmıyoruz)
RUN pip install --no-cache-dir --quiet \
    delta-spark==3.2.0 \
    mlflow==2.11.0 \
    kafka-python==2.0.2 \
    matplotlib==3.7.1 \
    seaborn==0.12.2 \
    pandas==2.1.4 \
    numpy==1.24.3 \
    findspark==2.0.1 \
    scikit-learn==1.3.0

# Temel imajdaki bozuk Jupyter lab eklentileri ve arşivleri için düzeltme
RUN find /opt/conda/share/jupyter \
    -type f -size 0c -delete || true

# Notebook çalışma dizinini oluştur
RUN mkdir -p /home/jovyan/notebooks && \
    chown -R jovyan:users /home/jovyan/notebooks

# Jovyan kullanıcısına geri dön
USER jovyan

WORKDIR /home/jovyan