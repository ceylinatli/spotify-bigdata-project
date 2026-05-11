FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

ENV PYSPARK_SUBMIT_ARGS="--packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
io.delta:delta-core_2.12:2.4.0 \
pyspark-shell"

RUN pip install --no-cache-dir --quiet \
    delta-spark==3.2.0 \
    mlflow==2.11.0 \
    kafka-python==2.0.2 \
    matplotlib==3.7.1 \
    seaborn==0.12.2 \
    pandas==2.1.4 \
    numpy==1.24.3 \
    scikit-learn==1.3.0

RUN find /opt/conda/share/jupyter \
    -type f -size 0c -delete || true

RUN mkdir -p /home/jovyan/notebooks \
             /home/jovyan/delta-lake/bronze \
             /home/jovyan/delta-lake/silver \
             /home/jovyan/delta-lake/gold \
             /home/jovyan/delta-lake/checkpoints && \
    chown -R jovyan:users /home/jovyan/notebooks && \
    chown -R jovyan:users /home/jovyan/delta-lake

USER jovyan
WORKDIR /home/jovyan