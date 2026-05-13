# MLflow sunucusu için Dockerfile
FROM python:3.10-slim

# MLflow paketini yüklüyoruz
RUN pip install --default-timeout=1000 --retries 5 mlflow==2.11.0

# MLflow varsayılan portu
EXPOSE 5000

# MLflow sunucusunu başlatıyoruz
CMD ["mlflow", "server", \
     "--host", "0.0.0.0", \
     "--port", "5000", \
     "--backend-store-uri", "/mlflow/mlruns", \
     "--serve-artifacts", \
     "--artifacts-destination", "/mlflow/artifacts"]
