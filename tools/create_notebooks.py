from __future__ import annotations

import json
import textwrap
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = BASE_DIR / "spark" / "notebooks"


def _source(text: str) -> list[str]:
    cleaned = textwrap.dedent(text).strip("\n") + "\n"
    return cleaned.splitlines(keepends=True)


def markdown(text: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": _source(text),
    }


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _source(text),
    }


def notebook(cells: list[dict]) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Spotify BigData",
                "language": "python",
                "name": "spotify-bigdata",
            },
            "language_info": {
                "name": "python",
                "pygments_lexer": "ipython3",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


ADIM6_CELLS = [
    markdown(
        """
        # Adım 6 - PySpark MLlib + MLflow

        Bu notebook `track_genre` tahmini için 5 sınıflandırma modeli eğitir:

        - Logistic Regression
        - Decision Tree Classifier
        - Random Forest Classifier
        - Gradient Boosted Trees (GBT) Classifier
        - Naive Bayes

        Her model için Accuracy, weighted F1, weighted Precision, weighted Recall, AUC-ROC ve Confusion Matrix hesaplanır. Sonuçlar MLflow'a kaydedilir.
        """
    ),
    markdown(
        """
        ## Windows Kullanım Notu

        Bu dosya proje kök klasörü altında çalışacak şekilde hazırlandı. VS Code içinde kernel olarak `Spotify BigData` seç.

        MLflow UI için notebook bittikten sonra PowerShell'de:

        ```powershell
        cd C:\\Users\\HP\\Documents\\GitHub\\spotify-bigdata-project
        .\\.venv\\Scripts\\Activate.ps1
        mlflow ui --backend-store-uri .\\mlruns --host 127.0.0.1 --port 5000
        ```

        Sonra tarayıcıda `http://127.0.0.1:5000` adresini aç.
        """
    ),
    code(
        """
        # Temel kütüphaneler ve proje klasörleri
        from pathlib import Path
        import json
        import math
        import os
        import shutil
        import warnings

        import numpy as np
        import pandas as pd

        warnings.filterwarnings("ignore")

        def find_base_dir() -> Path:
            # Notebook farklı klasörden çalıştırılsa bile proje kökünü buluyoruz.
            current = Path.cwd().resolve()
            for candidate in [current, *current.parents]:
                if (candidate / ".git").exists() or (candidate / "docker-compose.yml").exists():
                    return candidate
            if current.name.lower() == "notebooks":
                return current.parent
            return current

        BASE_DIR = find_base_dir()
        PROJECT_ROOT = BASE_DIR
        DASHBOARD_DIR = BASE_DIR / "dashboard"
        DASHBOARD_DATA_DIR = DASHBOARD_DIR / "data"
        RUN_ARTIFACT_DIR = BASE_DIR / "ml_artifacts"
        MLFLOW_TRACKING_DIR = BASE_DIR / "mlruns"

        for folder in [DASHBOARD_DIR, DASHBOARD_DATA_DIR, RUN_ARTIFACT_DIR, MLFLOW_TRACKING_DIR]:
            folder.mkdir(parents=True, exist_ok=True)

        # ML için önce Kişi 3'ün Feature Engineering çıktısı aranır.
        # Kendi veri yolun farklıysa DELTA_TABLE_PATH'i elle değiştirebilirsin.
        DELTA_CANDIDATES = [
            PROJECT_ROOT / "delta-lake" / "gold" / "features",
            BASE_DIR / "delta-lake" / "gold" / "features",
            PROJECT_ROOT / "delta-lake" / "silver",
            BASE_DIR / "delta-lake" / "silver",
            BASE_DIR / "data" / "delta" / "spotify_tracks",
            BASE_DIR / "delta" / "spotify_tracks",
            BASE_DIR / "spark" / "data" / "delta" / "spotify_tracks",
            PROJECT_ROOT / "data" / "delta" / "spotify_tracks",
            PROJECT_ROOT / "spark" / "data" / "delta" / "spotify_tracks",
            PROJECT_ROOT / "delta" / "spotify_tracks",
        ]

        DELTA_TABLE_PATH = next(
            (path for path in DELTA_CANDIDATES if (path / "_delta_log").exists()),
            DELTA_CANDIDATES[0],
        )

        print(f"Proje kökü: {BASE_DIR}")
        print(f"Delta tablo yolu: {DELTA_TABLE_PATH}")
        print(f"MLflow kayıt klasörü: {MLFLOW_TRACKING_DIR}")
        """
    ),
    code(
        """
        # Spark ve Delta Lake oturumu
        from pyspark.sql import SparkSession, Window, functions as F

        try:
            from delta import configure_spark_with_delta_pip
        except ModuleNotFoundError:
            configure_spark_with_delta_pip = None

        spark_builder = (
            SparkSession.builder
            .appName("SpotifyTrackGenreMLflow")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.warehouse.dir", str(BASE_DIR / "spark-warehouse"))
        )

        if configure_spark_with_delta_pip is not None:
            spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
        else:
            spark = spark_builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        print("Spark sürümü:", spark.version)
        """
    ),
    code(
        """
        # Delta Lake tablosunu oku
        if not (DELTA_TABLE_PATH / "_delta_log").exists():
            raise FileNotFoundError(
                "Delta tablosu bulunamadı. DELTA_TABLE_PATH değişkenini kendi Delta klasörüne göre düzenle. "
                "Zaman serisi grafiği için bu tabloda Kafka Producer tarafından eklenen timestamp kolonu olmalı."
            )

        df_raw = spark.read.format("delta").load(str(DELTA_TABLE_PATH))

        print("Ham satır sayısı:", df_raw.count())
        df_raw.printSchema()
        display(df_raw.limit(5).toPandas())
        """
    ),
    code(
        """
        # Feature Engineering çıktısı beklenen kolonlar
        label_col = "track_genre"
        feature_columns = [
            "danceability",
            "energy",
            "loudness_normalized",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo_category",
            "energy_acoustic_contrast",
            "dancefloor_score",
            "popularity",
            "key",
            "mode",
            "time_signature",
        ]
        numeric_features = [col for col in feature_columns if col != "tempo_category"]
        categorical_features = ["tempo_category"]

        df = df_raw

        # Önceki adımlarda bazı feature kolonları oluşmadıysa temel yedekleri üretiyoruz.
        # Asıl projede bu kolonların Feature Engineering adımından gelmesi beklenir.
        if "loudness_normalized" not in df.columns and "loudness" in df.columns:
            bounds = df.agg(
                F.min("loudness").alias("min_loudness"),
                F.max("loudness").alias("max_loudness"),
            ).first()
            min_loudness = float(bounds["min_loudness"] or 0.0)
            max_loudness = float(bounds["max_loudness"] or 1.0)
            denom = max(max_loudness - min_loudness, 1e-9)
            df = df.withColumn(
                "loudness_normalized",
                (F.col("loudness").cast("double") - F.lit(min_loudness)) / F.lit(denom),
            )

        if "tempo_category" not in df.columns and "tempo" in df.columns:
            df = df.withColumn(
                "tempo_category",
                F.when(F.col("tempo").cast("double") < 90, F.lit("yavas"))
                .when(F.col("tempo").cast("double") <= 130, F.lit("orta"))
                .otherwise(F.lit("hizli")),
            )

        if "energy_acoustic_contrast" not in df.columns:
            df = df.withColumn(
                "energy_acoustic_contrast",
                F.col("energy").cast("double") - F.col("acousticness").cast("double"),
            )

        if "dancefloor_score" not in df.columns:
            df = df.withColumn(
                "dancefloor_score",
                (
                    F.col("danceability").cast("double")
                    + F.col("valence").cast("double")
                ) / F.lit(2.0),
            )

        required_cols = [label_col] + feature_columns
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Eksik kolonlar var: {missing_cols}")

        # Modelleme için tipleri netleştiriyoruz.
        for col_name in numeric_features:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))

        df_model = (
            df
            .withColumn(label_col, F.col(label_col).cast("string"))
            .withColumn("tempo_category", F.coalesce(F.col("tempo_category").cast("string"), F.lit("bilinmiyor")))
            .select(label_col, *feature_columns)
            .dropna(subset=[label_col])
        )

        # Yerel Docker ortamında GBT OneVsRest, 100+ türde belleği zorlayabilir.
        # Bu yüzden ML deneyini en sık görülen türlerle ve tür başına dengeli örnekle sınırlıyoruz.
        max_genres_for_ml = 25
        max_rows_per_genre = 500
        top_genres = [
            row[label_col]
            for row in (
                df_model
                .groupBy(label_col)
                .count()
                .orderBy(F.desc("count"))
                .limit(max_genres_for_ml)
                .collect()
            )
        ]
        df_model = (
            df_model
            .where(F.col(label_col).isin(top_genres))
            .withColumn("_genre_row_number", F.row_number().over(Window.partitionBy(label_col).orderBy(F.rand(seed=42))))
            .where(F.col("_genre_row_number") <= max_rows_per_genre)
            .drop("_genre_row_number")
            .cache()
        )

        print("Modelleme satır sayısı:", df_model.count())
        print(f"Modelleme tür sayısı: {len(top_genres)}")
        print(f"Tür başına maksimum örnek: {max_rows_per_genre}")
        display(df_model.limit(5).toPandas())
        """
    ),
    code(
        """
        # Label encoding ve train/test ayrımı
        from pyspark.ml.feature import StringIndexer

        label_indexer = StringIndexer(inputCol=label_col, outputCol="label", handleInvalid="skip")
        label_model = label_indexer.fit(df_model)
        df_labeled = label_model.transform(df_model)

        label_names = list(label_model.labels)
        num_classes = len(label_names)

        if num_classes < 2:
            raise ValueError("Sınıflandırma için en az 2 farklı track_genre gerekir.")

        train_df, test_df = df_labeled.randomSplit([0.8, 0.2], seed=42)
        train_count = train_df.count()
        test_count = test_df.count()

        label_mapping = pd.DataFrame({
            "label_index": list(range(num_classes)),
            "track_genre": label_names,
        })
        label_mapping.to_csv(DASHBOARD_DATA_DIR / "label_mapping.csv", index=False, encoding="utf-8-sig")

        print(f"Sınıf sayısı: {num_classes}")
        print(f"Train satır sayısı: {train_count}")
        print(f"Test satır sayısı: {test_count}")
        display(label_mapping.head(10))
        """
    ),
    code(
        """
        # Preprocessing pipeline: eksik değer doldurma, tempo kategorisini sayısallaştırma, vektörleştirme ve ölçekleme
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import Imputer, MinMaxScaler, OneHotEncoder, VectorAssembler

        numeric_imputed_features = [f"{col}_imputed" for col in numeric_features]

        def build_preprocessing_stages():
            imputer = Imputer(
                inputCols=numeric_features,
                outputCols=numeric_imputed_features,
                strategy="median",
            )

            tempo_indexer = StringIndexer(
                inputCol="tempo_category",
                outputCol="tempo_category_index",
                handleInvalid="keep",
            )

            tempo_encoder = OneHotEncoder(
                inputCols=["tempo_category_index"],
                outputCols=["tempo_category_ohe"],
                dropLast=False,
            )

            assembler_input_cols = [
                f"{col}_imputed" if col in numeric_features else "tempo_category_ohe"
                for col in feature_columns
            ]

            assembler = VectorAssembler(
                inputCols=assembler_input_cols,
                outputCol="raw_features",
                handleInvalid="keep",
            )

            # Naive Bayes negatif değer kabul etmediği için tüm modellerde ortak 0-1 ölçekleme kullanıyoruz.
            scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")

            return [imputer, tempo_indexer, tempo_encoder, assembler, scaler]
        """
    ),
    code(
        """
        # İstenen 5 model
        from pyspark.ml.classification import (
            DecisionTreeClassifier,
            GBTClassifier,
            LogisticRegression,
            NaiveBayes,
            OneVsRest,
            RandomForestClassifier,
        )

        seed = 42

        logistic_regression = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            probabilityCol="probability",
            maxIter=60,
            regParam=0.01,
            elasticNetParam=0.0,
            family="multinomial",
        )

        decision_tree = DecisionTreeClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            probabilityCol="probability",
            maxDepth=8,
            impurity="gini",
            seed=seed,
        )

        random_forest = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            probabilityCol="probability",
            numTrees=25,
            maxDepth=8,
            featureSubsetStrategy="sqrt",
            subsamplingRate=0.8,
            seed=seed,
        )

        gbt_base = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=5,
            maxDepth=3,
            stepSize=0.1,
            subsamplingRate=0.8,
            seed=seed,
        )

        # Spark GBTClassifier doğrudan binary çalışır. track_genre çok sınıflıysa OneVsRest ile çok sınıflı hale getiriyoruz.
        if num_classes > 2:
            gbt_classifier = OneVsRest(
                classifier=gbt_base,
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                parallelism=1,
            )
        else:
            gbt_classifier = gbt_base

        naive_bayes = NaiveBayes(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            probabilityCol="probability",
            smoothing=1.0,
            modelType="multinomial",
        )

        model_specs = [
            {
                "name": "Logistic Regression",
                "experiment": "Kisi4_Logistic_Regression",
                "estimator": logistic_regression,
                "params": {
                    "maxIter": 60,
                    "regParam": 0.01,
                    "elasticNetParam": 0.0,
                    "family": "multinomial",
                },
            },
            {
                "name": "Decision Tree Classifier",
                "experiment": "Kisi4_Decision_Tree_Classifier",
                "estimator": decision_tree,
                "params": {
                    "maxDepth": 8,
                    "impurity": "gini",
                    "seed": seed,
                },
            },
            {
                "name": "Random Forest Classifier",
                "experiment": "Kisi4_Random_Forest_Classifier",
                "estimator": random_forest,
                "params": {
                    "numTrees": 25,
                    "maxDepth": 8,
                    "featureSubsetStrategy": "sqrt",
                    "subsamplingRate": 0.8,
                    "seed": seed,
                },
            },
            {
                "name": "Gradient Boosted Trees (GBT) Classifier",
                "experiment": "Kisi4_GBT_Classifier",
                "estimator": gbt_classifier,
                "params": {
                    "maxIter": 5,
                    "maxDepth": 3,
                    "stepSize": 0.1,
                    "subsamplingRate": 0.8,
                    "seed": seed,
                    "multiclass_strategy": "OneVsRest" if num_classes > 2 else "Binary GBTClassifier",
                },
            },
            {
                "name": "Naive Bayes",
                "experiment": "Kisi4_Naive_Bayes",
                "estimator": naive_bayes,
                "params": {
                    "smoothing": 1.0,
                    "modelType": "multinomial",
                },
            },
        ]

        print("Eğitilecek model sayısı:", len(model_specs))
        """
    ),
    code(
        """
        # Metrik, ROC, confusion matrix ve feature importance yardımcı fonksiyonları
        from sklearn.metrics import accuracy_score, confusion_matrix, precision_recall_fscore_support, roc_auc_score, roc_curve
        from sklearn.preprocessing import label_binarize

        def safe_name(value: str) -> str:
            return (
                value.lower()
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
                .replace("-", "_")
            )

        def vector_to_array(value):
            if value is None:
                return None
            if hasattr(value, "toArray"):
                return np.asarray(value.toArray(), dtype=float)
            if isinstance(value, (list, tuple, np.ndarray)):
                return np.asarray(value, dtype=float)
            return np.asarray([float(value)], dtype=float)

        def softmax(matrix: np.ndarray) -> np.ndarray:
            matrix = np.asarray(matrix, dtype=float)
            matrix = np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
            shifted = matrix - np.max(matrix, axis=1, keepdims=True)
            exp_values = np.exp(shifted)
            denom = np.sum(exp_values, axis=1, keepdims=True)
            denom[denom == 0] = 1.0
            return exp_values / denom

        def score_matrix_from_pdf(pdf: pd.DataFrame, score_col: str | None, class_count: int):
            if score_col is None or score_col not in pdf.columns:
                return None
            arrays = [vector_to_array(value) for value in pdf[score_col]]
            arrays = [arr for arr in arrays if arr is not None]
            if not arrays:
                return None
            scores = np.vstack(arrays)
            if scores.shape[1] < class_count:
                scores = np.pad(scores, ((0, 0), (0, class_count - scores.shape[1])), mode="constant")
            if scores.shape[1] > class_count:
                scores = scores[:, :class_count]

            row_sums = scores.sum(axis=1)
            looks_like_probability = (
                np.all(scores >= -1e-9)
                and np.all(scores <= 1 + 1e-9)
                and np.allclose(row_sums, 1.0, atol=1e-3)
            )
            if not looks_like_probability:
                scores = softmax(scores)
            return scores

        def compute_auc_and_roc(y_true: np.ndarray, scores: np.ndarray | None, model_name: str, class_count: int):
            if scores is None:
                return float("nan"), pd.DataFrame({"model": [model_name, model_name], "fpr": [0.0, 1.0], "tpr": [0.0, 1.0]})

            try:
                if class_count == 2:
                    positive_scores = scores[:, 1]
                    auc_value = roc_auc_score(y_true, positive_scores)
                    fpr, tpr, _ = roc_curve(y_true, positive_scores, pos_label=1)
                else:
                    classes = np.arange(class_count)
                    present_classes = np.array(sorted(set(y_true.tolist())))
                    if len(present_classes) < 2:
                        raise ValueError("Test verisinde AUC için en az 2 sınıf görünmeli.")
                    y_bin_all = label_binarize(y_true, classes=classes)
                    y_bin = y_bin_all[:, present_classes]
                    present_scores = scores[:, present_classes]
                    auc_value = roc_auc_score(y_bin, present_scores, average="weighted")
                    fpr, tpr, _ = roc_curve(y_bin.ravel(), present_scores.ravel())
            except ValueError as error:
                print(f"{model_name} için AUC hesaplanamadı: {error}")
                auc_value = float("nan")
                fpr, tpr = np.array([0.0, 1.0]), np.array([0.0, 1.0])

            if len(fpr) > 1000:
                idx = np.linspace(0, len(fpr) - 1, 1000).astype(int)
                fpr, tpr = fpr[idx], tpr[idx]

            roc_df = pd.DataFrame({"model": model_name, "fpr": fpr, "tpr": tpr})
            return float(auc_value), roc_df

        def evaluate_predictions(predictions, model_name: str):
            score_col = None
            if "probability" in predictions.columns:
                score_col = "probability"
            elif "rawPrediction" in predictions.columns:
                score_col = "rawPrediction"

            select_cols = ["label", "prediction"]
            if score_col:
                select_cols.append(score_col)

            pred_pdf = predictions.select(*select_cols).toPandas()
            pred_pdf["label"] = pred_pdf["label"].astype(int)
            pred_pdf["prediction"] = pred_pdf["prediction"].astype(int)

            y_true = pred_pdf["label"].to_numpy()
            y_pred = pred_pdf["prediction"].to_numpy()

            precision_value, recall_value, f1_value, _ = precision_recall_fscore_support(
                y_true,
                y_pred,
                average="weighted",
                zero_division=0,
            )

            metrics = {
                "accuracy": accuracy_score(y_true, y_pred),
                "f1_weighted": f1_value,
                "precision_weighted": precision_value,
                "recall_weighted": recall_value,
            }

            scores = score_matrix_from_pdf(pred_pdf, score_col, num_classes)
            auc_value, roc_df = compute_auc_and_roc(y_true, scores, model_name, num_classes)
            metrics["auc_roc"] = auc_value

            cm = confusion_matrix(y_true, y_pred, labels=list(range(num_classes)))
            return metrics, cm, roc_df, pred_pdf

        def feature_names_from_pipeline_model(pipeline_model):
            # Pipeline sırası: Imputer, StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler, Model
            tempo_model = pipeline_model.stages[1]
            encoder_model = pipeline_model.stages[2]
            tempo_labels = list(getattr(tempo_model, "labels", []))

            try:
                tempo_size = int(encoder_model.categorySizes[0])
            except Exception:
                tempo_size = len(tempo_labels)

            while len(tempo_labels) < tempo_size:
                tempo_labels.append("bilinmeyen")

            readable_tempo = [f"tempo_category={name}" for name in tempo_labels[:tempo_size]]
            readable_features = []
            for feature_name in feature_columns:
                if feature_name == "tempo_category":
                    readable_features.extend(readable_tempo)
                else:
                    readable_features.append(feature_name)
            return readable_features

        def tree_importance_dataframe(pipeline_model, model_name: str):
            classifier_model = pipeline_model.stages[-1]

            if hasattr(classifier_model, "featureImportances"):
                values = classifier_model.featureImportances.toArray()
            elif hasattr(classifier_model, "models"):
                # OneVsRest GBT için her sınıfın binary GBT önem skorlarını ortalıyoruz.
                arrays = [
                    model.featureImportances.toArray()
                    for model in classifier_model.models
                    if hasattr(model, "featureImportances")
                ]
                if not arrays:
                    return None
                values = np.mean(np.vstack(arrays), axis=0)
            else:
                return None

            names = feature_names_from_pipeline_model(pipeline_model)
            length = min(len(names), len(values))
            importance_df = pd.DataFrame({
                "model": model_name,
                "feature": names[:length],
                "importance": values[:length],
            }).sort_values("importance", ascending=False)
            return importance_df
        """
    ),
    code(
        """
        # MLflow ile modelleri eğit, metrikleri logla ve dashboard ara dosyalarını üret
        import mlflow
        import mlflow.spark

        mlflow.set_tracking_uri(MLFLOW_TRACKING_DIR.as_uri())

        all_metrics = []
        all_roc_frames = []
        best_result = {
            "f1_weighted": -1.0,
            "model_name": None,
            "predictions": None,
            "confusion_matrix": None,
        }

        for spec in model_specs:
            model_name = spec["name"]
            print("\\n" + "=" * 80)
            print(f"Model eğitiliyor: {model_name}")

            pipeline = Pipeline(stages=build_preprocessing_stages() + [spec["estimator"]])
            mlflow.set_experiment(spec["experiment"])

            with mlflow.start_run(run_name=model_name) as run:
                # Parametreleri MLflow'a yazıyoruz.
                mlflow.log_param("model_name", model_name)
                mlflow.log_param("target_column", label_col)
                mlflow.log_param("feature_columns", ",".join(feature_columns))
                mlflow.log_param("num_classes", num_classes)
                mlflow.log_param("train_count", train_count)
                mlflow.log_param("test_count", test_count)
                mlflow.log_param("delta_table_path", str(DELTA_TABLE_PATH))
                for param_name, param_value in spec["params"].items():
                    mlflow.log_param(param_name, param_value)

                fitted_pipeline = pipeline.fit(train_df)
                predictions = fitted_pipeline.transform(test_df)

                metrics, cm, roc_df, pred_pdf = evaluate_predictions(predictions, model_name)

                # Metrikleri MLflow'a yazıyoruz.
                for metric_name, metric_value in metrics.items():
                    if metric_value is not None and not math.isnan(float(metric_value)):
                        mlflow.log_metric(metric_name, float(metric_value))

                run_dir = RUN_ARTIFACT_DIR / safe_name(model_name)
                if run_dir.exists():
                    shutil.rmtree(run_dir)
                run_dir.mkdir(parents=True, exist_ok=True)

                cm_df = pd.DataFrame(cm, index=label_names, columns=label_names)
                cm_path = run_dir / "confusion_matrix.csv"
                cm_df.to_csv(cm_path, encoding="utf-8-sig")
                mlflow.log_artifact(str(cm_path), artifact_path="evaluation")

                metrics_path = run_dir / "metrics.json"
                with metrics_path.open("w", encoding="utf-8") as file:
                    json.dump(metrics, file, ensure_ascii=False, indent=2)
                mlflow.log_artifact(str(metrics_path), artifact_path="evaluation")

                importance_df = None
                if model_name in ["Random Forest Classifier", "Gradient Boosted Trees (GBT) Classifier"]:
                    importance_df = tree_importance_dataframe(fitted_pipeline, model_name)
                    if importance_df is not None and not importance_df.empty:
                        if model_name == "Random Forest Classifier":
                            importance_path = DASHBOARD_DATA_DIR / "rf_feature_importance.csv"
                        else:
                            importance_path = DASHBOARD_DATA_DIR / "gbt_feature_importance.csv"

                        importance_df.to_csv(importance_path, index=False, encoding="utf-8-sig")
                        mlflow.log_artifact(str(importance_path), artifact_path="feature_importance")
                        mlflow.log_param("top_feature", importance_df.iloc[0]["feature"])
                        mlflow.log_metric("top_feature_importance", float(importance_df.iloc[0]["importance"]))

                        print("En önemli 10 özellik:")
                        display(importance_df.head(10))

                mlflow.spark.log_model(fitted_pipeline, artifact_path="spark_model")

                row = {
                    "model": model_name,
                    "experiment": spec["experiment"],
                    "run_id": run.info.run_id,
                    **metrics,
                }
                all_metrics.append(row)
                all_roc_frames.append(roc_df)

                print("Metrikler:")
                display(pd.DataFrame([row]))
                print("Confusion Matrix ilk 10x10 görünüm:")
                display(cm_df.iloc[:10, :10])

                if metrics["f1_weighted"] > best_result["f1_weighted"]:
                    best_result = {
                        "f1_weighted": metrics["f1_weighted"],
                        "model_name": model_name,
                        "predictions": pred_pdf.copy(),
                        "confusion_matrix": cm_df.copy(),
                    }

                predictions.unpersist()

        metrics_df = pd.DataFrame(all_metrics).sort_values("f1_weighted", ascending=False)
        metrics_df.to_csv(DASHBOARD_DATA_DIR / "model_metrics.csv", index=False, encoding="utf-8-sig")

        roc_all_df = pd.concat(all_roc_frames, ignore_index=True)
        roc_all_df.to_csv(DASHBOARD_DATA_DIR / "roc_curve_points.csv", index=False, encoding="utf-8-sig")

        best_predictions = best_result["predictions"].copy()
        best_predictions["actual_genre"] = best_predictions["label"].map(lambda idx: label_names[int(idx)])
        best_predictions["predicted_genre"] = best_predictions["prediction"].map(lambda idx: label_names[int(idx)] if int(idx) < len(label_names) else "bilinmeyen")
        best_predictions[["label", "prediction", "actual_genre", "predicted_genre"]].to_csv(
            DASHBOARD_DATA_DIR / "best_model_predictions.csv",
            index=False,
            encoding="utf-8-sig",
        )

        best_result["confusion_matrix"].to_csv(
            DASHBOARD_DATA_DIR / "best_confusion_matrix.csv",
            encoding="utf-8-sig",
        )

        best_metadata = {
            "best_model_name": best_result["model_name"],
            "selection_metric": "f1_weighted",
            "best_f1_weighted": best_result["f1_weighted"],
        }
        with (DASHBOARD_DATA_DIR / "best_model_metadata.json").open("w", encoding="utf-8") as file:
            json.dump(best_metadata, file, ensure_ascii=False, indent=2)

        print("\\nEğitim tamamlandı.")
        print("En iyi model:", best_result["model_name"])
        display(metrics_df)
        """
    ),
    markdown(
        """
        ## Bu Notebookun Ürettiği Dosyalar

        Dashboard notebooku şu dosyaları kullanır:

        - `dashboard/data/model_metrics.csv`
        - `dashboard/data/rf_feature_importance.csv`
        - `dashboard/data/roc_curve_points.csv`
        - `dashboard/data/best_confusion_matrix.csv`
        - `dashboard/data/best_model_predictions.csv`
        - `dashboard/data/best_model_metadata.json`

        MLflow kayıtları `mlruns` altında tutulur.
        """
    ),
]


ADIM7_CELLS = [
    markdown(
        """
        # Adım 7 - Dashboard ve Görselleştirme

        Bu notebook Adım 6'da üretilen model sonuçlarını ve Delta Lake tablosunu kullanarak 11 zorunlu görseli üretir.
        Tüm görseller `dashboard` klasörüne `dpi=150` ile PNG olarak kaydedilir.
        """
    ),
    code(
        """
        # Temel kütüphaneler ve proje klasörleri
        from pathlib import Path
        import json
        import warnings

        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        import seaborn as sns

        warnings.filterwarnings("ignore")
        sns.set_theme(style="whitegrid", context="notebook")

        def find_base_dir() -> Path:
            # Notebook nereden çalışırsa çalışsın proje kökünü buluyoruz.
            current = Path.cwd().resolve()
            for candidate in [current, *current.parents]:
                if (candidate / ".git").exists() or (candidate / "docker-compose.yml").exists():
                    return candidate
            if current.name.lower() == "notebooks":
                return current.parent
            return current

        BASE_DIR = find_base_dir()
        PROJECT_ROOT = BASE_DIR
        DASHBOARD_DIR = BASE_DIR / "dashboard"
        DASHBOARD_DATA_DIR = DASHBOARD_DIR / "data"
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

        # Dashboard için önce feature tablosu, sonra streaming katmanları denenir.
        # Zaman serisi grafiği için seçilen tabloda Kafka timestamp kolonu bulunmalı.
        DELTA_CANDIDATES = [
            PROJECT_ROOT / "delta-lake" / "gold" / "features",
            BASE_DIR / "delta-lake" / "gold" / "features",
            PROJECT_ROOT / "delta-lake" / "silver",
            BASE_DIR / "delta-lake" / "silver",
            PROJECT_ROOT / "delta-lake" / "bronze",
            BASE_DIR / "delta-lake" / "bronze",
            BASE_DIR / "data" / "delta" / "spotify_tracks",
            BASE_DIR / "delta" / "spotify_tracks",
            BASE_DIR / "spark" / "data" / "delta" / "spotify_tracks",
            PROJECT_ROOT / "data" / "delta" / "spotify_tracks",
            PROJECT_ROOT / "spark" / "data" / "delta" / "spotify_tracks",
            PROJECT_ROOT / "delta" / "spotify_tracks",
        ]

        DELTA_TABLE_PATH = next(
            (path for path in DELTA_CANDIDATES if (path / "_delta_log").exists()),
            DELTA_CANDIDATES[0],
        )

        print(f"Proje kökü: {BASE_DIR}")
        print(f"Dashboard klasörü: {DASHBOARD_DIR}")
        print(f"Delta tablo yolu: {DELTA_TABLE_PATH}")
        """
    ),
    code(
        """
        # Adım 6'nın ürettiği sonuç dosyalarını oku
        required_files = {
            "metrics": DASHBOARD_DATA_DIR / "model_metrics.csv",
            "rf_importance": DASHBOARD_DATA_DIR / "rf_feature_importance.csv",
            "roc": DASHBOARD_DATA_DIR / "roc_curve_points.csv",
            "confusion_matrix": DASHBOARD_DATA_DIR / "best_confusion_matrix.csv",
            "predictions": DASHBOARD_DATA_DIR / "best_model_predictions.csv",
            "metadata": DASHBOARD_DATA_DIR / "best_model_metadata.json",
        }

        missing = [str(path) for path in required_files.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(
                "Önce adim6_ml_mlflow.ipynb notebookunu baştan sona çalıştırmalısın. Eksik dosyalar: "
                + ", ".join(missing)
            )

        metrics_df = pd.read_csv(required_files["metrics"])
        rf_importance_df = pd.read_csv(required_files["rf_importance"])
        roc_df = pd.read_csv(required_files["roc"])
        confusion_df = pd.read_csv(required_files["confusion_matrix"], index_col=0)
        predictions_df = pd.read_csv(required_files["predictions"])
        with open(required_files["metadata"], "r", encoding="utf-8") as file:
            best_metadata = json.load(file)

        best_model_name = best_metadata["best_model_name"]

        display(metrics_df)
        print("En iyi model:", best_model_name)
        """
    ),
    code(
        """
        # Görsel 1: 5 modelin performans karşılaştırması
        metric_columns = ["accuracy", "f1_weighted", "auc_roc"]
        metric_labels = {
            "accuracy": "Accuracy",
            "f1_weighted": "F1-Score",
            "auc_roc": "AUC",
        }

        plot_df = metrics_df[["model", *metric_columns]].melt(
            id_vars="model",
            value_vars=metric_columns,
            var_name="Metrik",
            value_name="Skor",
        )
        plot_df["Metrik"] = plot_df["Metrik"].map(metric_labels)

        plt.figure(figsize=(13, 6))
        sns.barplot(data=plot_df, x="model", y="Skor", hue="Metrik", palette="Set2")
        plt.title("5 Modelin Performans Karşılaştırması")
        plt.xlabel("Model")
        plt.ylabel("Skor")
        plt.ylim(0, 1)
        plt.xticks(rotation=20, ha="right")
        plt.legend(title="Metrik")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel1_model_karsilastirma.png", dpi=150)
        plt.show()
        print("Bu grafik 5 modelin Accuracy, F1-Score ve AUC metriklerini karşılaştırır. Skoru daha yüksek olan model, ilgili metrikte daha başarılıdır.")
        """
    ),
    code(
        """
        # Görsel 2: Random Forest feature importance yatay çubuk grafik
        top_rf = rf_importance_df.sort_values("importance", ascending=False).head(20)

        plt.figure(figsize=(11, 7))
        sns.barplot(data=top_rf, y="feature", x="importance", palette="viridis")
        plt.title("Özellik Önem Sıralaması (Random Forest)")
        plt.xlabel("Önem Skoru")
        plt.ylabel("Özellik")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel2_feature_importance.png", dpi=150)
        plt.show()
        print("Bu grafik Random Forest modelinde tahmine en fazla katkı veren özellikleri önem skorlarına göre sıralar. Üst sıralardaki özellikler model kararlarında daha etkili olmuştur.")
        """
    ),
    code(
        """
        # Görsel 3: En iyi modelin confusion matrix görseli
        class_count = confusion_df.shape[0]
        figure_size = max(9, min(24, class_count * 0.28))
        show_tick_labels = class_count <= 35

        plt.figure(figsize=(figure_size, figure_size))
        sns.heatmap(
            confusion_df,
            cmap="Blues",
            square=True,
            xticklabels=show_tick_labels,
            yticklabels=show_tick_labels,
            cbar_kws={"label": "Şarkı Sayısı"},
        )
        plt.title(f"Karışıklık Matrisi - {best_model_name}")
        plt.xlabel("Tahmin Edilen Tür")
        plt.ylabel("Gerçek Tür")
        if show_tick_labels:
            plt.xticks(rotation=90)
            plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel3_confusion_matrix.png", dpi=150)
        plt.show()
        print("Bu heatmap en iyi modelin gerçek türler ile tahmin ettiği türleri ne kadar eşleştirdiğini gösterir. Diyagonal değerlerin yüksek olması modelin doğru sınıflandırma yaptığını gösterir.")
        """
    ),
    code(
        """
        # Görsel 4: ROC eğrisi karşılaştırması
        plt.figure(figsize=(9, 7))
        for model_name, group in roc_df.groupby("model"):
            plt.plot(group["fpr"], group["tpr"], linewidth=2, label=model_name)

        plt.plot([0, 1], [0, 1], linestyle="--", color="gray", label="Rastgele Tahmin")
        plt.title("ROC Eğrisi Karşılaştırması")
        plt.xlabel("Yanlış Pozitif Oranı")
        plt.ylabel("Doğru Pozitif Oranı")
        plt.legend(title="Model", loc="lower right")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel4_roc_curve.png", dpi=150)
        plt.show()
        print("Bu ROC eğrisi modellerin sınıfları ayırt etme performansını karşılaştırır. Eğri sol üst köşeye yaklaştıkça modelin ayırt etme gücü artar.")
        """
    ),
    code(
        """
        # Spark ve Delta Lake oturumu: zaman serisi, popularity ve tür dağılımı için Delta verisini okuyacağız.
        from pyspark.sql import SparkSession, functions as F

        try:
            from delta import configure_spark_with_delta_pip
        except ModuleNotFoundError:
            configure_spark_with_delta_pip = None

        spark_builder = (
            SparkSession.builder
            .appName("SpotifyDashboardVisuals")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.warehouse.dir", str(BASE_DIR / "spark-warehouse"))
        )

        if configure_spark_with_delta_pip is not None:
            spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
        else:
            spark = spark_builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        if not (DELTA_TABLE_PATH / "_delta_log").exists():
            raise FileNotFoundError("Delta tablosu bulunamadı. DELTA_TABLE_PATH değişkenini düzenle.")

        delta_df = spark.read.format("delta").load(str(DELTA_TABLE_PATH))
        print("Delta satır sayısı:", delta_df.count())
        """
    ),
    code(
        """
        # Görsel 5: Kafka timestamp kolonuyla saatlik veri akışı trendi
        timestamp_candidates = ["kafka_timestamp", "timestamp"]
        timestamp_col = next((col for col in timestamp_candidates if col in delta_df.columns), None)
        if timestamp_col is None:
            raise ValueError("Delta tablosunda kafka_timestamp veya timestamp kolonu yok. Kafka Producer bu kolonlardan birini eklemeli.")

        hourly_df = (
            delta_df
            .withColumn("kafka_timestamp", F.col(timestamp_col).cast("timestamp"))
            .where(F.col("kafka_timestamp").isNotNull())
            .withColumn("saat", F.date_trunc("hour", F.col("kafka_timestamp")))
            .groupBy("saat")
            .count()
            .orderBy("saat")
        )

        hourly_pdf = hourly_df.toPandas()
        if hourly_pdf.empty:
            raise ValueError("timestamp kolonu timestamp tipine çevrilemedi veya boş geldi.")

        plt.figure(figsize=(12, 5))
        sns.lineplot(data=hourly_pdf, x="saat", y="count", marker="o", linewidth=2)
        plt.title("Saatlik Veri Akışı Trendi (Kafka Streaming)")
        plt.xlabel("Kafka Timestamp Saati")
        plt.ylabel("İşlenen Şarkı Sayısı")
        plt.xticks(rotation=25, ha="right")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel5_zaman_serisi.png", dpi=150)
        plt.show()
        print("Bu çizgi grafik Kafka timestamp bilgisine göre saatlik işlenen şarkı sayısını gösterir. Zaman içinde veri akışında yoğunlaşma veya düşüş olup olmadığını izlemeye yarar.")
        """
    ),
    code(
        """
        # Görsel 6: Popularity dağılımı histogramı
        if "popularity" not in delta_df.columns:
            raise ValueError("Delta tablosunda popularity kolonu yok.")

        popularity_pdf = (
            delta_df
            .select(F.col("popularity").cast("double").alias("popularity"))
            .where(F.col("popularity").isNotNull())
            .toPandas()
        )

        plt.figure(figsize=(10, 5))
        plt.hist(popularity_pdf["popularity"], bins=20, range=(0, 100), color="#4C78A8", edgecolor="white")
        plt.title("Şarkı Popülerlik Dağılımı")
        plt.xlabel("Popülerlik Skoru")
        plt.ylabel("Şarkı Sayısı")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel6_popularity_histogram.png", dpi=150)
        plt.show()
        print("Bu histogram şarkıların popularity skorlarının hangi aralıklarda yoğunlaştığını gösterir. Dağılım, veri setinde popüler ve az popüler şarkı dengesini anlamayı sağlar.")
        """
    ),
    code(
        """
        # Görsel 7: En popüler 10 tür dağılımı pie chart
        if "track_genre" not in delta_df.columns:
            raise ValueError("Delta tablosunda track_genre kolonu yok.")

        top_genres_pdf = (
            delta_df
            .groupBy("track_genre")
            .count()
            .orderBy(F.desc("count"))
            .limit(10)
            .toPandas()
        )

        plt.figure(figsize=(8, 8))
        plt.pie(
            top_genres_pdf["count"],
            labels=top_genres_pdf["track_genre"],
            autopct="%1.1f%%",
            startangle=90,
            counterclock=False,
        )
        plt.title("En Popüler 10 Müzik Türü Dağılımı")
        plt.ylabel("")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel7_tur_dagilimi_pie.png", dpi=150)
        plt.show()
        print("Bu pasta grafik veri setinde en sık görülen 10 müzik türünün oranını gösterir. Büyük dilimler, veri setinde daha fazla temsil edilen türleri ifade eder.")
        """
    ),
    code(
        """
        # Görsel 8: En iyi model için gerçek vs tahmin edilen türler
        top_actual_genres = predictions_df["actual_genre"].value_counts().head(10).index
        actual_counts = predictions_df["actual_genre"].value_counts().reindex(top_actual_genres, fill_value=0)
        predicted_counts = predictions_df["predicted_genre"].value_counts().reindex(top_actual_genres, fill_value=0)

        comparison_df = pd.DataFrame({
            "Tür": top_actual_genres,
            "Gerçek": actual_counts.values,
            "Tahmin": predicted_counts.values,
        }).melt(id_vars="Tür", var_name="Değer Tipi", value_name="Şarkı Sayısı")

        plt.figure(figsize=(12, 6))
        sns.barplot(data=comparison_df, x="Tür", y="Şarkı Sayısı", hue="Değer Tipi", palette="Set1")
        plt.title("Gerçek vs Tahmin Edilen Türler")
        plt.xlabel("Müzik Türü")
        plt.ylabel("Şarkı Sayısı")
        plt.xticks(rotation=25, ha="right")
        plt.legend(title=f"Model: {best_model_name}")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel8_gercek_vs_tahmin.png", dpi=150)
        plt.show()
        print("Bu grafik en iyi modelin seçili türlerde gerçek ve tahmin edilen şarkı sayılarını karşılaştırır. Gerçek ve tahmin çubuklarının yakın olması modelin tür dağılımını daha iyi yakaladığını gösterir.")
        """
    ),
    code(
        """
        # Görsel 9: Popularity vs danceability scatter plot
        required_columns = ["popularity", "danceability"]
        missing_columns = [col for col in required_columns if col not in delta_df.columns]
        if missing_columns:
            raise ValueError(f"Delta tablosunda eksik kolonlar var: {missing_columns}")

        scatter_pdf = (
            delta_df
            .select(
                F.col("popularity").cast("double").alias("popularity"),
                F.col("danceability").cast("double").alias("danceability"),
            )
            .where(F.col("popularity").isNotNull() & F.col("danceability").isNotNull())
            .limit(50000)
            .toPandas()
        )
        if scatter_pdf.empty:
            raise ValueError("Popularity ve danceability için çizilecek geçerli kayıt bulunamadı.")

        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=scatter_pdf, x="danceability", y="popularity", alpha=0.35, edgecolor=None)
        plt.title("Popülerlik ve Dans Edilebilirlik İlişkisi")
        plt.xlabel("Dans Edilebilirlik")
        plt.ylabel("Popülerlik")
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel9_scatter.png", dpi=150)
        plt.show()
        print("Bu scatter plot popularity ile danceability arasındaki ilişkiyi gösterir. Noktaların genel yönü, dans edilebilirliği yüksek şarkıların popülerlikle nasıl ilişkilendiğini yorumlamaya yardımcı olur.")
        """
    ),
    code(
        """
        # Görsel 10: Tür bazında ortalama energy karşılaştırması
        required_columns = ["track_genre", "energy"]
        missing_columns = [col for col in required_columns if col not in delta_df.columns]
        if missing_columns:
            raise ValueError(f"Delta tablosunda eksik kolonlar var: {missing_columns}")

        genre_energy_pdf = (
            delta_df
            .where(F.col("track_genre").isNotNull() & F.col("energy").isNotNull())
            .groupBy("track_genre")
            .agg(F.avg(F.col("energy").cast("double")).alias("Ortalama Energy"))
            .orderBy(F.desc("Ortalama Energy"))
            .limit(20)
            .toPandas()
        )
        if genre_energy_pdf.empty:
            raise ValueError("Tür ve energy için çizilecek geçerli kayıt bulunamadı.")

        plt.figure(figsize=(10, 8))
        sns.barplot(data=genre_energy_pdf, y="track_genre", x="Ortalama Energy", palette="mako")
        plt.title("Türlere Göre Ortalama Energy")
        plt.xlabel("Ortalama Energy")
        plt.ylabel("Müzik Türü")
        plt.xlim(0, 1)
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel10_tur_energy.png", dpi=150)
        plt.show()
        print("Bu yatay çubuk grafik müzik türlerine göre ortalama energy değerlerini karşılaştırır. Daha uzun çubuklar, ilgili türdeki şarkıların ortalama olarak daha yüksek enerjiye sahip olduğunu gösterir.")
        """
    ),
    code(
        """
        # Görsel 11: Tüm sayısal kolonlar için korelasyon heatmap
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.stat import Correlation
        from pyspark.sql.types import NumericType

        numeric_columns = [
            field.name
            for field in delta_df.schema.fields
            if isinstance(field.dataType, NumericType)
        ]
        if len(numeric_columns) < 2:
            raise ValueError("Korelasyon heatmap için en az iki sayısal kolon gerekir.")

        numeric_df = delta_df.select(*[
            F.col(col_name).cast("double").alias(col_name)
            for col_name in numeric_columns
        ])
        assembler = VectorAssembler(
            inputCols=numeric_columns,
            outputCol="numeric_features",
            handleInvalid="skip",
        )
        assembled_df = assembler.transform(numeric_df).select("numeric_features")
        if assembled_df.limit(1).count() == 0:
            raise ValueError("Korelasyon hesaplamak için geçerli sayısal kayıt bulunamadı.")

        corr_matrix = Correlation.corr(assembled_df, "numeric_features", "pearson").head()[0].toArray()
        corr_pdf = pd.DataFrame(corr_matrix, index=numeric_columns, columns=numeric_columns)

        plt.figure(figsize=(max(10, len(numeric_columns) * 0.65), max(8, len(numeric_columns) * 0.55)))
        sns.heatmap(corr_pdf, cmap="coolwarm", center=0, annot=False, square=True, linewidths=0.25)
        plt.title("Özellikler Arası Korelasyon")
        plt.xticks(rotation=45, ha="right")
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig(DASHBOARD_DIR / "gorsel11_korelasyon.png", dpi=150)
        plt.show()
        print("Bu korelasyon heatmap'i sayısal özelliklerin birbirleriyle doğrusal ilişki düzeyini gösterir. Pozitif veya negatif güçlü renkler, özellikler arasında daha belirgin ilişki olduğunu anlatır.")
        """
    ),
    markdown(
        """
        ## Kaydedilen Görseller

        Bu notebook bittiğinde şu PNG dosyaları oluşur:

        - `dashboard/gorsel1_model_karsilastirma.png`
        - `dashboard/gorsel2_feature_importance.png`
        - `dashboard/gorsel3_confusion_matrix.png`
        - `dashboard/gorsel4_roc_curve.png`
        - `dashboard/gorsel5_zaman_serisi.png`
        - `dashboard/gorsel6_popularity_histogram.png`
        - `dashboard/gorsel7_tur_dagilimi_pie.png`
        - `dashboard/gorsel8_gercek_vs_tahmin.png`
        - `dashboard/gorsel9_scatter.png`
        - `dashboard/gorsel10_tur_energy.png`
        - `dashboard/gorsel11_korelasyon.png`
        """
    ),
]


def write_notebook(path: Path, cells: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(notebook(cells), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> None:
    write_notebook(NOTEBOOK_DIR / "adim6_ml_mlflow.ipynb", ADIM6_CELLS)
    write_notebook(NOTEBOOK_DIR / "adim7_dashboard.ipynb", ADIM7_CELLS)
    print(f"Notebooklar oluşturuldu: {NOTEBOOK_DIR}")


if __name__ == "__main__":
    main()
