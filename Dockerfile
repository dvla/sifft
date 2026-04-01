FROM apache/spark-py:v3.4.0

USER root

WORKDIR /app

RUN pip3 install pyspark==3.5.7 delta-spark pytest pytest-bdd pytest-html pytest-cov openpyxl pyld pandas xlrd fsspec s3fs

COPY file_processing ./file_processing
COPY dataframe_validation ./dataframe_validation
COPY table_writing ./table_writing
COPY file_management ./file_management
COPY samples ./samples
COPY tests ./tests

ENV PYTHONPATH=/app

CMD ["python3", "-m", "pytest", "tests/", "-v"]
