ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

ARG PYSPARK_VERSION=4.1
ARG DELTA_VERSION=4.1

RUN apt-get update && apt-get install -y --no-install-recommends default-jdk && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY file_processing ./file_processing
COPY dataframe_validation ./dataframe_validation
COPY table_writing ./table_writing
COPY file_management ./file_management
COPY samples ./samples
COPY tests ./tests

RUN pip install --no-cache-dir -e ".[dev]" \
    "pyspark>=${PYSPARK_VERSION},<$(echo ${PYSPARK_VERSION} | awk -F. '{print $1"."$2+1}')" \
    "delta-spark>=${DELTA_VERSION},<$(echo ${DELTA_VERSION} | awk -F. '{print $1"."$2+1}')"

ENV PYTHONPATH=/app

CMD ["python", "-m", "pytest", "tests/", "-v"]
