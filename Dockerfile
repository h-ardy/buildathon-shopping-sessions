# Shopping Sessions: DuckDB pipeline. The scorer builds this natively on linux/amd64.
FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

RUN pip install --no-cache-dir duckdb==2.0.1 numpy==2.5.3 scipy==1.18.1 pyarrow==26.0.1

COPY pipeline.py /app/pipeline.py
# Compile bytecode at build time; the root filesystem is read-only at run time.
RUN python -m compileall -q /app /usr/local/lib/python3.12/site-packages/duckdb /usr/local/lib/python3.12/site-packages/scipy/sparse

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TMPDIR=/scratch HOME=/scratch
WORKDIR /app
CMD ["python", "/app/pipeline.py"]
