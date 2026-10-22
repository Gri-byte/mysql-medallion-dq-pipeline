# Image for the pipeline and for the optional Dagster UI (the MySQL server runs in its own container,
# see docker-compose.yml).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependencies first, so this layer is cached until the requirements files change.
COPY requirements.txt requirements-orchestration.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt -r requirements-orchestration.txt

COPY pytest.ini workspace.yaml ./
COPY sql ./sql
COPY src ./src
COPY orchestration ./orchestration
COPY tests ./tests

# Do not run as root inside the container. /dagster_home exists (and is owned by `app`) BEFORE the
# compose volume is mounted on it, so the named volume inherits the right owner and stays writable.
RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app/data/raw /dagster_home \
    && chown -R app:app /app /dagster_home
USER app

# Default command: run the whole pipeline once. Override it to run something else, e.g.
#   docker compose run --rm pipeline pytest -v
CMD ["python", "-m", "src.run_pipeline", "--reset", "-v"]
