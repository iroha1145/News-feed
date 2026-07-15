FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp

WORKDIR /app

COPY backend/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


FROM base AS test

COPY backend/requirements-dev.txt requirements-dev.txt
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY backend/app ./app
COPY backend/tests ./tests
COPY config ./config
CMD ["python", "-m", "pytest", "-q", "tests"]


FROM base AS runtime

RUN groupadd --gid 10001 macrolens \
    && useradd --uid 10001 --gid macrolens --create-home --shell /usr/sbin/nologin macrolens

COPY --chown=macrolens:macrolens backend/app ./app
COPY --chown=macrolens:macrolens config ./config
RUN mkdir -p /app/data && chown macrolens:macrolens /app/data

USER macrolens
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers", "--no-access-log"]
