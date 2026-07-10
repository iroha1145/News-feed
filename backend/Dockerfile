FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/tmp

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


FROM base AS test

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY app ./app
COPY tests ./tests

CMD ["python", "-m", "pytest", "-q", "tests"]


FROM base AS runtime

RUN groupadd --gid 10001 macrolens \
    && useradd --uid 10001 --gid macrolens --create-home --shell /usr/sbin/nologin macrolens

COPY --chown=macrolens:macrolens app ./app
RUN mkdir -p data && chown -R macrolens:macrolens /app/data

USER macrolens
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
