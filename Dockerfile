FROM python:3.11-slim

ARG APP_VERSION=unknown
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    APP_VERSION=${APP_VERSION}

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/app app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app app_main.py kafka_runtime.py read_model.py splitwise_to_excel.py web_app.py ./
COPY --chown=app:app templates ./templates

EXPOSE 8080

USER 10001:10001

ENTRYPOINT ["python", "app_main.py"]
