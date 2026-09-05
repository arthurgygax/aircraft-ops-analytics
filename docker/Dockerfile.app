# Flight Explorer: a consumer of the Gold tables.
# No Spark and no JVM -- the app reads Delta directly with delta-rs, so it
# starts in seconds. All heavy transformation happens in the pipeline.
FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

ENV PYTHONPATH=/app

EXPOSE 8501

CMD ["streamlit", "run", "app/main.py", \
     "--server.address=0.0.0.0", "--server.port=8501", \
     "--browser.gatherUsageStats=false"]
