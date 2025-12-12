FROM python:3.9-slim

WORKDIR /app

# Install system dependencies required for Cartopy/Shapely (used by traffic library)
RUN apt-get update && apt-get install -y \
    gcc g++ libgeos-dev libproj-dev proj-bin \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
EXPOSE 8501

CMD ["streamlit", "run", "src/app.py", "--server.address=0.0.0.0"]