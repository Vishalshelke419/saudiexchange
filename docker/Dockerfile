FROM python:3.12-slim

# System deps for Chromium + drivers
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    libnss3 libxi6 libgbm1 libx11-6 libxss1 libasound2 \
    fonts-liberation ca-certificates wget unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer caching)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files into the image
COPY . /app

# Default envs used by your scripts
ENV FINJSON_ROOT=/app/financials_json
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_BIN=/usr/bin/chromedriver

# Default command (harmless)
CMD ["python", "-m", "pip", "list"]
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_BIN=/usr/bin/chromedriver
