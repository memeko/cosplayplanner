FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY PROJECT_NAME.txt ./PROJECT_NAME.txt

ENV PYTHONUNBUFFERED=1
ENV PORT=80

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import os,sys,urllib.request; p=os.getenv('PORT','80'); urllib.request.urlopen(f'http://127.0.0.1:{p}/healthz', timeout=3); sys.exit(0)" || exit 1

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'"]
