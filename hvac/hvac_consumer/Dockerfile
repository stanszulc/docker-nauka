FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir torch==2.2.0+cpu \
       --index-url https://download.pytorch.org/whl/cpu
COPY consumer.py .
RUN useradd -m appuser && chown -R appuser /app
USER appuser
CMD ["python", "-u", "consumer.py"]
