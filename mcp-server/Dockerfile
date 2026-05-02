FROM python:3.11-slim
WORKDIR /app
RUN pip install mcp fastmcp
COPY server.py .
COPY fhir_patients_10.json .
EXPOSE 8001
CMD ["python", "server.py"]
