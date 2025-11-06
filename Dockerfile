FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src /app/src
COPY models /app/models
CMD ["python","/app/src/predict_api.py"]
