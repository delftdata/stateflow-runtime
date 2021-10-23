FROM python:3-slim

WORKDIR /home/flask-app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

# default flask port
EXPOSE 5000