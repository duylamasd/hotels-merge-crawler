FROM python:3-alpine

WORKDIR /app

COPY . .

RUN apk add --no-cache python3 py3-pip && \
  pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "src/main.py"]
