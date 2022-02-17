FROM python:3.8-slim

COPY requirements.txt .

RUN set -ex; \
	pip install --no-cache-dir -r requirements.txt

ADD opensky-producer/opensky.py .
ADD crypto-producer/crypto.py .

CMD ["python", "-u", "./crypto.py"]
