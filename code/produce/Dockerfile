# Dockerfile
FROM jfloff/alpine-python:3.6-slim

WORKDIR /usr/app

ADD ./requirements.txt ./
RUN pip install -r requirements.txt
ADD ./ ./

CMD ["python", "produce_demo.py"]
