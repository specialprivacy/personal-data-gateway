FROM python:3

RUN apt-get update
WORKDIR /app

RUN pip install Flask

RUN pip install pyyaml
RUN pip install filepath
RUN pip install kafka-python
RUN pip install requests

COPY . /app/

ENV PYTHONUNBUFFERED=1

ENV FLASK_APP=app.py
ENV FLASK_DEBUG=true
CMD flask run --host=0.0.0.0 --with-threads
