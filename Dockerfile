FROM python:3.7-slim

MAINTAINER sanjanadodley@gmail.com

RUN apt-get update -y && \
    apt-get install -y python-pip python-dev && \
    pip install --upgrade pip

COPY . /

WORKDIR /

RUN pip install -r requirements.txt

EXPOSE 5000

ENV NAME World

ENTRYPOINT [ "python" ]

CMD [ "app.py" ]