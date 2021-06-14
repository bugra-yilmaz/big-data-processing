FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y default-jdk wget tar python3.8 python3-pip && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.0.1-bin-hadoop3.2.tgz && \
    mv spark-3.0.1-bin-hadoop3.2/ /usr/local/spark && \
    ln -s /usr/local/spark spark

WORKDIR /usr/src/app

COPY ./app /usr/src/app

RUN pip3 install -r requirements.txt
