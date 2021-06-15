FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y default-jdk wget tar python3.8 python3-pip && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2/ /usr/local/spark

ENV SPARK_HOME=/usr/local/spark
ENV PYSPARK_PYTHON=python3

WORKDIR /usr/src/app

COPY ./app /usr/src/app

RUN pip3 install -r requirements.txt

RUN sed 's/log4j.rootCategory=INFO/log4j.rootCategory=ERROR/g' \
    /usr/local/spark/conf/log4j.properties.template > /usr/local/spark/conf/log4j.properties
