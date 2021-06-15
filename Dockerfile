FROM ubuntu:18.04

# Install required packages
RUN apt-get update && \
    apt-get install -y default-jdk wget tar python3.8 python3-pip && \
    apt-get clean

# Download latest Spark release and extract it
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2/ /usr/local/spark

# Set environment variables for Spark operations
ENV SPARK_HOME=/usr/local/spark
ENV PYSPARK_PYTHON=python3

WORKDIR /usr/src/app

# Include PySpark app source code
COPY ./app /usr/src/app

# Install required Python packages - PySpark and pytest
RUN pip3 install -r requirements.txt

# Set Spark logging level to ERROR
RUN sed 's/log4j.rootCategory=INFO/log4j.rootCategory=ERROR/g' \
    /usr/local/spark/conf/log4j.properties.template > /usr/local/spark/conf/log4j.properties
