FROM openjdk:8-jre-slim

# Set environment variables
ENV SPARK_VERSION=2.4.0
ENV HADOOP_VERSION=2.7

# Install required packages
RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz | tar xvz -C /opt && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm -rf /var/cache/apk/*

# Install required packages for python
RUN apt-get update && apt-get install -y wget build-essential checkinstall

RUN apt-get install -y zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev

# Install Python 3.7
RUN wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar xzf Python-3.7.9.tgz && \
    cd Python-3.7.9 && ./configure --enable-optimizations && \
    make altinstall && \
    python3.7 -V

# Set Spark home environment variable
ENV SPARK_HOME /opt/spark

# Set PATH environment variable
ENV PATH $SPARK_HOME/bin:$PATH
