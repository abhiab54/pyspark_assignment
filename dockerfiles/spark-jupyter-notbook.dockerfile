FROM openjdk:8-jre-slim

# Set environment variables
ENV SPARK_VERSION=2.4.0
ENV HADOOP_VERSION=2.7

RUN apt-get update && apt-get install -y wget build-essential checkinstall

# Install required packages for python
RUN apt-get install -y zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev

# Install Python 3.7
RUN wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar xzf Python-3.7.9.tgz && \
    cd Python-3.7.9 && ./configure --enable-optimizations && \
    make altinstall && \
    python3.7 -V

# Download and install Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz | tar xvz -C /opt && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark && \
    rm -rf /var/cache/apk/*

# Install Delta Lake for Spark
RUN wget -qO- https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.1.0/delta-core_2.12-1.1.0.jar -P /usr/local/spark/jars/

# Set Spark home environment variable
ENV SPARK_HOME /opt/spark

# Set PATH environment variable
ENV PATH $SPARK_HOME/bin:$PATH

# Install PySpark and Delta for PySpark
RUN apt-get install -y python3-pip && \
    pip3 install pyspark==2.4.0 && \
    pip3 install delta-spark==1.0 && \
	pip3 install jupyterlab

RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.11/0.6.1/delta-core_2.11-0.6.1.jar -P /opt/spark/jars/

# Copy the Spark job files to the container
WORKDIR /app

COPY scripts .

COPY data /data

# Run the Spark job
CMD ["jupyter-lab"]