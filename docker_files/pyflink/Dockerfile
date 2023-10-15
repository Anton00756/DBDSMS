FROM flink:1.17.1-java11

ENV TZ=Europe/Moscow
ENV PYTHONPATH=/work_dir

RUN apt-get update -y && \
apt-get install -y curl nano gcc g++ python3 python3-pip python3-dev libpq-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir work_dir
WORKDIR /work_dir
RUN wget -O flink-connector-kafka.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
RUN mkdir -p /opt/flink/plugins/s3
RUN wget -O /opt/flink/plugins/s3/flink-s3-hadoop.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.1/flink-s3-fs-hadoop-1.17.1.jar
RUN wget -O flink-connector-jdbc.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar
RUN wget -O postgres-connector-java.jar https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
COPY services/pyflink service
COPY utils utils

RUN addgroup --gid 1000 user
RUN adduser --home /home/user --uid 1000 --gid 1000 --shell /bin/sh --disabled-password user
RUN addgroup --gid 1001 docker && addgroup user docker
RUN chown -R user:user /var
RUN chown -R user:user /work_dir
RUN chown -R user:user /opt/flink

RUN ln -sf /proc/1/fd/1 /var/log/container_logs.log

USER user

RUN pip install wheel setuptools pip --upgrade
RUN pip install -r service/requirements.txt