FROM flink:latest

ENV TZ=Europe/Moscow

RUN apt-get update -y && \
apt-get install -y curl nano gcc g++ python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir work_dir
WORKDIR /work_dir
COPY services/pyflink .
COPY utils utils

RUN addgroup --gid 1000 user
RUN adduser --home /home/user --uid 1000 --gid 1000 --shell /bin/sh --disabled-password user
RUN addgroup --gid 1001 docker && addgroup user docker
RUN chown -R user:user /var
RUN chown -R user:user /work_dir

RUN ln -sf /proc/1/fd/1 /var/log/container_logs.log

USER user

RUN pip install wheel setuptools pip --upgrade
RUN pip install -r requirements.txt

CMD [ "python", "./processor.py" ]