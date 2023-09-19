FROM python:3.10-alpine

ENV TZ=Europe/Moscow
ENV PYTHONPATH=/work_dir
RUN apk update && apk add curl nano gcc g++

RUN mkdir work_dir
WORKDIR /work_dir
COPY services/data_generator service
COPY utils utils

RUN addgroup -g 1000 user && adduser -h /home/user -u 1000 -G user -s /bin/sh -D user
RUN addgroup -g 1001 docker && addgroup user docker
RUN chown -R user:user /var
RUN chown -R user:user /work_dir

RUN ln -sf /proc/1/fd/1 /var/log/container_logs.log

USER user

RUN pip install wheel setuptools pip --upgrade
RUN pip install -r service/requirements.txt

CMD [ "python", "service/generator.py" ]