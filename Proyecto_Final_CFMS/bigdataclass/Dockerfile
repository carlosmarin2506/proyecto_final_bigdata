FROM eclipse-temurin:17-jdk-alpine

RUN apk add bash
RUN apk add build-base
RUN apk add nano
RUN apk add postgresql-client
RUN apk add python3
RUN apk add python3-dev
RUN apk add libffi-dev
RUN apk add openblas-dev
RUN apk add zlib-dev
RUN apk add jpeg-dev
RUN apk add zeromq-dev
RUN apk add py3-pip
RUN python3 -m pip install --upgrade pip setuptools wheel --break-system-packages
RUN python3 -m pip install numpy --break-system-packages
RUN python3 -m pip install matplotlib --break-system-packages
RUN python3 -m pip install seaborn --break-system-packages
RUN python3 -m pip install pyspark --break-system-packages
RUN python3 -m pip install pytest --break-system-packages
RUN python3 -m pip install notebook --break-system-packages
RUN python3 -m pip install findspark --break-system-packages
RUN ln -sf /usr/bin/python3 /usr/bin/python

COPY postgresql-42.2.14.jar /tmp/postgresql-42.2.14.jar
RUN PYSPARK_PATH="$(python3 -c 'import pyspark; import sys; print(pyspark.__path__[0])')" \
 && mkdir -p "$PYSPARK_PATH/jars" \
 && mv /tmp/postgresql-42.2.14.jar "$PYSPARK_PATH/jars/postgresql-42.2.14.jar"


WORKDIR /src

COPY . /src
