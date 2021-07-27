FROM python:3.7.3-slim

# Defining Root Directory
ENV SRC_HOME=/opt/feature_store
ENV BUILD_HOME=/tmp

# Bug w/ click
ENV LC_ALL en_US.utf-8
ENV LANG en_US.utf-8

COPY feature_store/build $BUILD_HOME


RUN mkdir -p mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y $(cat $BUILD_HOME/apt-packages.txt) && \
    pip3 install --upgrade pip && \
    pip3 install -q --no-cache-dir -r $BUILD_HOME/requirements.txt && \
    apt-get purge -y wget unzip build-essential libffi-dev libssl-dev && \
    apt-get clean && \
    rm -rf $BUILD_HOME && \
    mkdir -p $SRC_HOME/lib

COPY shared /tmp/shared
RUN pip3 install /tmp/shared/.
# Copy Source into Container
COPY feature_store/src $SRC_HOME
CMD $SRC_HOME"/scripts/entrypoint.sh"

