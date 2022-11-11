FROM flink:1.14.4

LABEL maintainer="Hoyeop Lee <algorizm@gmail.com>"

# Debian GNU/Linux 11
RUN touch /etc/apt/apt.conf.d/proxy.conf
RUN echo "Acquire::http::Proxy \"http://192.168.1.139:3128/\";" > /etc/apt/apt.conf.d/proxy.conf
RUN echo "Acquire::https::Proxy \"https://192.168.1.139:3128/\";" >> /etc/apt/apt.conf.d/proxy.conf

RUN DEBIAN_FRONTEND=noninteractive apt-get update -y && \
apt-get upgrade -y && \
apt-get install -y apt-utils && \
apt-get install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev && \
apt-get install -y vim && \ 
apt-get install -y fcitx-hangul fcitx-lib* fonts-nanum* && \
apt-get install -y liblzma-dev
 
COPY Python-3.8.13.tar.xz /
WORKDIR /
RUN tar -xf /Python-3.8.13.tar.xz && \
cd Python-3.8.13 && \
./configure --without-tests --enable-shared && \
make -j16 && \
make install && \
ldconfig /usr/local/lib && \ 
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Cores(CPU)
#RUN make -j 16
#RUN make altinstall
#RUN ldconfig /opt/Python-3.8.13
#RUN ln -s /usr/local/bin/python3.8 /usr/bin/python

RUN pip3 config set global.proxy 192.168.1.139:3128
RUN pip3 install apache-flink==1.14.4
#RUN python -m pip install apache-flink

COPY jars /jars
RUN cp /jars/* /opt/flink/lib && chown flink:flink /opt/flink/lib/* && rm -rf /jars

ENV FLINK_HOME /opt/flink
ENV PYFLINK_CLIENT_EXECUTABLE /usr/local/bin/python

COPY --chown=flink:flink jnd-flink-sample /jnd-flink-sample
#RUN chown flink:flink /jnd-flink-sample
