FROM apache/airflow:latest-python3.7



USER root
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main'
RUN apt-get update
RUN apt-get install -y openjdk-8-jdk

COPY pip.conf /etc/pip.conf
USER airflow
RUN python3.7 -m pip install fs_etl==0.2.0