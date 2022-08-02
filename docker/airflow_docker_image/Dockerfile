FROM apache/airflow:latest-python3.7
#COPY pip.conf /etc/pip.conf
USER root
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && \
    apt-get update && \
    apt-get install -y software-properties-common openjdk-8-jdk && \
    apt-get clean
#    python3.7 -m pip install fs_etl==0.5.7

USER airflow